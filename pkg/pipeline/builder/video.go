// Copyright 2023 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package builder

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-gst/go-glib/glib"
	"github.com/go-gst/go-gst/gst"
	"github.com/go-gst/go-gst/gst/app"
	"github.com/linkdata/deadlock"

	"github.com/livekit/egress/pkg/config"
	"github.com/livekit/egress/pkg/errors"
	"github.com/livekit/egress/pkg/gstreamer"
	"github.com/livekit/egress/pkg/types"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

const (
	videoTestSrcName = "video_test_src"
)

func setGstEnumProperty(element *gst.Element, name string, value int) error {
	propType, err := element.GObject().GetPropertyType(name)
	if err != nil {
		return err
	}
	gValue, err := glib.ValueInit(propType)
	if err != nil {
		return err
	}
	// ValueInit installs a runtime finalizer that calls g_value_unset/free.
	// Calling Unset here as well double-frees the GValue later and can crash
	// the egress process when AV1 recordings finalize.
	gValue.SetEnum(value)
	return element.GObject().SetPropertyValue(name, gValue)
}

type VideoBin struct {
	bin  *gstreamer.Bin
	conf *config.PipelineConfig

	mu          deadlock.Mutex
	nextID      int
	selectedPad string
	lastPTS     uint64
	pads        map[string]*gst.Pad
	names       map[string]string
	selector    *gst.Element
	rawVideoTee *gst.Element
}

// buildVideoQueue creates a queue for the video pipeline. For live sources the
// queue is leaky (drops old buffers when full) to handle real-time overrun. For
// non-live replay the queue is blocking so backpressure throttles the source.
func (b *VideoBin) buildVideoQueue(name string) (*gst.Element, error) {
	queue, err := gstreamer.BuildQueue(name, b.conf.Latency.PipelineLatency, b.conf.Live)
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	return queue, nil
}

func BuildVideoBin(pipeline *gstreamer.Pipeline, p *config.PipelineConfig) error {
	b := &VideoBin{
		bin:  pipeline.NewBin("video"),
		conf: p,
	}

	switch p.SourceType {
	case types.SourceTypeWeb:
		if err := b.buildWebInput(); err != nil {
			return err
		}

	case types.SourceTypeSDK:
		if err := b.buildSDKInput(); err != nil {
			return err
		}

		pipeline.AddOnTrackAdded(b.onTrackAdded)
		pipeline.AddOnTrackRemoved(b.onTrackRemoved)
		pipeline.AddOnTrackMuted(b.onTrackMuted)
		pipeline.AddOnTrackUnmuted(b.onTrackUnmuted)
		pipeline.AddOnSourceBinReset(b.onSourceBinReset)
	}

	var getPad func() *gst.Pad
	if len(p.GetEncodedOutputs()) > 1 {
		tee, err := gst.NewElementWithName("tee", "video_tee")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		if err = b.bin.AddElement(tee); err != nil {
			return err
		}

		getPad = func() *gst.Pad {
			return tee.GetRequestPad("src_%u")
		}
	} else if len(p.GetEncodedOutputs()) > 0 {
		queue, err := b.buildVideoQueue("video_queue")
		if err != nil {
			return err
		}
		if err = b.bin.AddElement(queue); err != nil {
			return err
		}

		getPad = func() *gst.Pad {
			return queue.GetStaticPad("src")
		}
	}

	b.bin.SetGetSinkPad(func(name string) *gst.Pad {
		if strings.HasPrefix(name, "image") {
			return b.rawVideoTee.GetRequestPad("src_%u")
		} else if getPad != nil {
			return getPad()
		}

		return nil
	})

	return pipeline.AddSourceBin(b.bin)
}

func (b *VideoBin) onTrackAdded(ts *config.TrackSource) {
	if b.bin.GetState() > gstreamer.StateRunning {
		return
	}

	if ts.TrackKind == lksdk.TrackKindVideo {
		logger.Debugw("adding video app src bin", "trackID", ts.TrackID)
		if err := b.addAppSrcBin(ts); err != nil {
			logger.Errorw("failed to add video app src bin", err, "trackID", ts.TrackID)
			b.bin.OnError(err)
		}
	}
}

func (b *VideoBin) onTrackRemoved(trackID string) {
	if b.bin.GetState() > gstreamer.StateRunning {
		return
	}

	b.mu.Lock()
	name, ok := b.names[trackID]
	if !ok {
		b.mu.Unlock()
		return
	}
	delete(b.names, trackID)
	delete(b.pads, name)

	if b.selectedPad == name {
		if err := b.setSelectorPadLocked(videoTestSrcName); err != nil {
			b.mu.Unlock()
			b.bin.OnError(err)
			return
		}
	}
	b.mu.Unlock()

	if err := b.bin.RemoveSourceBin(name); err != nil {
		b.bin.OnError(err)
	}
}

func (b *VideoBin) onTrackMuted(trackID string) {
	if b.bin.GetState() > gstreamer.StateRunning {
		return
	}

	b.mu.Lock()
	if name, ok := b.names[trackID]; ok && b.selectedPad == name {
		if err := b.setSelectorPadLocked(videoTestSrcName); err != nil {
			b.mu.Unlock()
			b.bin.OnError(err)
			return
		}
	}
	b.mu.Unlock()
}

func (b *VideoBin) onTrackUnmuted(trackID string) {
	if b.bin.GetState() > gstreamer.StateRunning {
		return
	}

	b.mu.Lock()
	if name, ok := b.names[trackID]; ok {
		if err := b.setSelectorPadLocked(name); err != nil {
			b.mu.Unlock()
			b.bin.OnError(err)
			return
		}
	}
	b.mu.Unlock()
}

func (b *VideoBin) onSourceBinReset(ts *config.TrackSource) error {
	if ts.TrackKind != lksdk.TrackKindVideo {
		return nil
	}
	return b.resetVideoAppSrcBin(ts)
}

func (b *VideoBin) resetVideoAppSrcBin(ts *config.TrackSource) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	oldName, ok := b.names[ts.TrackID]
	if !ok {
		return errors.New("track already removed, cannot reset video source bin")
	}

	if b.bin.GetState() > gstreamer.StateRunning {
		return errors.New("pipeline stopping, cannot reset video source bin")
	}

	// If the stuck bin is the currently selected pad, switch to test src first
	if b.conf.VideoDecoding && b.selectedPad == oldName {
		if err := b.setSelectorPadLocked(videoTestSrcName); err != nil {
			return err
		}
	}

	// Clean up old pad reference before force-remove
	delete(b.pads, oldName)

	// Force-remove old bin (blocks on GLib main loop, safe to hold b.mu since
	// ForceRemoveSourceBin only acquires gstreamer.Bin's internal mutex)
	if err := b.bin.ForceRemoveSourceBin(oldName); err != nil {
		return fmt.Errorf("failed to force remove video source bin: %w", err)
	}

	// Create new appsrc element (reuse the same element name so watch.go works)
	newElement, err := gst.NewElementWithName("appsrc", fmt.Sprintf("app_%s", ts.TrackID))
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	ts.AppSrc = app.SrcFromElement(newElement)

	name := fmt.Sprintf("%s_%d", ts.TrackID, b.nextID)
	b.nextID++

	appSrcBin, err := b.buildAppSrcBin(ts, name)
	if err != nil {
		return fmt.Errorf("failed to build new video source bin: %w", err)
	}

	if b.conf.VideoDecoding {
		b.createSrcPadLocked(ts.TrackID, name)
	}

	if err = b.bin.AddSourceBin(appSrcBin); err != nil {
		return fmt.Errorf("failed to add new video source bin: %w", err)
	}

	if b.conf.VideoDecoding {
		if err := b.setSelectorPadLocked(name); err != nil {
			return err
		}
	}

	logger.Infow("video source bin reset complete", "trackID", ts.TrackID, "newBin", name)
	return nil
}

func (b *VideoBin) buildWebInput() error {
	xImageSrc, err := gst.NewElement("ximagesrc")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = xImageSrc.SetProperty("display-name", b.conf.Display); err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = xImageSrc.SetProperty("use-damage", false); err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = xImageSrc.SetProperty("show-pointer", false); err != nil {
		return errors.ErrGstPipelineError(err)
	}

	videoQueue, err := b.buildVideoQueue("video_input_queue")
	if err != nil {
		return err
	}

	videoConvert, err := gst.NewElement("videoconvert")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	videoRate, err := gst.NewElement("videorate")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = videoRate.SetProperty("skip-to-first", true); err != nil {
		return errors.ErrGstPipelineError(err)
	}

	caps, err := gst.NewElement("capsfilter")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = caps.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
		"video/x-raw,framerate=%d/1",
		b.conf.Framerate,
	),
	)); err != nil {
		return errors.ErrGstPipelineError(err)
	}

	if err = b.bin.AddElements(xImageSrc, videoQueue, videoConvert, videoRate, caps); err != nil {
		return err
	}

	return b.addDecodedVideoSink()
}

func (b *VideoBin) buildSDKInput() error {
	b.pads = make(map[string]*gst.Pad)
	b.names = make(map[string]string)

	// add selector first so pads can be created
	if b.conf.VideoDecoding {
		if err := b.addSelector(); err != nil {
			return err
		}
	}

	if b.conf.VideoTrack != nil {
		if err := b.addAppSrcBin(b.conf.VideoTrack); err != nil {
			return err
		}
	}

	if b.conf.VideoDecoding {
		b.bin.SetGetSrcPad(b.getSrcPad)

		if err := b.addVideoTestSrcBin(); err != nil {
			return err
		}
		if b.conf.VideoTrack == nil {
			if err := b.setSelectorPad(videoTestSrcName); err != nil {
				return err
			}
		}
		if err := b.addDecodedVideoSink(); err != nil {
			return err
		}
	}

	return nil
}

func (b *VideoBin) addAppSrcBin(ts *config.TrackSource) error {
	name := fmt.Sprintf("%s_%d", ts.TrackID, b.nextID)
	b.nextID++

	appSrcBin, err := b.buildAppSrcBin(ts, name)
	if err != nil {
		return err
	}

	if b.conf.VideoDecoding {
		b.createSrcPad(ts.TrackID, name)
	}

	if err = b.bin.AddSourceBin(appSrcBin); err != nil {
		return err
	}

	if b.conf.VideoDecoding {
		return b.setSelectorPad(name)
	}

	return nil
}

func (b *VideoBin) buildAppSrcBin(ts *config.TrackSource, name string) (*gstreamer.Bin, error) {
	appSrcBin := b.bin.NewBin(name)
	appSrcBin.SetEOSFunc(func() bool {
		return false
	})
	ts.AppSrc.SetArg("format", "time")
	if err := ts.AppSrc.SetProperty("is-live", b.conf.Live); err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if !b.conf.Live {
		if err := ts.AppSrc.SetProperty("block", true); err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}
	}
	if err := appSrcBin.AddElement(ts.AppSrc.Element); err != nil {
		return nil, err
	}

	switch ts.MimeType {
	case types.MimeTypeH264:
		if err := ts.AppSrc.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"application/x-rtp,media=video,payload=%d,encoding-name=H264,clock-rate=%d",
			ts.PayloadType, ts.ClockRate,
		))); err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		rtpH264Depay, err := gst.NewElement("rtph264depay")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		caps, err := gst.NewElement("capsfilter")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}
		if err = caps.SetProperty("caps", gst.NewCapsFromString(
			"video/x-h264,stream-format=byte-stream",
		)); err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		if err = appSrcBin.AddElements(rtpH264Depay, caps); err != nil {
			return nil, err
		}

		if !b.conf.VideoDecoding {
			h264ParseFixer, err := newPTSFixer("h264parse", fmt.Sprintf("track:%s", ts.TrackID))
			if err != nil {
				return nil, err
			}

			if err = appSrcBin.AddElement(h264ParseFixer.Element); err != nil {
				return nil, err
			}

			return appSrcBin, nil
		}

		avDecH264, err := gst.NewElement("avdec_h264")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		if err = appSrcBin.AddElement(avDecH264); err != nil {
			return nil, err
		}

	case types.MimeTypeH265:
		// Hardware-H265 publishers (every modern iPhone via VideoToolbox) were
		// previously dropped at ingest with ErrNotSupported, so their participant
		// egress produced no file and their composite tile rendered black. All
		// three elements (rtph265depay, h265parse, avdec_h265) are present in the
		// egress image; mirror the H264 path so the SDK source can decode HEVC and
		// re-encode to the configured recording codec (AV1).
		if err := ts.AppSrc.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"application/x-rtp,media=video,payload=%d,encoding-name=H265,clock-rate=%d",
			ts.PayloadType, ts.ClockRate,
		))); err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		rtpH265Depay, err := gst.NewElement("rtph265depay")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}
		if err = appSrcBin.AddElement(rtpH265Depay); err != nil {
			return nil, err
		}

		if !b.conf.VideoDecoding {
			h265ParseFixer, err := newPTSFixer("h265parse", fmt.Sprintf("track:%s", ts.TrackID))
			if err != nil {
				return nil, err
			}

			if err = appSrcBin.AddElement(h265ParseFixer.Element); err != nil {
				return nil, err
			}

			return appSrcBin, nil
		}

		// h265parse normalizes byte-stream NALs and surfaces VPS/SPS/PPS so
		// avdec_h265 negotiates reliably; HEVC is less forgiving than H264 here.
		h265Parse, err := gst.NewElement("h265parse")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		avDecH265, err := gst.NewElement("avdec_h265")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		if err = appSrcBin.AddElements(h265Parse, avDecH265); err != nil {
			return nil, err
		}

	case types.MimeTypeVP8:
		if err := ts.AppSrc.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"application/x-rtp,media=video,payload=%d,encoding-name=VP8,clock-rate=%d",
			ts.PayloadType, ts.ClockRate,
		))); err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		rtpVP8Depay, err := gst.NewElement("rtpvp8depay")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}
		if err = appSrcBin.AddElement(rtpVP8Depay); err != nil {
			return nil, err
		}

		if !b.conf.VideoDecoding {
			return appSrcBin, nil
		}
		vp8Dec, err := gst.NewElement("vp8dec")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}
		if err = appSrcBin.AddElement(vp8Dec); err != nil {
			return nil, err
		}

	case types.MimeTypeVP9:
		if err := ts.AppSrc.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"application/x-rtp,media=video,payload=%d,encoding-name=VP9,clock-rate=%d",
			ts.PayloadType, ts.ClockRate,
		))); err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}

		rtpVP9Depay, err := gst.NewElement("rtpvp9depay")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}
		if err = appSrcBin.AddElement(rtpVP9Depay); err != nil {
			return nil, err
		}

		if !b.conf.VideoDecoding {
			vp9ParseFixer, err := newPTSFixer("vp9parse", fmt.Sprintf("track:%s", ts.TrackID))
			if err != nil {
				return nil, err
			}
			vp9Parse := vp9ParseFixer.Element

			vp9Caps, err := gst.NewElement("capsfilter")
			if err != nil {
				return nil, errors.ErrGstPipelineError(err)
			}
			if err = vp9Caps.SetProperty("caps", gst.NewCapsFromString(
				"video/x-vp9,width=[16,2147483647],height=[16,2147483647]",
			)); err != nil {
				return nil, errors.ErrGstPipelineError(err)
			}

			if err = appSrcBin.AddElements(vp9Parse, vp9Caps); err != nil {
				return nil, err
			}
			return appSrcBin, nil
		}

		vp9Dec, err := gst.NewElement("vp9dec")
		if err != nil {
			return nil, errors.ErrGstPipelineError(err)
		}
		if err = appSrcBin.AddElement(vp9Dec); err != nil {
			return nil, err
		}

	default:
		return nil, errors.ErrNotSupported(string(ts.MimeType))
	}

	if err := b.addVideoConverter(appSrcBin); err != nil {
		return nil, err
	}

	return appSrcBin, nil
}

func (b *VideoBin) addVideoTestSrcBin() error {
	testSrcBin := b.bin.NewBin(videoTestSrcName)
	if err := b.bin.AddSourceBin(testSrcBin); err != nil {
		return err
	}

	videoTestSrc, err := gst.NewElement("videotestsrc")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = videoTestSrc.SetProperty("is-live", true); err != nil {
		return errors.ErrGstPipelineError(err)
	}
	videoTestSrc.SetArg("pattern", "black")

	queue, err := gstreamer.BuildQueue("video_test_src_queue", b.conf.Latency.PipelineLatency, false)
	if err != nil {
		return err
	}
	if err = queue.SetProperty("min-threshold-time", uint64(2e9)); err != nil {
		return errors.ErrGstPipelineError(err)
	}

	caps, err := b.newVideoCapsFilter(true)
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	if err = testSrcBin.AddElements(videoTestSrc, queue, caps); err != nil {
		return err
	}

	b.createTestSrcPad()
	return nil
}

func (b *VideoBin) addSelector() error {
	inputSelector, err := gst.NewElement("input-selector")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	videoRate, err := gst.NewElement("videorate")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = videoRate.SetProperty("skip-to-first", true); err != nil {
		return errors.ErrGstPipelineError(err)
	}

	caps, err := b.newVideoCapsFilter(true)
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	if err = b.bin.AddElements(inputSelector, videoRate, caps); err != nil {
		return err
	}

	b.selector = inputSelector
	return nil
}

func (b *VideoBin) addEncoder() error {
	videoQueue, err := gstreamer.BuildQueue("video_encoder_queue", b.conf.Latency.PipelineLatency, false)
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = b.bin.AddElement(videoQueue); err != nil {
		return err
	}

	switch b.conf.VideoOutCodec {
	// we only encode h264, the rest are too slow
	case types.MimeTypeH264:
		x264Enc, err := gst.NewElement("x264enc")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		x264Enc.SetArg("speed-preset", "veryfast")

		var options []string
		disabledSceneCut := false
		// Streaming outputs always set KeyFrameInterval, so this effectively disables scenecut for RTMP/SRT.
		if b.conf.KeyFrameInterval != 0 {
			keyframeInterval := uint(b.conf.KeyFrameInterval * float64(b.conf.Framerate))
			if err = x264Enc.SetProperty("key-int-max", keyframeInterval); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			options = append(options, "scenecut=0")
			disabledSceneCut = true
		}

		bufCapacity := uint(2000) // 2s
		if b.conf.GetSegmentConfig() != nil {
			// avoid key frames other than at segments boundaries as splitmuxsink can become inconsistent otherwise
			if !disabledSceneCut {
				options = append(options, "scenecut=0")
				disabledSceneCut = true
			}
			bufCapacity = uint(time.Duration(b.conf.GetSegmentConfig().SegmentDuration) * (time.Second / time.Millisecond))
		}
		if bufCapacity > 10000 {
			// Max value allowed by gstreamer
			bufCapacity = 10000
		}
		if err = x264Enc.SetProperty("vbv-buf-capacity", bufCapacity); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		if err = x264Enc.SetProperty("bitrate", uint(b.conf.VideoBitrate)); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		if sc := b.conf.GetStreamConfig(); sc != nil && sc.OutputType == types.OutputTypeRTMP {
			options = append(options, "nal-hrd=cbr")
		}
		if len(options) > 0 {
			optionString := strings.Join(options, ":")
			if err = x264Enc.SetProperty("option-string", optionString); err != nil {
				return errors.ErrGstPipelineError(err)
			}
		}

		caps, err := gst.NewElement("capsfilter")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = caps.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"video/x-h264,profile=%s,multiview-mode=mono,multiview-flags=(GstVideoMultiviewFlagsSet)0:ffffffff:/right-view-first/left-flipped/left-flopped/right-flipped/right-flopped/half-aspect/mixed-mono",
			b.conf.VideoProfile,
		))); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		if err = b.bin.AddElements(x264Enc, caps); err != nil {
			return err
		}
		return nil

	case types.MimeTypeH265:
		x265Enc, err := gst.NewElement("x265enc")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		// H265 was originally added with the same "veryfast" preset used for
		// H264. In real RoomComposite egress that can still run behind at
		// 1080p on the 4-vCPU canary workers, leaving the pipeline stuck in EOS
		// finalization and eventually reported as "pipeline frozen". Prefer
		// realtime stability over max compression for the canary path.
		x265Enc.SetArg("speed-preset", "ultrafast")
		x265Enc.SetArg("tune", "zerolatency")

		if b.conf.KeyFrameInterval != 0 {
			// x265enc exposes key-int-max as a signed gint, unlike x264enc's
			// guint property. Passing uint fails the pipeline at runtime with:
			// "invalid type guint for property key-int-max".
			keyframeInterval := int(b.conf.KeyFrameInterval * float64(b.conf.Framerate))
			if err = x265Enc.SetProperty("key-int-max", keyframeInterval); err != nil {
				return errors.ErrGstPipelineError(err)
			}
		}

		if err = x265Enc.SetProperty("bitrate", uint(b.conf.VideoBitrate)); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		h265Parse, err := gst.NewElement("h265parse")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		// Keep parameter sets in-band so finalized MP4s are self-contained.
		_ = h265Parse.SetProperty("config-interval", int(-1))

		caps, err := gst.NewElement("capsfilter")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = caps.SetProperty("caps", gst.NewCapsFromString(
			"video/x-h265,profile=main,stream-format=hvc1,alignment=au",
		)); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		if err = b.bin.AddElements(x265Enc, h265Parse, caps); err != nil {
			return err
		}
		return nil

	case types.MimeTypeAV1:
		// Force 4:2:0 at the encoder input. The headless-Chrome RoomComposite
		// source negotiates 4:4:4, which made av1enc emit AV1 High / yuv444p — a
		// profile no mobile hardware decoder can play, forcing software decode on
		// every viewer (and oversized files). A videoconvert + I420 capsfilter
		// pins Main / 4:2:0. SDK participant sources are already I420, so this is a
		// passthrough no-op there.
		av1InputConvert, err := gst.NewElement("videoconvert")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		av1InputCaps, err := gst.NewElement("capsfilter")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = av1InputCaps.SetProperty("caps", gst.NewCapsFromString(
			"video/x-raw,format=I420",
		)); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		// Prefer SVT-AV1 (svtav1enc): a realtime-tuned encoder that is dramatically
		// faster than libaom av1enc on the CPU-only c7g Graviton egress nodes
		// (strong NEON throughput). Together with the 30fps composite drop this
		// relieves the egress CPU starvation behind "no response from servers".
		// Fall back to libaom av1enc when the svtav1 plugin is absent so AV1 egress
		// never hard-breaks on an image that lacks it.
		av1Enc, err := gst.NewElement("svtav1enc")
		useSvtAv1 := err == nil && av1Enc != nil
		if !useSvtAv1 {
			av1Enc, err = gst.NewElement("av1enc")
			if err != nil {
				return errors.ErrGstPipelineError(err)
			}
		}

		if useSvtAv1 {
			// preset 0..13 (0 = best quality, 13 = fastest). 10 is the realtime
			// default and keeps 1080p encodes ahead of realtime on c7g.
			if err = av1Enc.SetProperty("preset", uint(10)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			// target-bitrate is in kbits/sec, matching b.conf.VideoBitrate (e.g.
			// 1875 participant / 6000 composite). SVT honors it far more tightly
			// than libaom, so no quantizer clamp is required to avoid ballooning.
			if err = av1Enc.SetProperty("target-bitrate", uint(b.conf.VideoBitrate)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			// Closed-GOP IDR keyframes at the requested interval (in frames).
			if b.conf.KeyFrameInterval != 0 {
				intra := int(b.conf.KeyFrameInterval * float64(b.conf.Framerate))
				if err = av1Enc.SetProperty("intra-period-length", intra); err != nil {
					return errors.ErrGstPipelineError(err)
				}
			}
			// 0 = use all available logical cores.
			if err = av1Enc.SetProperty("logical-processors", uint(0)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
		} else {
			// libaom av1enc fallback (slower). usage-profile/end-usage are
			// GstAV1Enc enums; go-glib needs a native enum GValue. The quantizer
			// clamp keeps libaom from ignoring target-bitrate and ballooning to
			// 50+Mbps in realtime mode.
			if err = setGstEnumProperty(av1Enc, "usage-profile", 1); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			if err = setGstEnumProperty(av1Enc, "end-usage", 1); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			if err = av1Enc.SetProperty("min-quantizer", uint(4)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			if err = av1Enc.SetProperty("max-quantizer", uint(56)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			if err = av1Enc.SetProperty("cpu-used", int(8)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			if err = av1Enc.SetProperty("row-mt", true); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			if err = av1Enc.SetProperty("threads", uint(0)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
			if b.conf.KeyFrameInterval != 0 {
				keyframeInterval := int(b.conf.KeyFrameInterval * float64(b.conf.Framerate))
				if err = av1Enc.SetProperty("keyframe-max-dist", keyframeInterval); err != nil {
					return errors.ErrGstPipelineError(err)
				}
			}
			if err = av1Enc.SetProperty("target-bitrate", uint(b.conf.VideoBitrate)); err != nil {
				return errors.ErrGstPipelineError(err)
			}
		}

		av1Parse, err := gst.NewElement("av1parse")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}

		caps, err := gst.NewElement("capsfilter")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = caps.SetProperty("caps", gst.NewCapsFromString(
			"video/x-av1,stream-format=obu-stream,alignment=tu",
		)); err != nil {
			return errors.ErrGstPipelineError(err)
		}

		if err = b.bin.AddElements(av1InputConvert, av1InputCaps, av1Enc, av1Parse, caps); err != nil {
			return err
		}
		return nil

	case types.MimeTypeVP9:
		vp9Enc, err := gst.NewElement("vp9enc")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = vp9Enc.SetProperty("deadline", int64(1)); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = vp9Enc.SetProperty("row-mt", true); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = vp9Enc.SetProperty("tile-columns", 3); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = vp9Enc.SetProperty("tile-rows", 1); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = vp9Enc.SetProperty("frame-parallel", true); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = vp9Enc.SetProperty("max-quantizer", 52); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = vp9Enc.SetProperty("min-quantizer", 2); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = b.bin.AddElement(vp9Enc); err != nil {
			return err
		}

		fallthrough

	default:
		return errors.ErrNotSupported(fmt.Sprintf("%s encoding", b.conf.VideoOutCodec))
	}
}

func (b *VideoBin) addDecodedVideoSink() error {
	var err error
	b.rawVideoTee, err = gst.NewElement("tee")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	if err = b.bin.AddElement(b.rawVideoTee); err != nil {
		return err
	}

	if b.conf.VideoEncoding {
		err = b.addEncoder()
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *VideoBin) addVideoConverter(bin *gstreamer.Bin) error {
	videoQueue, err := b.buildVideoQueue("video_input_queue")
	if err != nil {
		return err
	}

	videoConvert, err := gst.NewElement("videoconvert")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	videoScale, err := gst.NewElement("videoscale")
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}

	elements := []*gst.Element{videoQueue, videoConvert, videoScale}

	if !b.conf.VideoDecoding {
		videoRate, err := gst.NewElement("videorate")
		if err != nil {
			return errors.ErrGstPipelineError(err)
		}
		if err = videoRate.SetProperty("skip-to-first", true); err != nil {
			return errors.ErrGstPipelineError(err)
		}
		elements = append(elements, videoRate)
	}

	caps, err := b.newVideoCapsFilter(!b.conf.VideoDecoding)
	if err != nil {
		return errors.ErrGstPipelineError(err)
	}
	elements = append(elements, caps)

	return bin.AddElements(elements...)
}

func (b *VideoBin) newVideoCapsFilter(includeFramerate bool) (*gst.Element, error) {
	caps, err := gst.NewElement("capsfilter")
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	if includeFramerate {
		err = caps.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"video/x-raw,framerate=%d/1,format=I420,width=%d,height=%d,colorimetry=bt709,chroma-site=mpeg2,pixel-aspect-ratio=1/1",
			b.conf.Framerate, b.conf.Width, b.conf.Height,
		)))
	} else {
		err = caps.SetProperty("caps", gst.NewCapsFromString(fmt.Sprintf(
			"video/x-raw,format=I420,width=%d,height=%d,colorimetry=bt709,chroma-site=mpeg2,pixel-aspect-ratio=1/1",
			b.conf.Width, b.conf.Height,
		)))
	}
	if err != nil {
		return nil, errors.ErrGstPipelineError(err)
	}
	return caps, nil
}

func (b *VideoBin) getSrcPad(name string) *gst.Pad {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.pads[name]
}

func (b *VideoBin) createSrcPad(trackID, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.createSrcPadLocked(trackID, name)
}

func (b *VideoBin) createSrcPadLocked(trackID, name string) {
	b.names[trackID] = name

	pad := b.selector.GetRequestPad("sink_%u")
	pad.AddProbe(gst.PadProbeTypeBuffer, func(_ *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
		pts := uint64(info.GetBuffer().PresentationTimestamp())
		b.mu.Lock()
		if pts < b.lastPTS || (b.selectedPad != videoTestSrcName && b.selectedPad != name) {
			b.mu.Unlock()
			return gst.PadProbeDrop
		}
		b.lastPTS = pts
		b.mu.Unlock()
		return gst.PadProbeOK
	})

	b.pads[name] = pad
}

func (b *VideoBin) createTestSrcPad() {
	b.mu.Lock()
	defer b.mu.Unlock()

	pad := b.selector.GetRequestPad("sink_%u")
	pad.AddProbe(gst.PadProbeTypeBuffer, func(_ *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
		pts := uint64(info.GetBuffer().PresentationTimestamp())
		b.mu.Lock()
		if pts < b.lastPTS || (b.selectedPad != videoTestSrcName) {
			b.mu.Unlock()
			return gst.PadProbeDrop
		}
		b.lastPTS = pts
		b.mu.Unlock()
		return gst.PadProbeOK
	})

	b.pads[videoTestSrcName] = pad
}

func (b *VideoBin) setSelectorPad(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.setSelectorPadLocked(name)
}

func (b *VideoBin) setSelectorPadLocked(name string) error {
	pad := b.pads[name]

	// drop until the next keyframe
	pad.AddProbe(gst.PadProbeTypeBuffer, func(_ *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
		buffer := info.GetBuffer()
		if buffer.HasFlags(gst.BufferFlagDeltaUnit) {
			return gst.PadProbeDrop
		}
		logger.Debugw("active pad changed", "name", name)
		return gst.PadProbeRemove
	})

	if err := b.selector.SetProperty("active-pad", pad); err != nil {
		return errors.ErrGstPipelineError(err)
	}

	b.selectedPad = name
	return nil
}
