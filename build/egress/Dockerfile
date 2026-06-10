# Copyright 2023 LiveKit, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG TEMPLATE_TAG=latest

FROM livekit/egress-templates:$TEMPLATE_TAG AS template

# Build the SVT-AV1 GStreamer encoder plugin (svtav1enc) against the pinned
# GStreamer 1.24.12. The upstream livekit/gstreamer image is built without
# libsvtav1enc-dev, so svtav1enc is absent; building just this one plugin avoids
# a full from-source GStreamer base rebuild. video.go prefers svtav1enc (fast
# realtime AV1 on the CPU-only c7g nodes) and falls back to libaom av1enc.
FROM livekit/gstreamer:1.24.12-dev AS svtav1
ARG SVT_GST_BAD_VERSION=1.24.12
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsvtav1enc-dev wget xz-utils && \
    cd /tmp && \
    wget -q https://gstreamer.freedesktop.org/src/gst-plugins-bad/gst-plugins-bad-${SVT_GST_BAD_VERSION}.tar.xz && \
    tar xf gst-plugins-bad-${SVT_GST_BAD_VERSION}.tar.xz && \
    cd gst-plugins-bad-${SVT_GST_BAD_VERSION} && \
    meson setup build -Dauto_features=disabled -Dsvtav1=enabled && \
    ninja -C build && \
    install -D build/ext/svtav1/libgstsvtav1.so /out/libgstsvtav1.so

FROM livekit/gstreamer:1.24.12-dev AS builder

ARG TARGETPLATFORM
ARG TARGETARCH
ENV TARGETARCH=${TARGETARCH}
ENV TARGETPLATFORM=${TARGETPLATFORM}

WORKDIR /workspace

# install go
RUN wget https://go.dev/dl/go1.26.2.linux-${TARGETARCH}.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf go1.26.2.linux-${TARGETARCH}.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"

# download go modules
COPY go.mod .
COPY go.sum .
RUN go mod download

# copy source
COPY cmd/ cmd/
COPY pkg/ pkg/
COPY version/ version/

# copy templates
COPY --from=template workspace/build/ cmd/server/templates/
# delete .map files
RUN find cmd/server/templates/ -name *.map | xargs rm

# build
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} GO111MODULE=on GODEBUG=disablethp=1 go build -a -o egress ./cmd/server

# install tini
ENV TINI_VERSION v0.19.0

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TARGETARCH} /tini
RUN chmod +x /tini

FROM livekit/gstreamer:1.24.12-prod

ARG TARGETPLATFORM

# install deps
RUN apt-get update && \
    apt-get install -y \
    curl \
    fonts-noto \
    gnupg \
    pulseaudio \
    unzip \
    wget \
    xvfb \
    gstreamer1.0-plugins-base-

# install chrome
COPY --from=livekit/chrome-installer:146.0.7680.177-1 /chrome-installer /chrome-installer

RUN /chrome-installer/install-chrome "$TARGETPLATFORM"

# clean up
RUN rm -rf /var/lib/apt/lists/*

# SVT-AV1: runtime library + the svtav1enc plugin built in the svtav1 stage.
# Installs into the same plugin dir as the existing libgstaom.so (av1enc).
RUN apt-get update && apt-get install -y --no-install-recommends libsvtav1enc1d1 && \
    rm -rf /var/lib/apt/lists/*
COPY --from=svtav1 /out/libgstsvtav1.so /usr/lib/aarch64-linux-gnu/gstreamer-1.0/

# create egress user
RUN useradd -ms /bin/bash -g root -G sudo,pulse,pulse-access egress
RUN mkdir -p home/egress/tmp home/egress/.cache/xdgr && \
    chown -R egress /home/egress

# copy files
COPY --from=builder /workspace/egress /bin/
COPY --from=builder /tini /tini
COPY build/egress/entrypoint.sh /

# run
USER egress
ENV PATH=${PATH}:/chrome
ENV XDG_RUNTIME_DIR=/home/egress/.cache/xdgr
ENV CHROME_DEVEL_SANDBOX=/usr/local/sbin/chrome-devel-sandbox
ENTRYPOINT ["/entrypoint.sh"]
