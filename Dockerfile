FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV GST_DEBUG=2
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-gi \
    python3-gst-1.0 \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gir1.2-gst-plugins-bad-1.0 \
    gstreamer1.0-nice \
    libnice10 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir websockets

WORKDIR /app

COPY signaling.py .
COPY webrtc_sender.py .
COPY receiver.html .

EXPOSE 8765

CMD ["python3", "signaling.py"]