# PocWebRTC

A proof-of-concept WebRTC sender built with the GStreamer framework.

## Build the Solution

```bash
docker compose build --no-cache
```

## Start the Solution

```bash
docker compose up -d
```

## Access the Web Interface

Open your browser and navigate to:

```text
http://localhost:8081/
```

## Firefox Configuration

For Firefox-specific settings, refer to:

```text
firefox_conf.txt
```

## Tested Browsers

* Firefox 151.0.3 (64-bit)
* Chromium 148.0.7778.215 (Official Build)

Tested on:

* Ubuntu 24.04.4 LTS (64-bit)

## Known Issues

### Reconnection Handling

When the WebRTC connection is lost, automatic reconnection is not yet implemented.

**TODO:** Implement reliable reconnection and session recovery.
etc


