import asyncio
import json
import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstWebRTC", "1.0")
gi.require_version("GstSdp", "1.0")
from gi.repository import Gst, GstWebRTC, GstSdp

import websockets

Gst.init(None)

SIGNALING_SERVER = "ws://signaling:8765"
#"videotestsrc is-live=true pattern=ball ! "
PIPELINE_DESC = (
    "webrtcbin name=sendrecv bundle-policy=max-bundle "
    "stun-server=stun://stun.l.google.com:19302 "
    "v4l2src device=/dev/video0 ! videoconvert ! "
    "video/x-raw,framerate=30/1,width=640,height=480 ! "
    "videoconvert ! queue ! vp8enc deadline=1 keyframe-max-dist=30 ! "
    "rtpvp8pay ! "
    "queue ! application/x-rtp,media=video,encoding-name=VP8,payload=97 ! "
    "sendrecv."
)


def check_plugins():
    needed = ["vpx", "nice", "webrtc", "dtls", "srtp", "rtp",
              "rtpmanager", "videotestsrc"]
    missing = [p for p in needed
               if Gst.Registry.get().find_plugin(p) is None]
    if missing:
        print(f"[ERROR] Missing plugins: {missing}")
        return False
    return True


class WebRTCClient:
    def __init__(self):
        self.pipe    = None
        self.webrtc  = None
        self.ws      = None
        self._loop   = None
        self._q: asyncio.Queue = None

    # ── Pipeline ──────────────────────────────────────────────────────────

    def start_pipeline(self):
        self.pipe   = Gst.parse_launch(PIPELINE_DESC)
        self.webrtc = self.pipe.get_by_name("sendrecv")
        if self.webrtc is None:
            raise RuntimeError("webrtcbin 'sendrecv' not found")

        self.webrtc.connect("on-negotiation-needed", self._on_negotiation_needed)
        self.webrtc.connect("on-ice-candidate",      self._on_ice_candidate)
        self.webrtc.connect("pad-added",             self._on_pad_added)

        bus = self.pipe.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_bus_message)

        self.pipe.set_state(Gst.State.PLAYING)
        print("[GST] Pipeline started")

    def _stop_pipeline(self):
        """Stop and destroy the GStreamer pipeline cleanly."""
        if self.pipe:
            print("[GST] Stopping pipeline…")
            self.pipe.set_state(Gst.State.NULL)
            self.pipe   = None
            self.webrtc = None

    def _on_pad_added(self, element, pad):
        try:
            transceivers = element.emit("get-transceivers")
            if transceivers:
                transceivers[0].set_property(
                    "direction",
                    GstWebRTC.WebRTCRTPTransceiverDirection.SENDONLY,
                )
                print("[GST] Transceiver[0] → SENDONLY")
        except Exception as e:
            print(f"[WARN] pad-added: {e}")

    def _on_bus_message(self, bus, message):
        t = message.type
        if t == Gst.MessageType.ERROR:
            err, dbg = message.parse_error()
            print(f"[GST ERROR] {err.message} | {dbg}")
        elif t == Gst.MessageType.WARNING:
            w, _ = message.parse_warning()
            print(f"[GST WARN] {w.message}")

    # ── SDP ───────────────────────────────────────────────────────────────

    def _on_negotiation_needed(self, element):
        print("[GST] on-negotiation-needed → create-offer")
        promise = Gst.Promise.new_with_change_func(
            self._on_offer_created, element, None
        )
        element.emit("create-offer", None, promise)

    def _on_offer_created(self, promise, element, _):
        if promise.wait() != Gst.PromiseResult.REPLIED:
            print("[ERROR] create-offer: promise not replied")
            return
        reply = promise.get_reply()
        offer = reply["offer"]

        p = Gst.Promise.new()
        self.webrtc.emit("set-local-description", offer, p)
        p.interrupt()

        sdp_text = offer.sdp.as_text()
        print(f"[GST] Offer SDP ready ({len(sdp_text)} chars)")

        self._loop.call_soon_threadsafe(
            self._q.put_nowait,
            {"type": "offer", "sdp": sdp_text},
        )

    def _handle_answer(self, sdp_text):
        print("[GST] Answer → set-remote-description")
        res, sdp = GstSdp.SDPMessage.new_from_text(sdp_text)
        if res != GstSdp.SDPResult.OK:
            print(f"[ERROR] Parsing SDP answer: {res}")
            return
        answer = GstWebRTC.WebRTCSessionDescription.new(
            GstWebRTC.WebRTCSDPType.ANSWER, sdp
        )
        p = Gst.Promise.new()
        self.webrtc.emit("set-remote-description", answer, p)
        p.interrupt()

    # ── ICE ───────────────────────────────────────────────────────────────

    def _on_ice_candidate(self, element, mlineindex, candidate):
        self._loop.call_soon_threadsafe(
            self._q.put_nowait,
            {"type": "ice", "candidate": candidate, "sdpMLineIndex": mlineindex},
        )

    def _handle_ice(self, candidate, mlineindex):
        self.webrtc.emit("add-ice-candidate", mlineindex, candidate)

    # ── WebSocket ─────────────────────────────────────────────────────────

    def _flush_queue(self):
        """Flush expired ICE/offer messages from the queue."""
        n = 0
        while not self._q.empty():
            try:
                self._q.get_nowait()
                n += 1
            except asyncio.QueueEmpty:
                break
        if n:
            print(f"[WS] {n} expired messages flushed from queue")

    async def _restart_pipeline(self):
        """Destroy and recreate the GStreamer pipeline.
        This is necessary because webrtcbin does not support re-negotiation
        from the 'stable' state without recreating the object.
        """
        print("[GST] Restart pipeline for new session…")
        self._stop_pipeline()
        self._flush_queue()
        await asyncio.sleep(0.3)   # allow GStreamer to release resources
        self.start_pipeline()
        print("[GST] Pipeline restarted — new offer in progress")

    async def _writer(self):
        while True:
            msg = await self._q.get()
            print(f"[WS→] {msg.get('type')}")
            try:
                await self.ws.send(json.dumps(msg))
            except (websockets.exceptions.ConnectionClosedError,
                    websockets.exceptions.ConnectionClosedOK) as e:
                print(f"[WS] Closed during send: {e}")
                break
            except Exception as e:
                print(f"[WS] Send error {msg.get('type')}: {e}")
                break

    async def _listener(self):
        async for raw in self.ws:
            data     = json.loads(raw)
            msg_type = data.get("type")
            print(f"[WS←] {msg_type}")
            if msg_type == "offer":
                print("[WS] Offer ignored (own offer echoed back)")
            elif msg_type == "answer":
                self._handle_answer(data["sdp"])
            elif msg_type == "ice":
                self._handle_ice(data["candidate"], data["sdpMLineIndex"])
            elif msg_type == "request_offer":
                print("[WS] request_offer → full restart pipeline")
                # Do not await here to avoid blocking the listener
                asyncio.ensure_future(self._restart_pipeline())

    async def run(self):
        self._loop = asyncio.get_running_loop()
        self._q    = asyncio.Queue()

        self.start_pipeline()

        first_connect = True
        while True:
            try:
                print(f"[WS] Connecting to {SIGNALING_SERVER}…")
                self.ws = await websockets.connect(SIGNALING_SERVER)
                print("[WS] Connected")

                if not first_connect:
                    # WS reconnection (e.g., signaling restarted):
                    # recreate the pipeline to generate a fresh offer
                    self._flush_queue()
                    print("[WS] Reconnecting — restart pipeline")
                    await self._restart_pipeline()
                first_connect = False

                writer_task   = asyncio.create_task(self._writer())
                listener_task = asyncio.create_task(self._listener())

                done, pending = await asyncio.wait(
                    [writer_task, listener_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
                for t in done:
                    exc = t.exception()
                    if exc:
                        print(f"[WS] Task error: {exc}")

            except (OSError, websockets.exceptions.WebSocketException) as e:
                print(f"[WS] Connection error: {e}")

            print("[WS] Retry in 3s…")
            await asyncio.sleep(3)


async def main():
    if not check_plugins():
        return
    client = WebRTCClient()
    try:
        await client.run()
    except asyncio.CancelledError:
        pass
    finally:
        if client.pipe:
            client.pipe.set_state(Gst.State.NULL)


if __name__ == "__main__":
    asyncio.run(main())