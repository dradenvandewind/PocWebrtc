import asyncio
import websockets
import json

sender_ws         = None
receiver_ws       = None
cached_offer      = None
cached_sender_ice = []


async def safe_send(ws, data):
    """Send a message without checking .open — compatible with websockets >= 10."""
    if ws is None:
        return False
    try:
        await ws.send(data)
        return True
    except (websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK,
            Exception):
        return False


async def handler(websocket):
    global sender_ws, receiver_ws, cached_offer, cached_sender_ice

    print("[+] New client connected")

    try:
        async for raw in websocket:
            data     = json.loads(raw)
            msg_type = data.get("type", "?")
            who = ("sender"   if websocket is sender_ws  else
                   "receiver" if websocket is receiver_ws else "unknown")
            print(f"[MSG] {msg_type} from {who}")

            if msg_type == "offer":
                if websocket is receiver_ws:
                    print("[!] Was temporary receiver → promoted to sender")
                    receiver_ws = None
                sender_ws         = websocket
                cached_offer      = data
                cached_sender_ice = []
                print("[Sender] Offer cached")
                if receiver_ws is not None and receiver_ws is not websocket:
                    if await safe_send(receiver_ws, raw):
                        print("[->] Offer → receiver")

            elif msg_type == "answer":
                receiver_ws = websocket
                print("[Receiver] Answer received")
                if sender_ws is not None and sender_ws is not websocket:
                    if await safe_send(sender_ws, raw):
                        print("[->] Answer → sender")
                    else:
                        print("[!] Sender absent, answer lost")

            elif msg_type == "ice":
                if websocket is sender_ws:
                    if receiver_ws is not None:
                        if not await safe_send(receiver_ws, raw):
                            cached_sender_ice.append(data)
                            print("[Cache] ICE sender buffered")
                    else:
                        cached_sender_ice.append(data)
                        print("[Cache] ICE sender buffered (no receiver)")
                else:
                    if sender_ws is not None:
                        if not await safe_send(sender_ws, raw):
                            print("[!] Sender unreachable, ICE receiver lost")
                    else:
                        print("[!] Sender absent, ICE receiver lost")

    except (websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.ConnectionClosedOK) as e:
        print(f"[-] Connection closed: {e}")
    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        import traceback; traceback.print_exc()
    finally:
        if websocket is sender_ws:
            print("[-] Sender disconnected")
            sender_ws = None
        elif websocket is receiver_ws:
            print("[-] Receiver disconnected")
            receiver_ws = None


async def on_new_connection(websocket):
    global receiver_ws

    if cached_offer is not None and websocket is not sender_ws:
        receiver_ws = websocket
        print("[->] Receiver connected — pushing cached offer")
        await safe_send(websocket, json.dumps(cached_offer))
        for ice in cached_sender_ice:
            await safe_send(websocket, json.dumps(ice))
    elif cached_offer is None and websocket is not sender_ws:
        receiver_ws = websocket
        print("[->] Pre-offer connection — temporary receiver")
        if sender_ws is not None:
            await safe_send(sender_ws, json.dumps({"type": "request_offer"}))

    await handler(websocket)


async def main():
    print("Signaling server on ws://0.0.0.0:8765")
    async with websockets.serve(on_new_connection, "0.0.0.0", 8765):
        await asyncio.Future()


asyncio.run(main())