import asyncio
import websockets
import json

sender_ws             = None
receiver_ws           = None
cached_offer          = None
cached_sender_ice     = []
waiting_for_new_offer = False   # True between request_offer and the new offer
pending_ws            = set()   # connected but not yet identified


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
    global waiting_for_new_offer, pending_ws

    print("[+] New client connected")

    try:
        async for raw in websocket:
            data     = json.loads(raw)
            msg_type = data.get("type", "?")
            who = ("sender"   if websocket is sender_ws  else
                   "receiver" if websocket is receiver_ws else "pending")
            print(f"[MSG] {msg_type} from {who}")

            if msg_type == "offer":
                # This socket is identifying itself as the sender
                pending_ws.discard(websocket)
                if websocket is receiver_ws:
                    print("[!] Was receiver → promoted to sender")
                    receiver_ws = None
                sender_ws             = websocket
                cached_offer          = data
                cached_sender_ice     = []
                waiting_for_new_offer = False   # new offer received — release the wait
                print("[Sender] Offer cached — forwarding to receiver if present")
                if receiver_ws is not None and receiver_ws is not websocket:
                    if await safe_send(receiver_ws, raw):
                        print("[->] Offer → receiver")

            elif msg_type == "answer":
                # This socket is identifying itself as the receiver
                pending_ws.discard(websocket)
                receiver_ws = websocket
                if waiting_for_new_offer:
                    # answer based on the old (cached) offer — stale
                    print("[!] Stale answer ignored (waiting for fresh offer from sender)")
                    continue
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
                    if waiting_for_new_offer:
                        print("[!] Stale ICE from receiver ignored")
                        continue
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
        pending_ws.discard(websocket)
        if websocket is sender_ws:
            print("[-] Sender disconnected")
            sender_ws = None
        elif websocket is receiver_ws:
            print("[-] Receiver disconnected")
            receiver_ws = None


async def on_new_connection(websocket):
    global receiver_ws, waiting_for_new_offer, pending_ws

    if cached_offer is not None and websocket is not sender_ws:
        # There's a cached offer — treat this as a receiver right away
        receiver_ws = websocket
        pending_ws.discard(websocket)
        print("[->] Receiver connected — pushing cached offer")
        await safe_send(websocket, json.dumps(cached_offer))
        for ice in cached_sender_ice:
            await safe_send(websocket, json.dumps(ice))
        # Ask sender for a fresh offer for this new session
        if sender_ws is not None:
            waiting_for_new_offer = True
            print("[->] request_offer → sender (answers/ICE stale until fresh offer)")
    elif cached_offer is None and websocket is not sender_ws:
        # No offer yet — keep this connection in pending until it identifies itself
        pending_ws.add(websocket)
        print("[->] Pre-offer connection — pending (will identify on first message)")
        if sender_ws is not None:
            await safe_send(sender_ws, json.dumps({"type": "request_offer"}))

    await handler(websocket)


async def main():
    print("Signaling server on ws://0.0.0.0:8765")
    async with websockets.serve(on_new_connection, "0.0.0.0", 8765):
        await asyncio.Future()


asyncio.run(main())