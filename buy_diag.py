#!/usr/bin/env python3
# DIAG beli item via RoomEvent "buy" (ToxicBirdBuyMechanic) — 1 bot tes.
# Dipakai untuk membeli terrain 4706 (1 koin) di akun pkxdshop supaya
# copy UGC TROPlCAL PARADlSE bisa create (terrain konsumabel: 1 beli =
# 1 world; pembelian sebelumnya habis dipakai membuat W0RLD WAR 3).
#
# Wire (dump_3, TERBUKTI via disassembly):
# - kirim RoomEvent "buy" (BuyItemCommandKey), balas RoomEvent "d"
#   (BuyResultCommandKey).
# - BuyItemRequest: f1 EventId (GamePurchaseEventId: 0 BuyCommonItem,
#   1 BuyOffer, 2 BuyLootbox — 0 dihilangkan seperti proto3 default),
#   f2 Items repeated int32 (packed), f3 StoreIds repeated string.
# - Untuk terrain builder: StoreId = "ugc-builder-in-game-store-1_91_0"
#   (store tree UGC, sama seperti f4 StoreTreeId di edit draft).
# - Respons "d" di-dump generik (hex + parse protobuf) karena nomor field
#   respons BuyResponse belum dibongkar penuh; bukti kepemilikan yang
#   menentukan adalah create draft di tes copy sesudahnya.
# - Kirim buy TEPAT 1x; bila respons hilang, JANGAN kirim ulang membabi
#   buta (bisa beli ganda) — verifikasi lewat create draft / metadata.
# - Harga yang terlihat di toko game untuk terrain ini: 1 koin.
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "probe-buy-001"
DEFAULT_STORE_ID = "ugc-builder-in-game-store-1_91_0"
DEFAULT_ITEM_ID = 4706
EVENT = "buy"
RESULT_EVENT = "d"
STEP_TIMEOUT = 20


def _varint_bytes(value):
    out = bytearray()
    v = value
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _str_field(field_no, value):
    b = value.encode("utf-8")
    return bytes([(field_no << 3) | 2]) + _varint_bytes(len(b)) + b


def _try_str(chunk):
    try:
        s = chunk.decode("utf-8")
    except Exception:
        return None
    if not s or any(ord(c) < 0x20 and c not in "\n\r\t" for c in s):
        return None
    return s


def parse_protobuf(buf):
    """Parse generik protobuf -> dict {field: value}. Nilai bytes yang
    tampak seperti string diubah ke str; sub-message di-parse rekursif."""
    out = {}
    i = 0
    n = len(buf)
    while i < n:
        try:
            tag = buf[i]
            i += 1
            field = tag >> 3
            wt = tag & 7
            if wt == 0:
                v = 0
                shift = 0
                while i < n:
                    b = buf[i]
                    i += 1
                    v |= (b & 0x7F) << shift
                    shift += 7
                    if not (b & 0x80):
                        break
                out.setdefault(field, []).append(v)
            elif wt == 2:
                ln = 0
                shift = 0
                while i < n:
                    b = buf[i]
                    i += 1
                    ln |= (b & 0x7F) << shift
                    shift += 7
                    if not (b & 0x80):
                        break
                chunk = buf[i:i + ln]
                i += ln
                s = _try_str(chunk)
                if s is not None:
                    out.setdefault(field, []).append(s)
                else:
                    try:
                        sub = parse_protobuf(chunk)
                        out.setdefault(field, []).append(sub)
                    except Exception:
                        out.setdefault(field, []).append(
                            chunk.hex())
            elif wt == 5:
                out.setdefault(field, []).append(
                    buf[i:i + 4].hex())
                i += 4
            elif wt == 1:
                out.setdefault(field, []).append(
                    buf[i:i + 8].hex())
                i += 8
            else:
                break
        except Exception:
            break
    return out


def _jsonable(v):
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, bytes):
        return v.hex()
    return v


def build_buy_payload(item_id, store_id):
    """BuyItemRequest: f2 Items packed [item_id], f3 StoreIds [store_id].
    f1 EventId=0 (BuyCommonItem) dihilangkan (proto3 default)."""
    out = bytearray()
    packed = _varint_bytes(item_id)
    out.append((2 << 3) | 2)
    out += _varint_bytes(len(packed))
    out += packed
    out += _str_field(3, store_id)
    return bytes(out)


async def buy_once(session, email, password, device_id, item_id, store_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "item_id": item_id, "store_id": store_id,
             "login_ok": False, "deviceid_sent": False,
             "buy_sent": False, "result": None, "result_hex": None,
             "error": None}
    user_jwt, alloc = await L.login_and_allocate(email, password,
                                                 device_id, session)
    if not alloc:
        hasil["error"] = "login/alokasi gagal"
        print("[!] login/alokasi gagal.")
        return hasil
    hasil["login_ok"] = True
    host, port = L.find_host_port(alloc)
    if not host or not port:
        hasil["error"] = "host/port tidak ketemu di allocation"
        print("[!] host/port tidak ketemu.")
        return hasil
    print(f"[+] alokasi OK: {host}:{port}")
    sock, pid = L.tcp_connect(host, port)
    if sock is None:
        hasil["error"] = "TCP connect gagal"
        print("[!] TCP connect gagal.")
        return hasil
    print(f"[+] Connected: playerId={pid}.")
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        hasil["error"] = f"kirim login frame gagal: {exc}"
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = G.FrameReader(sock)
    try:
        print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
        got, closed = fr.read_until(
            lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
        if closed:
            hasil["error"] = "koneksi tertutup saat tunggu login"
            print("[!] koneksi tertutup saat tunggu login.")
            return hasil
        if got is not None and G._is_tbugv(got):
            print("[*] tbugv diterima — observasi 1 dtk lagi.")
            time.sleep(1)
        elif got is not None:
            print("[*] JoinedRoom diterima.")
        else:
            print("[*] timeout jendela login (koneksi hidup) — lanjut kirim.")
        send_dev = device_id or DEFAULT_DEVICE_ID
        try:
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId", send_dev.encode("utf-8")))
            hasil["deviceid_sent"] = True
            print("[+] SendDeviceId terkirim ('Analytics/deviceId').")
        except Exception as exc:
            print(f"[!] kirim SendDeviceId gagal: {exc}")

        payload = build_buy_payload(item_id, store_id)
        print(f"[*] BUY item {item_id} store {store_id!r}: "
              f"kirim 'buy' ({len(payload)} B): {payload.hex()}")
        try:
            sock.sendall(L.build_room_event_varint(EVENT, payload))
        except Exception as exc:
            hasil["error"] = f"kirim buy gagal: {exc}"
            print(f"[!] kirim buy gagal: {exc}")
            return hasil
        hasil["buy_sent"] = True
        # Tunggu respons 'd' (20 dtk), catat semua event yang lewat.
        end = time.time() + STEP_TIMEOUT
        data = None
        while time.time() < end:
            for f in fr.drain(0.5):
                ev_name, d = G.decode_room_event(f)
                if ev_name is not None:
                    print(f"[.] event lewat: '{ev_name}' "
                          f"({len(d) if d else 0} B)")
                if ev_name == RESULT_EVENT:
                    data = d
                    break
            if data is not None:
                break
        if data is None:
            hasil["error"] = "tidak ada respons 'd' dalam 20 dtk"
            print("[!] tidak ada respons 'd' dalam 20 dtk — pembelian "
                  "mungkin tetap diproses server; verifikasi via create "
                  "draft, JANGAN beli ulang membabi buta.")
            return hasil
        hasil["result_hex"] = data.hex()
        try:
            hasil["result"] = parse_protobuf(data)
        except Exception as exc:
            hasil["result"] = {"parse_error": str(exc)}
        print(f"[+] respons 'd' ({len(data)} B): {data.hex()}")
        print(f"[+] dump: {json.dumps(_jsonable(hasil['result']), ensure_ascii=False)}")
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("DB_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("DB_DEVICE_ID", ""))
    ap.add_argument("--item-id", type=int, default=int(
        os.getenv("DB_ITEM_ID", str(DEFAULT_ITEM_ID)) or DEFAULT_ITEM_ID))
    ap.add_argument("--store-id", default=os.getenv(
        "DB_STORE_ID", DEFAULT_STORE_ID))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    payload = build_buy_payload(args.item_id, args.store_id)
    print("========== RENCANA BELI (BUY DIAG) ==========")
    print(f"Item: {args.item_id}  Store: {args.store_id!r}")
    print(f"Payload BUY ({len(payload)} B): {payload.hex()}")
    print("=============================================")
    if args.dry_run:
        print("[*] DRY-RUN: tidak ada jaringan game, selesai.")
        return 0
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi (mode live).")
        return 1
    L.apply_panel_versions()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await buy_once(
                session, args.email, args.password, args.device_id,
                args.item_id, args.store_id)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "item_id": args.item_id, "store_id": args.store_id,
                     "login_ok": False, "deviceid_sent": False,
                     "buy_sent": False, "result": None,
                     "result_hex": None,
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] buy_once gagal: {type(exc).__name__}: {exc}")
    print("")
    print("========== HASIL BELI ==========")
    print(f"buy_sent: {hasil.get('buy_sent')}")
    print(f"result_hex: {hasil.get('result_hex')}")
    print(f"result: {json.dumps(_jsonable(hasil.get('result')), ensure_ascii=False)}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("================================")
    print("BUY_JSON:" + json.dumps(_jsonable(hasil),
                                   separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
