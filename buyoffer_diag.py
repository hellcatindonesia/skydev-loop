#!/usr/bin/env python3
# DIAG beli paket toko via RoomEvent "buy" (ToxicBirdBuyMechanic) — 1 bot tes.
# Dipakai untuk membeli 1 paket koin/gems (Bartes, 2026-11-28): offer
# dumbells_gym_furniture, 9.900 koin, di akun pkxdshop.
#
# Wire (dump_3, TERBUKTI via deklarasi + BuyResponse):
# - kirim RoomEvent "buy" (BuyItemCommandKey), balas RoomEvent "d"
#   (BuyResultCommandKey).
# - BuyOfferRequest (protobuf): f1 EventId (GamePurchaseEventId:
#   0 BuyCommonItem, 1 BuyOffer, 2 BuyLootbox), f2 OfferId string,
#   f3 OfferRevision int32, f4 StoreId string, f5 OfferGroupId string.
#   TIDAK ada harga/jumlah/daftar item di payload — server yang lookup
#   dari katalog (revisi ikut divalidasi).
# - Respons "d" di-dump generik (hex + parse protobuf) karena nomor
#   field pesan respons di wire belum dibongkar penuh; model client
#   BuyResponse (Success, Items, Wallet, Spent, Received, Pets,
#   Vehicles, HouseTemplateItems, ErrorResponseCode, ErrorMessage)
#   dipakai sebagai panduan semantik saja — JANGAN klaim field
#   tertentu tanpa bukti dari dump respons.
# - Kirim buy TEPAT 1x per run; bila respons hilang, JANGAN kirim
#   ulang membabi buta (bisa beli ganda) — verifikasi lewat respons /
#   wallet di run berikutnya, bukan beli ulang.
# - Pengaman: mode live WAJIB --confirm 1 (atau DB_CONFIRM=1).
#   Tanpa itu script hanya mencetak rencana + payload lalu berhenti.
#   Harga yang terlihat di katalog untuk offer ini: 9.900 koin.
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "probe-buyoffer-001"
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


def build_buyoffer_payload(offer_id, revision, store_id, offer_group_id):
    """BuyOfferRequest: f1 EventId=1 (BuyOffer), f2 OfferId, f3 revision,
    f4 StoreId, f5 OfferGroupId."""
    out = bytearray()
    out.append((1 << 3) | 0)
    out += _varint_bytes(1)
    out += _str_field(2, offer_id)
    out.append((3 << 3) | 0)
    out += _varint_bytes(revision)
    out += _str_field(4, store_id)
    out += _str_field(5, offer_group_id)
    return bytes(out)


async def buy_once(session, email, password, device_id,
                   offer_id, revision, store_id, offer_group_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "offer_id": offer_id, "revision": revision,
             "store_id": store_id, "offer_group_id": offer_group_id,
             "login_ok": False, "deviceid_sent": False,
             "buy_sent": False, "result": None, "result_hex": None,
             "events_seen": [], "error": None}
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

        payload = build_buyoffer_payload(
            offer_id, revision, store_id, offer_group_id)
        print(f"[*] BUY OFFER {offer_id!r} rev {revision} store "
              f"{store_id!r} grup {offer_group_id!r}: "
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
                    hasil["events_seen"].append(
                        {"event": ev_name, "bytes": len(d) if d else 0})
                if ev_name == RESULT_EVENT:
                    data = d
                    break
            if data is not None:
                break
        if data is None:
            hasil["error"] = "tidak ada respons 'd' dalam 20 dtk"
            print("[!] tidak ada respons 'd' dalam 20 dtk — pembelian "
                  "mungkin tetap diproses server; verifikasi lewat "
                  "respons/wallet, JANGAN beli ulang membabi buta.")
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
    ap.add_argument("--offer-id", default=os.getenv("DB_OFFER_ID", ""))
    ap.add_argument("--revision", type=int, default=int(
        os.getenv("DB_REVISION", "1") or 1))
    ap.add_argument("--store-id", default=os.getenv("DB_STORE_ID", ""))
    ap.add_argument("--offer-group-id", default=os.getenv(
        "DB_OFFER_GROUP_ID", ""))
    ap.add_argument("--confirm", default=os.getenv("DB_CONFIRM", ""))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if not args.offer_id or not args.store_id or not args.offer_group_id:
        print("[!] offer-id/store-id/offer-group-id wajib diisi.")
        return 1
    payload = build_buyoffer_payload(
        args.offer_id, args.revision, args.store_id, args.offer_group_id)
    print("========== RENCANA BELI (BUYOFFER DIAG) ==========")
    print(f"Offer: {args.offer_id}  Revisi: {args.revision}")
    print(f"Store: {args.store_id!r}  Grup: {args.offer_group_id!r}")
    print(f"Payload BUY ({len(payload)} B): {payload.hex()}")
    print("=============================================")
    if args.dry_run:
        print("[*] DRY-RUN: tidak ada jaringan game, selesai.")
        return 0
    if str(args.confirm) != "1":
        print("[!] mode live butuh --confirm 1 (pengaman 1x beli).")
        return 1
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi (mode live).")
        return 1
    L.apply_panel_versions()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await buy_once(
                session, args.email, args.password, args.device_id,
                args.offer_id, args.revision, args.store_id,
                args.offer_group_id)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "offer_id": args.offer_id, "revision": args.revision,
                     "store_id": args.store_id,
                     "offer_group_id": args.offer_group_id,
                     "login_ok": False, "deviceid_sent": False,
                     "buy_sent": False, "result": None,
                     "result_hex": None, "events_seen": [],
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
    print("BUYOFFER_JSON:" + json.dumps(_jsonable(hasil),
                                        separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
