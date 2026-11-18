# -*- coding: utf-8 -*-
"""
dailybox_diag.py — Probe Daily Box (kotak hadiah gratis di toko) 1 bot tes
(pkxdshop.id@gmail.com).

Latar (2026-11-18, analisis dump_3 — ~/workspace/free_claims_analysis.md):
- Sistem bernama Daily Box, mekanik server ToxicBirdDailyBoxMechanic,
  nama RoomEvent "daily_box", EventId: GetStatus=0, Collect=1.
  Request DailyBoxRequest HANYA punya f1 EventId — jadi:
    Status  : "daily_box" (kosong, 0 byte — EventId=0 default proto3)
    Collect : "daily_box" 08 01
- Respons:
    DailyBoxStatusResponse : f1 EventId, f2 AvailableBoxes,
                             f3 TimeToNextBox (detik sampai kotak berikut)
    DailyBoxCollectResponse: f1 EventId, f2 Success, f3 AvailableBoxes,
                             f4 TimeToNextBox, f5 Reward
                             (GrantLootBoxRewardResponse: f1 Gift Money,
                             f2 Items repeated int, f3 Wallet Money;
                             Money f1=koin, f2=gems)
- Mekanik ini terdaftar di ToxicBirdClient (bukan room): TIDAK perlu
  destinationUserId, TIDAK ada syarat room/activity, Bartes tidak
  perlu online. TIDAK kirim JoinRoom.
- TIDAK ADA varian iklan di request ini — Collect (EventId=1) adalah
  klaim GRATIS-nya. Alur iklan di UI ("nonton iklan lalu klaim") hanya
  memanggil Collect yang sama sesudah iklan ditonton; probe ini TIDAK
  menyentuh iklan sama sekali.
- Probe ini HANYA klaim bila status menunjukkan AvailableBoxes > 0.
  Bila 0, probe berhenti di status (melaporkan TimeToNextBox) dan
  TIDAK mengirim Collect.
Alur: login -> alokasi sendiri -> TCP login -> tunggu jendela login
(tbugv, maks 3 dtk) -> SendDeviceId ('Analytics/deviceId') ->
Status -> (bila ada kotak: Collect 1x) -> cetak DAILYBOX_JSON.
Tiap request ditunggu responsnya maks 10 dtk, maks 1 kirim ulang
bila tidak ada respons.
"""
import argparse
import asyncio
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"


def _add(out, field, val):
    """Kumpulkan field berulang jadi list; field tunggal tetap skalar."""
    if field in out:
        if isinstance(out[field], list):
            out[field].append(val)
        else:
            out[field] = [out[field], val]
    else:
        out[field] = val


def _try_str(chunk):
    """Wire-2 chunk -> str HANYA bila isinya teks printable."""
    try:
        s = chunk.decode("utf-8")
    except Exception:
        return chunk
    if s and all(c.isprintable() for c in s):
        return s
    return chunk


def parse_protobuf(buf):
    """Parse protobuf generik. Wire 0 varint, wire 2 length-delim
    (panjang varint), wire 5 fixed32, wire 1 fixed64."""
    out = {}
    i = 0
    n = len(buf)
    while i < n:
        tag = buf[i]
        i += 1
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            v = 0
            s = 0
            while i < n:
                b = buf[i]
                i += 1
                v |= (b & 0x7F) << s
                if not (b & 0x80):
                    break
                s += 7
            _add(out, field, v)
        elif wire == 2:
            ln, m = L.read_varint(buf, i)
            if ln is None:
                break
            i += m
            chunk = bytes(buf[i:i + ln])
            i += ln
            _add(out, field, _try_str(chunk))
        elif wire == 5:
            if i + 4 <= n:
                _add(out, field, struct.unpack("<f", buf[i:i + 4])[0])
                i += 4
            else:
                break
        elif wire == 1:
            if i + 8 <= n:
                _add(out, field, struct.unpack("<d", buf[i:i + 8])[0])
                i += 8
            else:
                break
        else:
            break
    return out


def _nested(v, depth=0):
    """Nilai wire-2 berupa bytes: coba parse sebagai protobuf nested,
    REKURSIF (maks 3 level). Bila tidak bisa diparse, kembalikan hex."""
    if isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        if depth > 3:
            return {"hex": bytes(v).hex()}
        try:
            d = parse_protobuf(bytes(v))
        except Exception:
            d = {}
        if not d:
            return {"hex": bytes(v).hex()}
        return {k: _nested(x, depth + 1) for k, x in d.items()}
    if isinstance(v, list):
        return [_nested(x, depth) for x in v]
    if isinstance(v, dict):
        return {k: _nested(x, depth) for k, x in v.items()}
    return v


def _jsonable(v):
    if isinstance(v, dict):
        return {str(k): _jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return v


def parse_dailybox_status(data):
    d = parse_protobuf(data)
    return {"event_id": d.get(1) or 0,
            "available_boxes": d.get(2),
            "time_to_next_box": d.get(3)}


def parse_dailybox_collect(data):
    d = parse_protobuf(data)
    reward = d.get(5)
    return {"event_id": d.get(1) or 0,
            "success": bool(d.get(2)),
            "available_boxes": d.get(3),
            "time_to_next_box": d.get(4),
            "reward": _jsonable(_nested(reward))
            if reward is not None else None}


async def dailybox_once(session, email, password, device_id,
                        status_only=False, force_collect=False):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "status": None, "collect": None, "collect_sent": False,
             "error": None}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
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
    hasil["player_id"] = pid
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

        def wait_dailybox(timeout=10):
            """Baca frame sampai dapat RoomEvent 'daily_box' atau timeout.
            Kembalikan data protobuf (bytes) atau None."""
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name == "daily_box":
                        return d
            return None

        def dailybox_request(label, payload, parse_fn, timeout=10):
            """Kirim event 'daily_box' + tunggu respons; maks 1 kirim ulang."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("daily_box",
                                                           payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} "
                      f"({payload.hex() if payload else '(kosong)'}), "
                      f"percobaan {attempt}/2, tunggu respons maks "
                      f"{timeout} dtk...")
                data = wait_dailybox(timeout)
                if data is not None:
                    print(f"[+] respons 'daily_box' diterima untuk {label} "
                          f"({len(data)} B): {data.hex()}")
                    try:
                        return parse_fn(data)
                    except Exception as exc:
                        print(f"[!] parse respons {label} gagal: {exc!r} "
                              f"(raw hex di atas)")
                        import traceback
                        traceback.print_exc()
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons 'daily_box' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        # 1. Status — ada berapa kotak, berapa lama lagi kotak berikut.
        hasil["status"] = dailybox_request("GetStatus", b"",
                                           parse_dailybox_status)
        if isinstance(hasil["status"], dict):
            print(f"[+] Status: {hasil['status']}")
        else:
            print("[!] Status: TIDAK ADA respons — berhenti (tanpa Collect).")
            hasil["error"] = "tidak ada respons status"
            return hasil
        if "parse_error" in hasil["status"]:
            hasil["error"] = "parse status gagal"
            return hasil

        # 2. Collect GRATIS — hanya bila ada kotak tersedia (dan tidak
        # --status-only). --force-collect mengirim Collect walau status
        # menunjukkan 0 kotak, untuk melihat respons server.
        if status_only:
            print("[*] Mode --status-only: Collect TIDAK dikirim.")
            return hasil
        avail = hasil["status"].get("available_boxes")
        if force_collect:
            print(f"[*] Mode --force-collect: kirim Collect walau "
                  f"available_boxes={avail}.")
        if (isinstance(avail, int) and avail > 0) or force_collect:
            print(f"[*] Ada {avail} kotak — kirim Collect gratis (tanpa iklan).")
            hasil["collect_sent"] = True
            hasil["collect"] = dailybox_request(
                "Collect gratis", bytes.fromhex("0801"),
                parse_dailybox_collect)
            if isinstance(hasil["collect"], dict):
                print(f"[+] Collect: {hasil['collect']}")
            else:
                print("[!] Collect: TIDAK ADA respons.")
        else:
            print(f"[*] Tidak ada kotak tersedia (available_boxes={avail}) — "
                  "Collect TIDAK dikirim.")
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("DB_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("DB_DEVICE_ID", ""))
    ap.add_argument("--status-only", action="store_true",
                    help="Hanya baca status, jangan kirim Collect")
    ap.add_argument("--force-collect", action="store_true",
                    help="Kirim Collect walau status menunjukkan 0 kotak")
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await dailybox_once(
                session, args.email, args.password, args.device_id,
                status_only=args.status_only,
                force_collect=args.force_collect)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "server": "", "room": None, "player_id": None,
                     "login_ok": False, "deviceid_sent": False,
                     "status": None, "collect": None, "collect_sent": False,
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] dailybox_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL DAILY BOX ==========")
    st = hasil.get("status") or {}
    co = hasil.get("collect") or {}
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    print(f"Status: AvailableBoxes={st.get('available_boxes')} "
          f"TimeToNextBox={st.get('time_to_next_box')}")
    if hasil.get("collect_sent"):
        print(f"Collect (gratis): Success={co.get('success')} "
              f"AvailableBoxes={co.get('available_boxes')} "
              f"TimeToNextBox={co.get('time_to_next_box')} "
              f"Reward={co.get('reward')}")
    else:
        print("Collect: TIDAK dikirim (tidak ada kotak / status gagal).")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("DAILYBOX_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
