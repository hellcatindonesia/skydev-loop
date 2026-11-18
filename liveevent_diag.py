# -*- coding: utf-8 -*-
"""
liveevent_diag.py — Probe Live Event READ-ONLY, 1 bot tes
(pkxdshop.id@gmail.com). TIDAK ADA pembelian.

Latar (analisis dump_3 — 2026-11-18):
- Toko tersembunyi = Wandering Vendor, LiveEventType 5. Event live
  diputuskan SERVER (bukan jam tetap di client): server mengirim
  daftar event aktif beserta jam mulai/berakhir.
- Mekanik server ToxicBirdLiveEventsMechanic, COMMAND_KEY "live_event".
- Request: GetActiveEventsRequest, f1 EventId = GetActiveEvents = 0
  (SATU-SATUNYA client event; proto3 menghilangkan nilai 0) ->
  payload KOSONG (0 byte). Pola sama seperti Daily Box GetStatus dan
  Daily Streak STRKS (keduanya payload kosong).
- Respons ActiveEventsResponse: f1 EventId (=0 ActiveEventsReceived),
  f2 Successful (bool), f3 ErrorMessage (string), f4 activeEvents
  (repeated LiveEventPayload: f1 occurrenceId string, f2 type enum,
  f3 startedAt ulong, f4 endAt ulong, f5 effects repeated).
  LiveEventType: 0 Rain, 1 Thunderstorm, 2 Snow, 3 Blizzard, 4 Mirage,
  5 WanderingVendor, 6 AdminAbuse, 7 Fireworks, 8 Rainbow, 9 SaveTheSeas,
  10 AdminTimer, 11 Windstorm.
  LiveEventsServerEventId: 0 ActiveEventsReceived, 1 EventStarted,
  2 EventFinished.
- Respons ditunggu di RoomEvent "live_event" (mekanik ini tidak punya
  sub-key seperti Daily Bonus); semua nama event yang lewat dicatat
  untuk jaga-jaga bila respons datang dengan nama lain.
- Mekanik terdaftar di ToxicBirdClient (bukan room): TIDAK perlu
  destinationUserId, TIDAK ada syarat room/activity, Bartes tidak
  perlu online. TIDAK kirim JoinRoom.
Alur: login -> alokasi sendiri -> TCP login -> tunggu jendela login
(tbugv, maks 3 dtk) -> SendDeviceId ('Analytics/deviceId') ->
GetActiveEvents (payload kosong) -> cetak LIVEEVENT_JSON.
Request ditunggu responsnya maks 20 dtk, maks 1 kirim ulang.
CATATAN: daftar item + harga vendor TIDAK ada di respons ini — harga
vendor hidup di store content (data server, endpoint /v1/store-content
dan CDN game-store-data butuh sesi platform / signed URL; percobaan
2026-11-16 gagal 404/403). Probe ini menjawab EVENT APA SAJA yang
aktif + sampai jam berapa.
"""
import argparse
import asyncio
import datetime
import json
import os
import struct
import time

import lookup_uuid as L
import gift_diag as G

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"
EVENT = "live_event"

TYPE_NAMES = {0: "Rain", 1: "Thunderstorm", 2: "Snow", 3: "Blizzard",
              4: "Mirage", 5: "WanderingVendor", 6: "AdminAbuse",
              7: "Fireworks", 8: "Rainbow", 9: "SaveTheSeas",
              10: "AdminTimer", 11: "Windstorm"}


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


def _ts_str(ts):
    """Timestamp Unix (detik; bila milidetik, bagi 1000) -> 'YYYY-MM-DD
    HH:MM WIB'. None bila tidak valid."""
    if not isinstance(ts, int) or ts <= 0:
        return None
    if ts > 10**12:
        ts //= 1000
    try:
        dt = datetime.datetime.fromtimestamp(
            ts, tz=datetime.timezone(datetime.timedelta(hours=7)))
        return dt.strftime("%Y-%m-%d %H:%M WIB")
    except Exception:
        return None


def parse_active_events(data):
    d = parse_protobuf(data)
    events = []
    raw = d.get(4)
    if raw is not None:
        raw = raw if isinstance(raw, list) else [raw]
        for item in raw:
            ev = _jsonable(_nested(item))
            if not isinstance(ev, dict):
                events.append({"raw": ev})
                continue
            t = ev.get("2")
            events.append({
                "occurrence_id": ev.get("1"),
                "type": TYPE_NAMES.get(t, t),
                "type_id": t,
                "started_at": ev.get("3"),
                "started_at_wib": _ts_str(ev.get("3")),
                "end_at": ev.get("4"),
                "end_at_wib": _ts_str(ev.get("4")),
                "effects": ev.get("5"),
            })
    return {"event_id": d.get(1), "successful": d.get(2),
            "error": d.get(3), "active_events": events}


async def liveevent_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "active": None, "error": None}
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

        def wait_event(name, timeout=20):
            """Baca frame sampai dapat RoomEvent `name` atau timeout.
            Mencatat SEMUA nama RoomEvent yang lewat (untuk melihat bila
            respons datang dengan nama lain). Kembalikan data protobuf
            (bytes) atau None."""
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    ev_name, d = G.decode_room_event(f)
                    if ev_name is not None:
                        print(f"[.] event lewat: '{ev_name}' "
                              f"({len(d) if d else 0} B)")
                    if ev_name == name:
                        return d
            return None

        # GetActiveEvents — payload KOSONG (EventId=0, proto3 default).
        for attempt in (1, 2):
            try:
                sock.sendall(L.build_room_event_varint(EVENT, b""))
            except Exception as exc:
                print(f"[!] kirim GetActiveEvents gagal: {exc}")
                hasil["error"] = f"kirim gagal: {exc}"
                return hasil
            print(f"[*] kirim GetActiveEvents (payload kosong), "
                  f"percobaan {attempt}/2, tunggu respons '{EVENT}' "
                  f"maks 20 dtk...")
            data = wait_event(EVENT, 20)
            if data is not None:
                print(f"[+] respons '{EVENT}' diterima ({len(data)} B): "
                      f"{data.hex()}")
                try:
                    hasil["active"] = parse_active_events(data)
                except Exception as exc:
                    print(f"[!] parse respons gagal: {exc!r}")
                    import traceback
                    traceback.print_exc()
                    hasil["active"] = {"parse_error": repr(exc),
                                       "raw_hex": data.hex()}
                break
            print(f"[!] tidak ada respons '{EVENT}' "
                  f"(percobaan {attempt}/2).")
        else:
            hasil["error"] = "tidak ada respons GetActiveEvents"
            print("[!] GetActiveEvents: TIDAK ADA respons.")
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
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await liveevent_once(
                session, args.email, args.password, args.device_id)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "server": "", "room": None, "player_id": None,
                     "login_ok": False, "deviceid_sent": False,
                     "active": None,
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] liveevent_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL LIVE EVENT (READ-ONLY) ==========")
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    act = hasil.get("active") or {}
    print(f"GetActiveEvents successful: {act.get('successful')}  "
          f"event aktif: {len(act.get('active_events') or [])}")
    for ev in (act.get("active_events") or []):
        print(f"  {ev.get('type')} (id {ev.get('occurrence_id')}): "
              f"{ev.get('started_at_wib')} s/d {ev.get('end_at_wib')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("===================================================")
    print("LIVEEVENT_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
