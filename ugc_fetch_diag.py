# -*- coding: utf-8 -*-
"""
ugc_fetch_diag.py — Probe ambil data world UGC (TerrainData) READ-ONLY,
1 bot tes (pkxdshop.id@gmail.com). Langkah 1 rencana copy builder
antar akun Bartes sendiri. TIDAK ADA pembuatan draft, update draft,
publish, atau penulisan apa pun — hanya login + join + baca.

Wire format (TERBUKTI via disassembly libil2cpp.so LAMA + dump lama,
2026-11-18 — bukan tebakan):
- Mekanik ToxicBirdUgcRoomMechanic.GetCurrentServerActivityData
  (RVA 0x7471F14) memanggil ToxicBirdClient.EmitRoomEvent dengan
  command key "activity/get" (slot .rela.dyn 0x8900AE0 -> string
  'activity/get') dan payload = ToxicBirdServer.EMPTY_REQUEST, yaitu
  byte array KOSONG (dibuktikan .cctor ToxicBirdServer RVA 0x74C0898:
  new byte[0] disimpan ke static_fields[0]).
- Respons sukses datang sebagai RoomEvent "get-activity" (slot
  0x8900AD8; didaftarkan di Install), error sebagai
  "get-activity-error" (slot 0x8900AD0).
- Payload "get-activity" = protobuf GetActivityResponse:
  f1 Data (ActivityData). ActivityData:
  f1 OwnerId string, f2 ActivityId string, f3 PublicationId string,
  f4 Type int (= TerrainType — dibuktikan disassembly
  UgcTransformations.ToActivityTerrainData RVA 0x742B26C: ctor
  ActivityTerrainData(terrainType=Type, items, rooms)),
  f5 Items repeated ActivityItem, f6 Rooms repeated ActivityRoom.
  ActivityItem: f1 Id int, f2 Position Vector3, f3 Rotation Vector3,
  f4 ColorId int, f5 EquippedItems repeated int32.
  ActivityRoom: f1 Floor int, f2 Walls repeated int32.
  Vector3 (protobuf): f1 X float, f2 Y float, f3 Z float (fixed32).
- Konfirmasi independen: visit_loop.py / join_uuid_standby.py sudah
  mengirim build_room_event_varint("activity/get", b"") sebagai
  bagian dari rangkaian init kunjungan.

Alur: login -> alokasi activity (3 percobaan pola visit_loop.py:
  /lobby/activity/allocation, /lobby/allocation+activityId,
  /lobby/allocation+destinationUserId bila target user id diisi)
  -> TCP connect -> login frame -> tunggu JoinedRoom/tbugv
  -> kirim init RoomEvents + "activity/get" (SEKALI SAJA, payload
  kosong) + SendDeviceId + 'C' spawn -> tunggu "get-activity"
  maks 20 dtk -> parse -> cetak ringkasan + UGCFETCH_TERRAIN_JSON
  (seluruh TerrainData) + UGCFETCH_JSON (ringkasan).
CATATAN: rangkaian init + 'C' sama seperti satu kunjungan biasa,
jadi counter kunjungan world bisa bertambah 1 — ini world Bartes
sendiri dan nilainya tidak berarti dibanding target 1 juta.
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
DEFAULT_ACTIVITY_ID = "0d2522bd-b29a-4809-af63-5f72263d2deb"
REQ_EVENT = "activity/get"
RESP_EVENT = "get-activity"
RESP_ERROR_EVENT = "get-activity-error"
MAX_FRAME = 8 * 1024 * 1024  # dunia besar bisa > 256 KB (batas FrameReader)


# ---------------------------------------------------------------- protobuf
def _varint(buf, i):
    """Varint sampai 64-bit (10 byte). L.read_varint berhenti di
    shift 35 (maks 5 byte) sehingga varint 64-bit (mis. nilai
    negatif int32 yang di-sign-extend, atau field ulong) akan
    dikira terpotong — itu bug yang membuat parse gagal."""
    val = 0
    shift = 0
    n = len(buf)
    start = i
    while i < n and shift < 70:
        b = buf[i]
        val |= (b & 0x7F) << shift
        i += 1
        if not (b & 0x80):
            return val, i - start
        shift += 7
    return None, 0


def _read_varint(buf, i):
    v, m = _varint(buf, i)
    if v is None:
        raise ValueError("varint terpotong")
    return v, m


def _parse_fields(buf):
    """Parse protobuf generik -> {field: [(wire, value), ...]}.
    wire 0 varint, 2 bytes, 5 fixed32 float, 1 fixed64 double."""
    out = {}
    i = 0
    n = len(buf)
    while i < n:
        tag, m = _read_varint(buf, i)
        i += m
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            v, m = _read_varint(buf, i)
            i += m
            out.setdefault(field, []).append((0, v))
        elif wire == 2:
            ln, m = _read_varint(buf, i)
            i += m
            chunk = bytes(buf[i:i + ln])
            i += ln
            out.setdefault(field, []).append((2, chunk))
        elif wire == 5:
            if i + 4 > n:
                raise ValueError("fixed32 terpotong")
            out.setdefault(field, []).append(
                (5, struct.unpack("<f", buf[i:i + 4])[0]))
            i += 4
        elif wire == 1:
            if i + 8 > n:
                raise ValueError("fixed64 terpotong")
            out.setdefault(field, []).append(
                (1, struct.unpack("<d", buf[i:i + 8])[0]))
            i += 8
        else:
            raise ValueError(f"wire type {wire} tidak dikenal")
    return out


def _first(fields, field, wire=None):
    for w, v in fields.get(field, []):
        if wire is None or w == wire:
            return v
    return None


def _str_field(fields, field):
    v = _first(fields, field, 2)
    if v is None:
        return None
    return v.decode("utf-8", "replace")


def _int_field(fields, field):
    v = _first(fields, field, 0)
    return v


def _repeated_int(fields, field):
    """Repeated int32: terima bentuk unpacked (wire 0 per nilai)
    maupun packed (wire 2 berisi varint beruntun)."""
    vals = []
    for w, v in fields.get(field, []):
        if w == 0:
            vals.append(v)
        elif w == 2:
            i = 0
            while i < len(v):
                x, m = _read_varint(v, i)
                i += m
                vals.append(x)
    return vals


def _parse_vector3(buf):
    f = _parse_fields(buf)
    return {"x": _first(f, 1, 5), "y": _first(f, 2, 5),
            "z": _first(f, 3, 5)}


def _parse_activity_item(buf):
    f = _parse_fields(buf)
    pos = _first(f, 2, 2)
    rot = _first(f, 3, 2)
    return {"id": _int_field(f, 1),
            "position": _parse_vector3(pos) if pos is not None else None,
            "rotation": _parse_vector3(rot) if rot is not None else None,
            "color_id": _int_field(f, 4),
            "equipped_items": _repeated_int(f, 5)}


def _parse_activity_room(buf):
    f = _parse_fields(buf)
    return {"floor": _int_field(f, 1),
            "walls": _repeated_int(f, 2)}


def _parse_activity_data(buf):
    f = _parse_fields(buf)
    items = []
    for (w, c) in f.get(5, []):
        if w != 2:
            continue
        try:
            items.append(_parse_activity_item(c))
        except Exception as exc:
            items.append({"parse_error": repr(exc), "raw_hex": c.hex()})
    rooms = []
    for (w, c) in f.get(6, []):
        if w != 2:
            continue
        try:
            rooms.append(_parse_activity_room(c))
        except Exception as exc:
            rooms.append({"parse_error": repr(exc), "raw_hex": c.hex()})
    return {"owner_id": _str_field(f, 1),
            "activity_id": _str_field(f, 2),
            "publication_id": _str_field(f, 3),
            "type": _int_field(f, 4),
            "items": items,
            "rooms": rooms}


def _dump_generic(buf, depth=0):
    """Dump struktur protobuf generik (nomor field + wire + nilai
    ringkas) untuk diagnosis bila parse ketat gagal. Dipakai untuk
    melihat struktur ASLI respons, bukan tebakan."""
    try:
        f = _parse_fields(buf)
    except Exception as exc:
        return {"parse_error": repr(exc),
                "hex_head": bytes(buf[:64]).hex()}
    out = {}
    for field, vals in sorted(f.items()):
        shown = []
        for w, v in vals:
            if w == 2:
                # String printable -> tampilkan sebagai string,
                # jangan diparse sebagai protobuf nested.
                try:
                    s = bytes(v).decode("utf-8")
                    if s and all(32 <= ord(c) < 127 for c in s):
                        shown.append(s)
                        continue
                except Exception:
                    pass
                if depth < 2 and len(v) >= 2:
                    shown.append(_dump_generic(v, depth + 1))
                else:
                    shown.append({"bytes_len": len(v),
                                  "hex_head": bytes(v[:32]).hex()})
            else:
                shown.append(v)
        out[str(field)] = shown if len(shown) > 1 else shown[0]
    return out


def parse_get_activity_response(data):
    """GetActivityResponse: f1 = ActivityData (message)."""
    f = _parse_fields(data)
    raw = _first(f, 1, 2)
    if raw is None:
        raise ValueError("GetActivityResponse: field 1 (Data) tidak ada")
    return {"activity_data": _parse_activity_data(raw)}


def _jsonable(v):
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return v


# ------------------------------------------------------------------- frame
class BigFrameReader(G.FrameReader):
    """FrameReader yang sama, tapi batas frame 8 MB (dunia besar)."""

    def _pump(self):
        while len(self.buf) >= 4:
            ln = struct.unpack(">I", self.buf[:4])[0]
            if ln > MAX_FRAME:
                return None
            if len(self.buf) - 4 < ln:
                break
            fr = bytes(self.buf[4:4 + ln])
            del self.buf[:4 + ln]
            if fr == b"\x01":
                try:
                    self.s.sendall(struct.pack(">I", 1) + b"\x02")
                except Exception:
                    pass
                continue
            return fr
        return None


def parse_gsid(gsid):
    parts = (gsid or "").split("::")
    server = parts[1] if len(parts) >= 2 else ""
    room = None
    if len(parts) >= 3:
        try:
            room = int(parts[2])
        except Exception:
            room = None
    return server, room


def build_c_payload(x, y, z, teleported, sequence):
    """Payload posisi 'C' 18 byte (TERBUKTI):
    [Teleported][seq][X/Y/Z/Rot f32 BE]."""
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", 0.0))


async def allocate_world(session, jwt, activity_id, target_user_id):
    """Alokasi masuk world (pola visit_loop.py). Kembalikan
    (body, label) atau ({}, '')."""
    attempts = [
        ("/lobby/activity/allocation",
         {"userToken": jwt, "activityId": activity_id,
          "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "datacenterId": "blr1"},
         "activity"),
        ("/lobby/allocation",
         {"userToken": jwt, "gameServerProtocolVersion": L.GAME_PROTOCOL,
          "isPrivate": False, "activityId": activity_id},
         "lobby+activityId"),
    ]
    if target_user_id:
        attempts.append(
            ("/lobby/allocation",
             {"userToken": jwt, "gameServerProtocolVersion": L.GAME_PROTOCOL,
              "isPrivate": False, "destinationUserId": target_user_id},
             "lobby+destinationUserId"))
    for endpoint, body, label in attempts:
        b = json.dumps(body, separators=(",", ":")).encode("utf-8")
        try:
            r = await session.post(L.LOBBY_URL + endpoint, data=b,
                                   headers=L.HEADERS_BASE.copy(), timeout=20)
            resp = r.json()
        except Exception as exc:
            print(f"[!] alokasi {label} gagal: {exc!r}")
            continue
        if not isinstance(resp, dict):
            print(f"[!] alokasi {label}: respons bukan dict")
            continue
        host, port = L.find_host_port(resp)
        if host and port:
            print(f"[+] alokasi OK via {label}: {host}:{port}")
            return resp, label
        print(f"[!] alokasi {label}: host/port tidak ketemu "
              f"(keys: {sorted(resp.keys())[:8]})")
    return {}, ""


def wait_joined(sock, rm):
    """Tunggu JoinedRoom (standalone 0x06 0x01 / batch 0x0d) atau
    tbugv, maks ~5 dtk. Kembalikan (joined_bool, tbugv_bool, fr).
    tbugv saja sudah cukup untuk lanjut (server auto-place bot di
    room saat login; ActivityId respons nanti yang memverifikasi)."""
    fr = BigFrameReader(sock)
    needle = (bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
              if rm is not None else b"")

    def _is_join_or_tbugv(f):
        if G._is_tbugv(f):
            return True
        if rm is not None and len(f) >= 6 and f[0] == 0x06 and f[1] == 1 \
                and int.from_bytes(f[2:6], "big") == rm:
            return True
        if rm is not None and f and f[0] == 0x0d and needle in bytes(f):
            return True
        return False

    got, closed = fr.read_until(_is_join_or_tbugv, 3)
    if closed:
        return False, False, fr
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
        return True, True, fr
    if got is not None:
        return True, False, fr
    if rm is not None:
        for f in fr.drain(2):
            raw = bytes(f)
            if (len(raw) >= 6 and raw[0] == 0x06 and raw[1] == 1
                    and int.from_bytes(raw[2:6], "big") == rm) \
                    or (raw and raw[0] == 0x0d and needle in raw):
                return True, False, fr
    return False, False, fr


async def ugc_fetch_once(session, email, password, device_id,
                         activity_id, target_user_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "activity_id_target": activity_id,
             "login_ok": False, "alloc_label": None,
             "joined": False, "activity_get_sent": False,
             "terrain": None, "error": None,
             "events_seen": []}
    user_jwt, _alloc = await L.login_and_allocate(
        email, password, device_id, session)
    if not user_jwt:
        hasil["error"] = "login/alokasi awal gagal"
        print("[!] login/alokasi awal gagal.")
        return hasil
    hasil["login_ok"] = True
    body, label = await allocate_world(session, user_jwt, activity_id,
                                       target_user_id)
    if not body:
        hasil["error"] = "alokasi activity gagal (semua percobaan)"
        print("[!] alokasi activity gagal.")
        return hasil
    hasil["alloc_label"] = label
    host, port = L.find_host_port(body)
    if not host or not port:
        hasil["error"] = "host/port tidak ketemu di allocation activity"
        print("[!] host/port activity tidak ketemu.")
        return hasil
    srv, rm = parse_gsid(body.get("gameServerId", ""))
    print(f"[+] activity server={srv} room={rm} host={host}:{port}")
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
    try:
        print("[*] tunggu JoinedRoom/tbugv (maks ~5 dtk)...")
        joined, via_tbugv, fr = wait_joined(sock, rm)
        if not joined:
            hasil["error"] = "JoinedRoom/tbugv tidak terkonfirmasi"
            print("[!] JoinedRoom/tbugv tidak terkonfirmasi.")
            return hasil
        hasil["joined"] = True
        print(f"[+] joined (via_tbugv={via_tbugv}).")
        # Rangkaian init pola visit_loop.py (TERBUKTI).
        try:
            for name in ["init_pos", "init_day_night", "preset",
                         "fetch_events"]:
                sock.sendall(L.build_room_event_varint(name, b""))
            time.sleep(0.3)
            # GetCurrentServerActivityData — SEKALI SAJA, payload kosong.
            sock.sendall(L.build_room_event_varint(REQ_EVENT, b""))
            hasil["activity_get_sent"] = True
            print(f"[+] '{REQ_EVENT}' terkirim (payload kosong).")
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId",
                (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
            seq = [0]
            seq[0] = (seq[0] + 1) & 0xFF
            sock.sendall(L.build_room_event_varint(
                "C", build_c_payload(0.0, 2.0, 0.0, 1, seq[0])))
            print("[+] init + SendDeviceId + 'C' spawn terkirim.")
        except Exception as exc:
            hasil["error"] = f"kirim init gagal: {exc}"
            print(f"[!] kirim init gagal: {exc}")
            return hasil

        # Tunggu respons "get-activity" maks 20 dtk.
        print(f"[*] tunggu respons '{RESP_EVENT}' maks 20 dtk...")
        data = None
        err_data = None
        end = time.time() + 20
        while time.time() < end:
            for f in fr.drain(0.5):
                ev_name, d = G.decode_room_event(f)
                if ev_name is not None:
                    hasil["events_seen"].append(ev_name)
                    print(f"[.] event lewat: '{ev_name}' "
                          f"({len(d) if d else 0} B)")
                    if ev_name == RESP_EVENT:
                        data = d
                        break
                    if ev_name == RESP_ERROR_EVENT:
                        err_data = d
                        break
            if data is not None or err_data is not None:
                break
        if err_data is not None:
            hasil["error"] = (f"server balas '{RESP_ERROR_EVENT}' "
                              f"({len(err_data)} B): {err_data.hex()}")
            print(f"[!] {hasil['error']}")
            return hasil
        if data is None:
            hasil["error"] = (f"tidak ada respons '{RESP_EVENT}' "
                              f"dalam 20 dtk")
            print(f"[!] {hasil['error']}")
            return hasil
        print(f"[+] respons '{RESP_EVENT}' diterima ({len(data)} B).")
        print("UGCFETCH_RAW_HEX:" + data.hex())
        try:
            dump = _dump_generic(data)
            print("UGCFETCH_DUMP_JSON:" + json.dumps(
                _jsonable(dump), separators=(",", ":")))
        except Exception as exc:
            print(f"[!] dump generik gagal: {exc!r}")
        try:
            parsed = parse_get_activity_response(data)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil["error"] = f"parse respons gagal: {exc!r}"
            hasil["raw_hex_len"] = len(data)
            print(f"[!] parse gagal: {exc!r} (raw {len(data)} B, "
                  f"hex lengkap ada di baris UGCFETCH_RAW_HEX di atas)")
            return hasil
        ad = parsed["activity_data"]
        terrain = {"activity_id": ad["activity_id"],
                   "owner_id": ad["owner_id"],
                   "publication_id": ad["publication_id"],
                   "terrain_type": ad["type"],
                   "placed_items": ad["items"],
                   "rooms": ad["rooms"]}
        hasil["terrain"] = terrain
        print(f"[+] TERRAIN: activity={ad['activity_id']} "
              f"owner={ad['owner_id']} type={ad['type']} "
              f"items={len(ad['items'])} rooms={len(ad['rooms'])}")
        if ad["activity_id"] != activity_id:
            print(f"[!] PERINGATAN: ActivityId respons "
                  f"({ad['activity_id']}) BEDA dari target "
                  f"({activity_id}) — bot mungkin tidak di world target!")
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
    ap.add_argument("--activity-id", default=os.getenv(
        "DB_ACTIVITY_ID", DEFAULT_ACTIVITY_ID))
    ap.add_argument("--target-user-id", default=os.getenv(
        "DB_TARGET_USER_ID", ""))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    if not args.activity_id:
        print("[!] activity_id wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await ugc_fetch_once(
                session, args.email, args.password, args.device_id,
                args.activity_id, args.target_user_id or None)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "activity_id_target": args.activity_id,
                     "login_ok": False, "alloc_label": None,
                     "joined": False, "activity_get_sent": False,
                     "terrain": None,
                     "error": f"{type(exc).__name__}: {exc}",
                     "events_seen": []}
            print(f"[!] ugc_fetch_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    terrain = hasil.get("terrain") or {}
    print("")
    print("========== HASIL UGC FETCH (READ-ONLY) ==========")
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"alloc: {hasil.get('alloc_label')}  "
          f"joined: {hasil.get('joined')}  "
          f"activity/get: {hasil.get('activity_get_sent')}")
    print(f"ActivityId target: {hasil.get('activity_id_target')}")
    print(f"ActivityId respons: {terrain.get('activity_id')}")
    print(f"OwnerId: {terrain.get('owner_id')}")
    print(f"TerrainType: {terrain.get('terrain_type')}  "
          f"items: {len(terrain.get('placed_items') or [])}  "
          f"rooms: {len(terrain.get('rooms') or [])}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print(f"Events terlihat: {hasil.get('events_seen')}")
    print("===================================================")
    if terrain:
        print("UGCFETCH_TERRAIN_JSON:" + json.dumps(
            terrain, separators=(",", ":")))
    print("UGCFETCH_JSON:" + json.dumps(
        {k: v for k, v in hasil.items() if k != "terrain"},
        separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
