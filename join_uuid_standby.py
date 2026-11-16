# -*- coding: utf-8 -*-
"""
join_uuid_standby.py — Eksperimen join seseorang via UUID (GitHub akun ke-2).

Perintah Bartes 2026-11-17: pakai 1 bot bernama, join ke UUID SkyDev,
bot standby online supaya Bartes bisa cek di game apakah bot muncul.
Jalan di hellcatindonesia/skydev-loop.

Cara kerja (pola TERBUKTI dari visit_loop.py / JOIN-NECESSITY DIAG v3):
1. Login HTTP (JWT) -> alokasi /lobby/allocation + destinationUserId
   -> server tempatkan bot di room target (tanpa JoinRoom, tanpa
   pertemanan).
2. TCP connect -> login frame -> tunggu JoinedRoom.
3. Kirim init RoomEvents + 'C' spawn teleported=1.
4. Standby sampai durasi habis: tiap 10 dtk kirim 'C' (teleported=0)
   + drain frame (jawab ping). Reconnect bila sesi putus.
5. Di Plaza server kirim RoomUserUpdate jadi Bartes bisa lihat bot.
   Di activity (Sekolah/UGC) bot join tapi TIDAK tampil.

JANGAN echo password di log. JANGAN login sebagai SkyDev.
"""
import argparse
import asyncio
import json
import random
import struct
import time

import lookup_uuid as L
import gift_diag as G

SKYDEV_USER_ID = "aae2efe1-1abb-48c2-8799-ea192692a49c"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"


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


async def allocate_to_user(session, jwt, target_user_id):
    """Alokasi ke room target via destinationUserId. Kembalikan body
    dict (dengan host/port) atau {} bila gagal."""
    body = {"userToken": jwt, "gameServerProtocolVersion": L.GAME_PROTOCOL,
            "isPrivate": False, "destinationUserId": target_user_id}
    b = json.dumps(body, separators=(",", ":")).encode("utf-8")
    try:
        r = await session.post(L.LOBBY_URL + "/lobby/allocation", data=b,
                               headers=L.HEADERS_BASE.copy(), timeout=20)
        resp = r.json()
    except Exception:
        return {}
    if not isinstance(resp, dict):
        return {}
    host, port = L.find_host_port(resp)
    if host and port:
        return resp
    return {}


def send_spawn(sock, device_id, seq):
    """Init RoomEvents + SendDeviceId + 'C' spawn teleported=1.
    Kembalikan (ok, error)."""
    try:
        for name in ["init_pos", "init_day_night", "preset",
                     "fetch_events"]:
            sock.sendall(L.build_room_event_varint(name, b""))
        time.sleep(0.3)
        sock.sendall(L.build_room_event_varint("activity/get", b""))
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId",
            (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
        seq[0] = (seq[0] + 1) & 0xFF
        sock.sendall(L.build_room_event_varint(
            "C", build_c_payload(0.0, 2.0, 0.0, 1, seq[0])))
        return True, None
    except Exception as exc:
        return False, str(exc)


def send_position(sock, seq):
    """'C' posisi berkala (teleported=0). Kembalikan (ok, error)."""
    try:
        seq[0] = (seq[0] + 1) & 0xFF
        sock.sendall(L.build_room_event_varint(
            "C", build_c_payload(0.0, 2.0, 0.0, 0, seq[0])))
        return True, None
    except Exception as exc:
        return False, str(exc)


def wait_joined(sock, rm):
    """Tunggu JoinedRoom (standalone 0x06 0x01 / batch 0x0d), maks
    ~5 dtk. Kembalikan (True/False, FrameReader)."""
    fr = G.FrameReader(sock)
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
        return False, fr
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    if got is not None and not G._is_tbugv(got):
        return True, fr
    if rm is not None:
        for f in fr.drain(2):
            raw = bytes(f)
            if (len(raw) >= 6 and raw[0] == 0x06 and raw[1] == 1
                    and int.from_bytes(raw[2:6], "big") == rm) \
                    or (raw and raw[0] == 0x0d and needle in raw):
                return True, fr
    return False, fr


async def _establish(email, password, device_id, target_user_id):
    """Satu kali login HTTP + alokasi destinationUserId. Kembalikan
    (jwt, body, host, port) atau None bila gagal."""
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        user_jwt, _alloc = await L.login_and_allocate(
            email, password, device_id, session)
        if not user_jwt:
            return None
        body = await allocate_to_user(session, user_jwt, target_user_id)
        if not body:
            return None
        host, port = L.find_host_port(body)
        if not host or not port:
            return None
        return user_jwt, body, host, port


def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--device-id", default=DEFAULT_DEVICE_ID)
    ap.add_argument("--bot-name", default="?")
    ap.add_argument("--target-user-id", default=SKYDEV_USER_ID)
    ap.add_argument("--duration-min", type=float, default=30)
    ap.add_argument("--heartbeat-s", type=float, default=10)
    args = ap.parse_args()

    bot_name = args.bot_name
    target = args.target_user_id
    end_time = time.time() + args.duration_min * 60
    t0 = time.time()
    joined_ok = False
    reconnects = 0
    heartbeats = 0
    errors = {}
    print(f"[+] Bot: {bot_name}  target: {target}  "
          f"durasi: {args.duration_min} mnt", flush=True)

    while time.time() < end_time:
        try:
            est = asyncio.run(_establish(
                args.email, args.password, args.device_id, target))
        except Exception as exc:
            est = None
            errors[f"establish: {exc}"] = errors.get(f"establish: {exc}", 0) + 1
        if not est:
            errors["login/alloc gagal"] = \
                errors.get("login/alloc gagal", 0) + 1
            print("[-] login/alokasi gagal, coba lagi 5 dtk", flush=True)
            time.sleep(5)
            continue
        user_jwt, body, host, port = est
        srv, rm = parse_gsid(body.get("gameServerId", ""))
        print(f"[+] Alokasi OK: server={srv} room={rm} "
              f"host={host}:{port}", flush=True)
        try:
            sock, pid = L.tcp_connect(host, port)
        except Exception as exc:
            errors[f"tcp: {exc}"] = errors.get(f"tcp: {exc}", 0) + 1
            time.sleep(5)
            continue
        if sock is None:
            errors["tcp connect gagal"] = \
                errors.get("tcp connect gagal", 0) + 1
            time.sleep(5)
            continue
        try:
            sock.sendall(L.build_login_frame(user_jwt))
            joined, fr = wait_joined(sock, rm)
        except Exception as exc:
            errors[f"login frame: {exc}"] = \
                errors.get(f"login frame: {exc}", 0) + 1
            try:
                sock.close()
            except Exception:
                pass
            time.sleep(5)
            continue
        if not joined:
            errors["joined tidak terkonfirmasi"] = \
                errors.get("joined tidak terkonfirmasi", 0) + 1
            print("[-] JoinedRoom tidak terkonfirmasi", flush=True)
            try:
                sock.close()
            except Exception:
                pass
            time.sleep(5)
            continue
        joined_ok = True
        reconnects += 1
        print(f"[+] JOINED room {rm} — bot {bot_name} online, "
              f"cek di game sekarang", flush=True)
        seq = [0]
        ok, err = send_spawn(sock, args.device_id, seq)
        if not ok:
            errors[f"spawn: {err}"] = errors.get(f"spawn: {err}", 0) + 1
            print(f"[-] spawn gagal: {err}", flush=True)
            try:
                sock.close()
            except Exception:
                pass
            time.sleep(5)
            continue
        print("[+] Spawn 'C' terkirim", flush=True)
        alive = True
        while alive and time.time() < end_time:
            try:
                fr.drain(1)
            except Exception:
                alive = False
                break
            if sock.fileno() == -1:
                alive = False
                break
            ok, err = send_position(sock, seq)
            if not ok:
                errors[f"posisi: {err}"] = \
                    errors.get(f"posisi: {err}", 0) + 1
                alive = False
                break
            heartbeats += 1
            if heartbeats % 6 == 0:
                elapsed = (time.time() - t0) / 60
                print(f"[=] ONLINE {elapsed:.1f}/{args.duration_min} mnt "
                      f"bot={bot_name} room={rm}", flush=True)
            time.sleep(args.heartbeat_s)
        try:
            sock.close()
        except Exception:
            pass
        if time.time() < end_time:
            print("[!] Sesi putus, reconnect...", flush=True)
            time.sleep(5)

    elapsed_min = (time.time() - t0) / 60
    hasil = {"bot_name": bot_name,
             "target_user_id": target,
             "joined": joined_ok,
             "duration_min": args.duration_min,
             "elapsed_min": round(elapsed_min, 1),
             "reconnects": reconnects,
             "heartbeats": heartbeats,
             "errors": errors}
    print("")
    print("========== HASIL JOIN UUID STANDBY ==========")
    print(f"Bot: {bot_name}  joined: {joined_ok}  "
          f"online: {elapsed_min:.1f} mnt")
    print(f"Reconnects: {reconnects}  errors: {errors or '-'}")
    print("Bartes HARUS cek di game: bot muncul di samping SkyDev "
          "atau tidak (Plaza = kelihatan, activity = tidak tampil).")
    print("=============================================")
    print("JOIN_STANDBY_JSON:" + json.dumps(hasil,
                                            separators=(",", ":")))
    return 0 if joined_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
