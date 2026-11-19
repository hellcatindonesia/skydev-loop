# -*- coding: utf-8 -*-
"""
roomobs_diag.py — Probe observasi room (Plaza) 1 bot tes
(pkxdshop.id@gmail.com). READ-ONLY (tidak kirim quest, tidak ubah apa
pun di akun) — hanya mengamati siapa saja yang ada di room.

Latar (2026-11-19, bedah pizza quest): 4 probe pizza membuktikan
server memvalidasi waypoint ([0,1,2] diterima, [1,2,3] ditolak) tapi
hadiah tetap KOSONG — antaran harus terdaftar lewat jalur lain,
paling mungkin POSISI pemain di dekat NPC pizza (delivery radius
`_deliveryMinDistance` di PizzaQuestNPCController) atau user variable
`pizzasDelivered`/`pizzaQuestState`. Probe ini mencari NPC pizza di
Plaza: ID + posisi mereka.
- Bot login -> alokasi default (Plaza) -> TCP login -> tunggu
  JoinedRoom -> umumkan diri (SendDeviceId + tbulv idle + 1x posisi
  spawn, persis seperti client normal) -> amati 45 dtk.
- Yang dicatat: semua 'C' RoomEvent (batch posisi, Deflate) ->
  cluster posisi STATIONER (NPC berdiri diam; pemain bergerak);
  semua nama RoomEvent; sub-frame batch 0x0d (cari string
  "pizza"/"npc"); JoinedRoom (room id).
- 'C' tidak membawa user ID — posisi dikelompokkan berdasarkan
  kedekatan + urutan seq. Cluster stationer = kandidat NPC (atau
  pemain AFK). Nama pemain lain TIDAK dicetak (hanya string yang
  mengandung "pizza"/"npc", bila ada).
Cetak ROOMOBS_JSON.
"""
import argparse
import asyncio
import json
import math
import os
import struct
import time
import zlib

import lookup_uuid as L
import gift_diag as G
import ugc_fetch_diag as F

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"
OBSERVE_SECONDS = 45


# ---------- helper posisi (di-inline dari follow_diag/spawn_test_diag,
# supaya probe ini mandiri — kedua file itu TIDAK ada di skydev-loop) ---
def build_position_payload(x, y, z, rotation=0.0, teleported=1, sequence=0):
    """RoomEvent 'C': [Teleported 1B][seq 1B][X/Y/Z/Rot f32 BE]."""
    return (bytes([teleported & 0xFF, sequence & 0xFF])
            + struct.pack(">f", x) + struct.pack(">f", y)
            + struct.pack(">f", z) + struct.pack(">f", rotation))


def parse_c_batch(data):
    """Dekompresi payload event 'C' (raw Deflate) -> list posisi
    (x, y, z, rot, teleported, seq). Record 18 byte."""
    try:
        dec = zlib.decompress(bytes(data), -15)
    except Exception:
        return []
    out = []
    for base in (0, 1):
        i = base
        while i + 18 <= len(dec):
            tele, seq = dec[i], dec[i + 1]
            try:
                x, y, z, rot = struct.unpack(">ffff", dec[i + 2:i + 18])
            except Exception:
                break
            if math.isfinite(x) and math.isfinite(y) and math.isfinite(z):
                out.append((x, y, z, rot, tele, seq))
            i += 18
        if out:
            break
    return out


def build_spawn_aaa(x, y, z, player_id=None):
    """Frame posisi 'aaa' (tbulv). Prefix 00 06 <ID-2byte> bila
    player_id diberikan (terbukti bikin bot idle, run 43221201166)."""
    f32 = lambda v: struct.pack("<f", v)
    sub_y = bytes.fromhex("08f980a5ab05") + b"\x1d" + f32(y)
    sub_x = bytes.fromhex("089b87987010b3f2de61") + b"\x1d" + f32(x)
    sub_z = bytes.fromhex("08a2b0b7b0fdffffffff01") + b"\x1d" + f32(z)
    sub_r = bytes.fromhex("08ffffffffffffffffff0110ffffffffffffffffff01")
    sub_v = bytes.fromhex("08ffffffffffffffffff0110ffffffffffffffffff01")
    pb = b""
    for field, sub in ((1, sub_y), (2, sub_x), (3, sub_r),
                       (4, sub_z), (5, sub_v)):
        pb += bytes([(field << 3) | 2, len(sub)]) + sub
    prefix = b""
    if player_id is not None:
        prefix = bytes([0x00, 0x06]) + (player_id & 0xFFFF).to_bytes(2, "big")
    return prefix + struct.pack(">I", 3) + b"aaa" + b"\x05" + pb


def split_batch_0x0d(frame):
    """Pecah frame batch 0x0d jadi sub-frame [4-byte BE len][isi].
    Kembalikan list sub-frame (bytes) atau [] bila format tak cocok."""
    out = []
    i = 1  # lewati byte tipe 0x0d
    n = len(frame)
    try:
        while i + 4 <= n:
            ln = struct.unpack(">I", frame[i:i + 4])[0]
            if ln <= 0 or i + 4 + ln > n:
                break
            out.append(bytes(frame[i + 4:i + 4 + ln]))
            i += 4 + ln
        if i != n:
            return []
    except Exception:
        return []
    return out


def _printable_strings(blob, min_len=4):
    """String ASCII printable dalam blob (untuk cari nama NPC)."""
    out = []
    cur = bytearray()
    for b in blob:
        if 32 <= b < 127:
            cur.append(b)
        else:
            if len(cur) >= min_len:
                out.append(bytes(cur).decode("ascii"))
            cur = bytearray()
    if len(cur) >= min_len:
        out.append(bytes(cur).decode("ascii"))
    return out


async def roomobs_once(session, email, password, device_id, seconds):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "login_ok": False, "joined": False,
             "server": "", "room": None, "player_id": None,
             "frames_total": 0, "c_events": 0, "c_records": 0,
             "event_names": {}, "batch_subframes": {},
             "stationary": [], "pizza_strings": [],
             "distinct_strings": 0, "error": None}
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
    srv, rm = F.parse_gsid(alloc.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] alokasi OK: {host}:{port} server={srv} room={rm}")
    sock, pid = L.tcp_connect(host, port)
    if sock is None:
        hasil["error"] = "TCP connect gagal"
        print("[!] TCP connect gagal.")
        return hasil
    hasil["player_id"] = pid
    print(f"[+] Connected: playerId={pid}.")
    try:
        sock.sendall(L.build_login_frame(user_jwt))
        print("[+] login frame terkirim.")
    except Exception as exc:
        hasil["error"] = f"kirim login frame gagal: {exc}"
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    try:
        print("[*] tunggu JoinedRoom/tbugv (maks ~5 dtk)...")
        joined, via_tbugv, fr = F.wait_joined(sock, rm)
        hasil["joined"] = joined
        print(f"[+] joined={joined} (via_tbugv={via_tbugv}).")
        if not joined:
            hasil["error"] = "tidak JoinedRoom"
            return hasil
        # Umumkan diri seperti client normal (supaya server relay
        # posisi user lain ke bot): SendDeviceId + tbulv idle + 1x
        # posisi spawn.
        send_dev = device_id or DEFAULT_DEVICE_ID
        try:
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId", send_dev.encode("utf-8")))
            print("[+] SendDeviceId terkirim.")
        except Exception as exc:
            print(f"[!] kirim SendDeviceId gagal: {exc}")
        try:
            sock.sendall(L.build_room_event_varint(
                "tbulv", build_spawn_aaa(0.0, 0.0, 0.0, player_id=pid)))
            print("[+] tbulv idle terkirim.")
        except Exception as exc:
            print(f"[!] kirim tbulv gagal: {exc}")
        try:
            sock.sendall(L.build_room_event_varint(
                "C", build_position_payload(0.0, 0.0, 0.0,
                                           rotation=0.0,
                                           teleported=1, sequence=0)))
            print("[+] posisi spawn terkirim.")
        except Exception as exc:
            print(f"[!] kirim posisi gagal: {exc}")

        # Amati.
        clusters = {}   # (x, y, z) dibulatkan 0.1 -> [count, last_seq]
        event_names = {}
        batch_sub = {}
        pizza_strings = set()
        distinct_strings = set()
        print(f"[*] amati room selama {seconds} dtk...")
        end = time.time() + seconds
        total = 0
        while time.time() < end:
            for f in fr.drain(0.5):
                total += 1
                raw = bytes(f)
                if not raw:
                    continue
                t = raw[0]
                if t == 0x08:
                    name, d = G.decode_room_event(raw)
                    if name:
                        event_names[name] = event_names.get(name, 0) + 1
                    if name == "C" and d:
                        hasil["c_events"] += 1
                        for (x, y, z, rot, tele, s) in parse_c_batch(d):
                            hasil["c_records"] += 1
                            key = (round(x, 1), round(y, 1), round(z, 1))
                            c = clusters.get(key)
                            if c is None:
                                clusters[key] = [1, s]
                            else:
                                c[0] += 1
                                c[1] = s
                elif t == 0x0d:
                    subs = split_batch_0x0d(raw)
                    for sf in subs:
                        st = sf[0] if sf else None
                        batch_sub[st] = batch_sub.get(st, 0) + 1
                        if sf and sf[0] == 0x08:
                            nm, _dd = G.decode_room_event(sf)
                            if nm:
                                event_names[nm] = event_names.get(nm, 0) + 1
                        for s in _printable_strings(sf):
                            distinct_strings.add(s)
                            if "pizza" in s.lower() or "npc" in s.lower():
                                pizza_strings.add(s)
                elif t == 0x06:
                    event_names["<joinedroom>"] = \
                        event_names.get("<joinedroom>", 0) + 1
                else:
                    event_names[f"<type 0x{t:02x}>"] = \
                        event_names.get(f"<type 0x{t:02x}>", 0) + 1
        hasil["frames_total"] = total
        hasil["event_names"] = event_names
        hasil["batch_subframes"] = {str(k): v
                                    for k, v in batch_sub.items()}
        hasil["distinct_strings"] = len(distinct_strings)
        hasil["pizza_strings"] = sorted(pizza_strings)
        # Cluster stationer: posisi yang muncul >= 10x (NPC diam atau
        # pemain AFK). Urutkan dari yang paling sering terlihat.
        stat = sorted(
            ((k[0], k[1], k[2], v[0]) for k, v in clusters.items()
             if v[0] >= 10),
            key=lambda r: -r[3])
        hasil["stationary"] = [
            {"x": x, "y": y, "z": z, "count": c} for (x, y, z, c) in stat]
        print(f"[+] total frame: {total}, event 'C': {hasil['c_events']}, "
              f"record posisi: {hasil['c_records']}")
        print(f"[+] nama event: {event_names}")
        print(f"[+] cluster stationer (>=10x): {hasil['stationary']}")
        print(f"[+] string pizza/npc: {hasil['pizza_strings']}")
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
    ap.add_argument("--seconds", type=int, default=OBSERVE_SECONDS)
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await roomobs_once(
                session, args.email, args.password, args.device_id,
                args.seconds)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "login_ok": False, "joined": False,
                     "server": "", "room": None, "player_id": None,
                     "frames_total": 0, "c_events": 0, "c_records": 0,
                     "event_names": {}, "batch_subframes": {},
                     "stationary": [], "pizza_strings": [],
                     "distinct_strings": 0,
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] roomobs_once gagal: {type(exc).__name__}: {exc}")
    print("")
    print("========== HASIL OBSERVASI ROOM ==========")
    print(f"Login OK: {hasil.get('login_ok')}  Joined: {hasil.get('joined')}  "
          f"server={hasil.get('server')} room={hasil.get('room')}")
    print(f"Frame total: {hasil.get('frames_total')}  "
          f"event 'C': {hasil.get('c_events')}  "
          f"record posisi: {hasil.get('c_records')}")
    print(f"Nama event: {hasil.get('event_names')}")
    print(f"Sub-frame batch 0x0d (tipe->jumlah): "
          f"{hasil.get('batch_subframes')}")
    print(f"Cluster stationer: {hasil.get('stationary')}")
    print(f"String pizza/npc: {hasil.get('pizza_strings')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("ROOMOBS_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
