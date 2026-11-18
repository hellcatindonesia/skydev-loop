# -*- coding: utf-8 -*-
"""
inv_capture_diag.py — Tangkap SEMUA frame TCP sesudah login (READ-ONLY).
Tujuan: menemukan di frame mana inventory (UserInventory, List<int>)
dikirim server saat login — supaya auto-dekor tahu item apa saja
yang tersedia di akun SkyDev.

Alur: login -> alokasi default -> TCP connect -> kirim login frame ->
baca SEMUA frame selama 15 dtk (TANPA kirim init/RoomEvent, supaya
yang tertangkap murni kiriman server sesudah login) -> untuk tiap
frame catat: tipe byte pertama, panjang, nama RoomEvent (bila 0x08),
dan dump protobuf generik (bila bisa diparse). Simpan ringkasan ke
INVCAP_JSON. TIDAK ada penulisan apa pun ke akun.
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G
import ugc_fetch_diag as F


def _frame_summary(idx, fr):
    """Ringkasan satu frame: tipe, panjang, event, dump generik."""
    raw = bytes(fr)
    out = {"idx": idx, "len": len(raw), "type": raw[0] if raw else None,
           "hex_head": raw[:32].hex()}
    if raw and raw[0] == 0x08:
        name, d = G.decode_room_event(raw)
        out["event"] = name
        out["payload_len"] = len(d) if d else 0
        if d:
            try:
                out["dump"] = F._jsonable(F._dump_generic(d, depth=1))
            except Exception as exc:
                out["dump_error"] = repr(exc)
    else:
        # Bukan RoomEvent — coba parse sebagai protobuf generik.
        try:
            out["dump"] = F._jsonable(F._dump_generic(raw, depth=1))
        except Exception as exc:
            out["dump_error"] = repr(exc)
    return out


async def inv_capture_once(session, email, password, device_id, seconds):
    hasil = {"email": email, "login_ok": False, "joined": False,
             "frames": [], "error": None}
    user_jwt, _alloc = await L.login_and_allocate(
        email, password, device_id, session)
    if not user_jwt:
        hasil["error"] = "login/alokasi awal gagal"
        print("[!] login/alokasi awal gagal.")
        return hasil
    hasil["login_ok"] = True
    body = _alloc
    host, port = L.find_host_port(body)
    if not host or not port:
        hasil["error"] = "host/port tidak ketemu di allocation default"
        print("[!] host/port default tidak ketemu.")
        return hasil
    srv, rm = F.parse_gsid(body.get("gameServerId", ""))
    print(f"[+] server={srv} room={rm} host={host}:{port}")
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
        # Tunggu JoinedRoom/tbugv dulu (tanda login diterima).
        # wait_joined membuat BigFrameReader sendiri dan mengembalikannya
        # — pakai reader ITU untuk capture (jangan buat reader kedua
        # di socket yang sama, nanti frame berebut).
        print("[*] tunggu JoinedRoom/tbugv (maks ~5 dtk)...")
        joined, via_tbugv, fr = F.wait_joined(sock, rm)
        hasil["joined"] = joined
        print(f"[+] joined={joined} (via_tbugv={via_tbugv}).")
        # Tangkap semua frame selama `seconds` detik.
        print(f"[*] tangkap semua frame selama {seconds} dtk "
              f"(tanpa kirim apa pun)...")
        end = time.time() + seconds
        idx = 0
        while time.time() < end:
            for f in fr.drain(0.5):
                s = _frame_summary(idx, f)
                hasil["frames"].append(s)
                ev = s.get("event")
                print(f"[.] frame #{idx} type=0x{s['type']:02x} "
                      f"len={s['len']} event={ev!r}")
                idx += 1
        print(f"[+] total {idx} frame tertangkap.")
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
    ap.add_argument("--seconds", type=int,
                    default=int(os.getenv("DB_SECONDS", "15")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await inv_capture_once(
                session, args.email, args.password, args.device_id,
                args.seconds)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "login_ok": False,
                     "joined": False, "frames": [],
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] inv_capture_once gagal: "
                  f"{type(exc).__name__}: {exc}")
    hasil = F._jsonable(hasil)
    print("")
    print("========== HASIL INV CAPTURE (READ-ONLY) ==========")
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"joined: {hasil.get('joined')}  "
          f"frames: {len(hasil.get('frames') or [])}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    # Ringkasan event yang terlihat.
    from collections import Counter
    c = Counter()
    for f in hasil.get("frames") or []:
        c[(f.get("type"), f.get("event"))] += 1
    for (t, ev), n in sorted(c.items(), key=lambda kv: -kv[1]):
        print(f"  type=0x{t:02x} event={ev!r}: {n}x")
    print("====================================================")
    print("INVCAP_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
