# -*- coding: utf-8 -*-
"""
verify_rename_diag.py — Verifikasi independen hasil rename_twice_diag.

Tes rename dua kali (run 44097453509) mengembalikan Name='TesNama1' lalu
Name='TesNama2', TAPI dengan catatan "has no old name" dan TANPA field
Wallet/Spent. Jadi belum terbukti nama benar-benar menempel di server.

Tes ini: login sebagai bot fresh lain (search nama itu publik), lalu cari
" TesNama2" dan "TesNama1" lewat SearchFriend (EventId=7) dan lihat UUID
yang kembali:
- TesNama2 -> UUID target, TesNama1 tidak ketemu = NAMA2_MENEMPEL
  (rename #2 benar-benar menempel di server).
- TesNama1 -> UUID target, TesNama2 tidak ketemu = NAMA1_MASIH
  (rename #2 TIDAK menempel; echo Name tadi hanya gema).
- Keduanya tidak ketemu = TIDAK_KETEMU.
- Keduanya ketemu = CAMPURAN (aneh, laporkan apa adanya).

Password TIDAK pernah dicetak ke log.
"""
import argparse
import asyncio
import json
import os
import time
import urllib.request

import lookup_uuid as L

SKYDEV_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"


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


def fetch_unnamed_bot(supabase_url, supabase_key, exclude_email="", limit=5):
    """Ambil 1 bot tanpa nama fresh dari panel via RPC get_fresh_bots.
    Ambil beberapa lalu pilih yang emailnya BUKAN exclude_email — bot
    TIDAK BOLEH mencari namanya sendiri (search kemungkinan tidak
    mengembalikan diri sendiri, hasil jadi tidak bisa disimpulkan)."""
    payload = {"p_target_uuid": SKYDEV_UUID, "p_limit": limit}
    req = urllib.request.Request(
        f"{supabase_url}/rest/v1/rpc/get_fresh_bots",
        data=json.dumps(payload).encode("utf-8"), method="POST")
    req.add_header("apikey", supabase_key)
    req.add_header("Authorization", f"Bearer {supabase_key}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"[-] Gagal mengambil bot dari database: {exc}")
        return None
    if not isinstance(data, list) or not data:
        print("[-] RPC get_fresh_bots kosong.")
        return None
    for bot in data:
        if (bot.get("email") or "") != exclude_email:
            print(f"[+] Bot pencari: {bot.get('email')} "
                  f"(device_id {bot.get('device_id')})")
            return bot
    print("[-] Semua bot yang dikembalikan adalah bot yang di-rename.")
    return None


def do_search(sock, name):
    """Kirim SearchFriend, tunggu search_friend_result (maks 12 dtk).
    Kembalikan dict {success, uuid, name} atau None."""
    frame = L.build_room_event_varint("friends", L.build_search_protobuf(name))
    try:
        sock.sendall(frame)
        sock.sendall(frame)
    except Exception as exc:
        print(f"[-] kirim search {name!r} gagal: {exc}")
        return None

    def _pred(fr):
        for p in fr:
            dec = L.decode_search_result(p)
            if dec is not None and dec["success"] and dec["uuid"]:
                return True
        return False

    resp, closed, _, _ = L.read_until(sock, _pred, 12)
    for p in resp:
        dec = L.decode_search_result(p)
        if dec is not None:
            print(f"    decode {name!r}: success={dec['success']} "
                  f"uuid={dec['uuid']} nama={dec['name']!r}")
            if dec["success"] and dec["uuid"]:
                return dec
    return None


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("VERIFY_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("VERIFY_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("VERIFY_DEVICE_ID", ""))
    ap.add_argument("--name1", default=os.getenv("VERIFY_NAME1", "TesNama1"))
    ap.add_argument("--name2", default=os.getenv("VERIFY_NAME2", "TesNama2"))
    ap.add_argument("--uuid", default=os.getenv(
        "VERIFY_UUID", "ad3713eb-6d39-439e-999d-c0af4a98c7dc"))
    ap.add_argument("--exclude-email", default=os.getenv(
        "VERIFY_EXCLUDE_EMAIL", "g43dtt893wuk@outlook.com"))
    ap.add_argument("--control-name", default=os.getenv(
        "VERIFY_CONTROL_NAME", "SkyDev"))
    ap.add_argument("--control-uuid", default=os.getenv(
        "VERIFY_CONTROL_UUID", "aae2efe1-1abb-48c2-8799-ea192692a49c"))
    args = ap.parse_args()
    if not args.email or not args.password:
        bot = fetch_unnamed_bot(os.getenv("SUPABASE_URL", ""),
                                os.getenv("SUPABASE_SERVICE_KEY", ""),
                                exclude_email=args.exclude_email)
        if not bot or not bot.get("email") or not bot.get("password"):
            print("[!] tidak ada bot tanpa nama fresh.")
            return 1
        args.email = bot["email"]
        args.password = bot["password"]
        args.device_id = bot.get("device_id") or args.device_id
    if not args.device_id:
        args.device_id = "9f31c2d4-probe-device"

    hasil = {"searcher": args.email, "name1": args.name1,
             "name2": args.name2, "expected_uuid": args.uuid,
             "control_name": args.control_name,
             "found_control": None, "found1": None, "found2": None,
             "verdict": "UNKNOWN"}
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        user_jwt, alloc = await L.login_and_allocate(
            args.email, args.password, args.device_id, session)
        if not alloc:
            print("[!] login/alokasi gagal.")
            hasil["verdict"] = "LOGIN_GAGAL"
            print("VERIFY_JSON:" + json.dumps(hasil, separators=(",", ":")))
            return 1
        srv, rm = parse_gsid(alloc.get("gameServerId", ""))
        print(f"[+] alokasi OK: server={srv} room={rm}")
        if not srv or rm is None:
            hasil["verdict"] = "ALOKASI_GAGAL"
            print("VERIFY_JSON:" + json.dumps(hasil, separators=(",", ":")))
            return 1
        host, port = L.find_host_port(alloc)
        if not host or not port:
            hasil["verdict"] = "ALOKASI_GAGAL"
            print("VERIFY_JSON:" + json.dumps(hasil, separators=(",", ":")))
            return 1
        sock, _pid = L.tcp_connect(host, port)
        if sock is None:
            print("[!] TCP connect gagal.")
            hasil["verdict"] = "TCP_GAGAL"
            print("VERIFY_JSON:" + json.dumps(hasil, separators=(",", ":")))
            return 1
        try:
            sock.sendall(L.build_login_frame(user_jwt))
            print("[+] Event login terkirim.")
        except Exception as exc:
            print(f"[-] kirim login gagal: {exc}")
            hasil["verdict"] = "TCP_GAGAL"
            print("VERIFY_JSON:" + json.dumps(hasil, separators=(",", ":")))
            return 1
        time.sleep(1)
        # Kontrol dulu: search nama yang PASTI ada (SkyDev). Bila kontrol
        # saja tidak ketemu, berarti search-nya yang rusak, bukan namanya.
        print(f"[*] Kontrol, cari: {args.control_name} ...")
        hasil["found_control"] = do_search(sock, args.control_name)
        if hasil["found_control"] is None:
            print("[*] Search kontrol kosong — tunggu 6 dtk lalu search ulang.")
            L.read_frames(sock, 6)
            hasil["found_control"] = do_search(sock, args.control_name)
        time.sleep(1)
        print(f"[*] Cari: {args.name2} ...")
        hasil["found2"] = do_search(sock, args.name2)
        if hasil["found2"] is None:
            print("[*] Search langsung kosong — tunggu 6 dtk lalu search ulang.")
            L.read_frames(sock, 6)
            hasil["found2"] = do_search(sock, args.name2)
        time.sleep(1)
        print(f"[*] Cari: {args.name1} ...")
        hasil["found1"] = do_search(sock, args.name1)
        if hasil["found1"] is None:
            print("[*] Search langsung kosong — tunggu 6 dtk lalu search ulang.")
            L.read_frames(sock, 6)
            hasil["found1"] = do_search(sock, args.name1)
        try:
            sock.close()
        except Exception:
            pass

    f1 = hasil["found1"] or {}
    f2 = hasil["found2"] or {}
    fc = hasil["found_control"] or {}
    m1 = f1.get("uuid") == args.uuid
    m2 = f2.get("uuid") == args.uuid
    mc = fc.get("uuid") == args.control_uuid
    if not mc:
        hasil["verdict"] = "SEARCH_RUSAK"
    elif m2 and not m1:
        hasil["verdict"] = "NAMA2_MENEMPEL"
    elif m1 and not m2:
        hasil["verdict"] = "NAMA1_MASIH"
    elif not m1 and not m2:
        hasil["verdict"] = "TIDAK_KETEMU"
    else:
        hasil["verdict"] = "CAMPURAN"
    print("========== HASIL VERIFIKASI RENAME ==========")
    print(f"Kontrol {args.control_name!r}: {fc} "
          f"Cari {args.name2!r}: {f2}  Cari {args.name1!r}: {f1}")
    print(f"VERDICT: {hasil['verdict']}")
    print("============================================")
    print("VERIFY_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
