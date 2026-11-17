# -*- coding: utf-8 -*-
"""
rename_twice_diag.py — Tes ganti nama DUA KALI di 1 bot tanpa nama.

Pertanyaan (2026-11-17, Bartes): ganti nama pertama gratis, ganti nama
kedua pakai gems — apakah aturan itu ditegakkan SERVER, atau cuma client
yang menampilkan tombol bayar? Bot mengirim UpdateNickname LANGSUNG
(tanpa lewat purchase flow beli offer "change_nickname"), jadi:
- Rename #1 sukses + Spent=0  -> gratis pertama (sesuai aturan).
- Rename #2 sukses + Spent=0  -> GRATIS_KEDUANYA: server TIDAK menagih
  lewat jalur ini (celah).
- Rename #2 sukses + Spent>0  -> KEDUA_BAYAR: server menagih langsung
  dari wallet (aturan ditegakkan server).
- Rename #2 gagal (ErrorMessage) -> KEDUA_DITOLAK: server menolak
  tanpa pembelian offer dulu.

Jalur (dump.cs, ToxicBirdProfileMechanic, event "profile_update"):
- UpdateNicknameRequest: f1 EventId=9, f2 Nickname.
  Response "update_name_result": f1 Name, f2 ErrorMessage,
  f3 Wallet (Money), f4 Spent (Money).
- Money: f1 coins, f2 gems (varint; field 0 dihilangkan proto3).

Bot: 1 akun TANPA NAMA fresh dari panel (RPC get_fresh_bots).
BUKAN pkxdshop (perintah Bartes 2026-11-17 20:53).
Password TIDAK pernah dicetak ke log.
"""
import argparse
import asyncio
import json
import os
import struct
import time
import urllib.request

import lookup_uuid as L
import gift_diag as G

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


def parse_protobuf(buf):
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
            out[field] = v
        elif wire == 2:
            if i >= n:
                break
            ln = buf[i]
            i += 1
            if ln & 0x80:
                out[field] = b""
                break
            chunk = buf[i:i + ln]
            i += ln
            try:
                out[field] = chunk.decode("utf-8")
            except Exception:
                out[field] = chunk
        elif wire == 5:
            if i + 4 <= n:
                out[field] = struct.unpack("<f", buf[i:i + 4])[0]
                i += 4
            else:
                break
        elif wire == 1:
            i += 8
        else:
            break
    return out


def parse_money(raw):
    """Money bersarang (bytes) -> {coins, gems}. Bukan bytes -> None."""
    if not isinstance(raw, (bytes, bytearray)):
        return None
    try:
        f = parse_protobuf(bytes(raw))
    except Exception:
        return None
    coins = f.get(1)
    gems = f.get(2)
    return {"coins": coins if isinstance(coins, int) else 0,
            "gems": gems if isinstance(gems, int) else 0}


def _varint(v):
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _sfield(num, text):
    raw = text.encode("utf-8")
    return bytes([(num << 3) | 2]) + _varint(len(raw)) + raw


def build_update_nickname(name):
    # UpdateNicknameRequest: f1 EventId=9, f2 Nickname.
    return b"\x08" + _varint(9) + _sfield(2, name)


def listen_for(fr, wanted, seconds):
    """Dengarkan RoomEvent bernama `wanted` selama `seconds` detik.
    Kembalikan (fields dict, event_names) atau (None, event_names)."""
    event_names = {}
    end = time.time() + seconds
    while time.time() < end:
        for f in fr.drain(0.5):
            name, d = G.decode_room_event(f)
            if not name:
                continue
            event_names[name] = event_names.get(name, 0) + 1
            if name != wanted:
                continue
            try:
                fields = parse_protobuf(bytes(d))
            except Exception:
                fields = {}
            return fields, event_names
    return None, event_names


def fetch_unnamed_bot(supabase_url, supabase_key, limit=1):
    """Ambil 1 bot tanpa nama fresh dari panel via RPC get_fresh_bots
    (pola visit_loop.py). Kembalikan dict {email, password, device_id}
    atau None. Password TIDAK pernah dicetak ke log."""
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
        print("[-] RPC get_fresh_bots kosong — tidak ada bot tanpa nama fresh.")
        return None
    bot = data[0]
    print(f"[+] Bot tanpa nama: {bot.get('email')} "
          f"(device_id {bot.get('device_id')})")
    return bot


async def rename_twice(session, email, password, device_id, name1, name2):
    hasil = {"email": email, "name1": name1, "name2": name2,
             "server": "", "room": None, "joined_room": None,
             "rename1": {}, "rename2": {}, "verdict": "UNKNOWN"}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        hasil["verdict"] = "LOGIN_GAGAL"
        return hasil
    srv, rm = parse_gsid(alloc.get("gameServerId", ""))
    hasil["server"] = srv
    hasil["room"] = rm
    print(f"[+] alokasi OK: server={srv} room={rm}")
    if not srv or rm is None:
        hasil["verdict"] = "ALOKASI_GAGAL"
        return hasil
    host, port = L.find_host_port(alloc)
    if not host or not port:
        print("[!] host/port tidak ketemu.")
        hasil["verdict"] = "ALOKASI_GAGAL"
        return hasil
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        hasil["verdict"] = "TCP_GAGAL"
        return hasil
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        hasil["verdict"] = "TCP_GAGAL"
        return hasil
    fr = G.FrameReader(sock)
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        hasil["verdict"] = "TCP_GAGAL"
        return hasil
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
    print(f"[*] kirim JoinRoom({rm}) ...")
    try:
        sock.sendall(L.build_join_frame(rm))
    except Exception as exc:
        print(f"[!] kirim JoinRoom gagal: {exc}")
        hasil["verdict"] = "TCP_GAGAL"
        return hasil

    def _is_target_room(f):
        return (len(f) >= 6 and f[0] == 0x06 and f[1] == 1
                and int.from_bytes(f[2:6], "big") == rm)

    joined, closed = fr.read_until(_is_target_room, 4)
    if closed:
        print("[!] koneksi tertutup saat JoinRoom.")
        hasil["verdict"] = "TCP_GAGAL"
        return hasil
    if joined is not None:
        hasil["joined_room"] = rm
        print(f"[+] JoinedRoom({rm}) TERKONFIRMASI.")
    else:
        print("[!] TIDAK ada konfirmasi JoinedRoom — tetap lanjut.")

    for key, name in (("rename1", name1), ("rename2", name2)):
        # Kuras sisa event supaya hasil rename sebelumnya tidak kebawa.
        for _ in fr.drain(0.5):
            pass
        print(f"[*] {key}: kirim UpdateNickname {name!r} "
              f"(tunggu update_name_result, 5 dtk)...")
        entry = {"name": name, "success": False, "server_name": None,
                 "error": None, "wallet": None, "spent": None}
        try:
            sock.sendall(L.build_room_event_varint(
                "profile_update", build_update_nickname(name)))
        except Exception as exc:
            print(f"[!] {key} kirim gagal: {exc}")
            entry["error"] = str(exc)
            hasil[key] = entry
            continue
        fields, ev_names = listen_for(fr, "update_name_result", 5)
        if fields:
            nm = fields.get(1)
            err = fields.get(2)
            wallet = parse_money(fields.get(3))
            spent = parse_money(fields.get(4))
            entry["server_name"] = nm if isinstance(nm, str) else None
            entry["error"] = err if isinstance(err, str) else None
            entry["wallet"] = wallet
            entry["spent"] = spent
            entry["success"] = bool(isinstance(nm, str) and nm)
            print(f"[+] {key}: Name={nm!r} Error={err!r} "
                  f"Wallet={wallet} Spent={spent}")
        else:
            print(f"[*] {key}: tidak ada update_name_result (5 dtk).")
            entry["error"] = "NO_RESPONSE"
        hasil[key] = entry
        time.sleep(1)
    try:
        sock.close()
    except Exception:
        pass

    r1 = hasil["rename1"]
    r2 = hasil["rename2"]
    # KOREKSI 2026-11-17: Spent=None berarti server TIDAK MENGIRIM field
    # Spent — BUKAN Spent=0. Jangan klaim gratis dari field yang tidak ada.
    s1 = r1.get("spent")
    s2 = r2.get("spent")
    if r1.get("success") and r2.get("success"):
        if s1 is None or s2 is None:
            hasil["verdict"] = "SPENT_TIDAK_DIKIRIM"
        else:
            tot1 = s1.get("gems", 0) + s1.get("coins", 0)
            tot2 = s2.get("gems", 0) + s2.get("coins", 0)
            hasil["verdict"] = ("GRATIS_KEDUANYA" if tot1 == 0 and tot2 == 0
                                else "KEDUA_BAYAR")
    elif r1.get("success") and not r2.get("success"):
        hasil["verdict"] = "KEDUA_DITOLAK"
    else:
        hasil["verdict"] = "GAGAL"
    print("========== HASIL RENAME DUA KALI ==========")
    print(f"Bot: {email}  Rename1: {r1.get('server_name')!r} "
          f"Spent={r1.get('spent')}  Rename2: {r2.get('server_name')!r} "
          f"Spent={r2.get('spent')}  Error2={r2.get('error')!r}")
    print(f"VERDICT: {hasil['verdict']}")
    print("==========================================")
    return hasil


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("RENAME_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("RENAME_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("RENAME_DEVICE_ID", ""))
    ap.add_argument("--name1", default=os.getenv("RENAME_NAME1", "TesNama1"))
    ap.add_argument("--name2", default=os.getenv("RENAME_NAME2", "TesNama2"))
    args = ap.parse_args()
    if not args.email or not args.password:
        bot = fetch_unnamed_bot(os.getenv("SUPABASE_URL", ""),
                                os.getenv("SUPABASE_SERVICE_KEY", ""))
        if not bot or not bot.get("email") or not bot.get("password"):
            print("[!] tidak ada bot tanpa nama fresh.")
            return 1
        args.email = bot["email"]
        args.password = bot["password"]
        args.device_id = bot.get("device_id") or args.device_id
    if not args.device_id:
        args.device_id = "9f31c2d4-probe-device"
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await rename_twice(session, args.email, args.password,
                                   args.device_id, args.name1, args.name2)
    print("RENAME_TWICE_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
