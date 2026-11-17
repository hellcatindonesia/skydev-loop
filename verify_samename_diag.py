# -*- coding: utf-8 -*-
"""
verify_samename_diag.py — Verifikasi apakah rename #2 benar-benar menempel.

Latar: rename_twice (run 44097453509) mengembalikan Name='TesNama2'
tanpa penolakan, TAPI dengan catatan "has no old name" dan tanpa
Wallet/Spent. Verifikasi via SearchFriend gagal total (v1: pencari =
bot itu sendiri; v2/v3: search tidak jalan di instance itu — kontrol
SkyDev pun tidak ketemu, join gagal).

Tes ini (satu variabel, jalur yang SUDAH TERBUKTI jalan):
login sebagai bot yang di-rename, kirim UpdateNickname('TesNama2') LAGI.
- Server MENOLAK dengan error sama-nama ("same") -> NAMA_MENEMPEL:
  nama TesNama2 memang sudah jadi nama bot itu di server.
- Server MENERIMA lagi (Name='TesNama2', tanpa error sama-nama) ->
  NAMA_TIDAK_MENEMPEL: rename #2 tadi tidak menempel (echo saja).
- Error lain -> laporkan apa adanya (TIDAK_DIKETAHUI).

Bot diambil langsung dari tabel accounts panel berdasarkan email
(bukan RPC fresh — harus bot yang SAMA). Password TIDAK pernah
dicetak ke log.
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
    return b"\x08" + _varint(9) + _sfield(2, name)


def fetch_bot_by_email(supabase_url, supabase_key, email):
    """Ambil 1 baris accounts berdasarkan email (service key).
    Kembalikan {email, password, device_id} atau None."""
    url = (f"{supabase_url}/rest/v1/accounts?email=eq.{email}"
           f"&select=email,password,device_id")
    req = urllib.request.Request(url, method="GET")
    req.add_header("apikey", supabase_key)
    req.add_header("Authorization", f"Bearer {supabase_key}")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"[-] Gagal mengambil bot dari database: {exc}")
        return None
    if not isinstance(data, list) or not data:
        print(f"[-] accounts kosong untuk email {email}.")
        return None
    bot = data[0]
    print(f"[+] Bot: {bot.get('email')} "
          f"(device_id {bot.get('device_id')})")
    return bot


def listen_for(fr, wanted, seconds):
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


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv(
        "VERIFY_EMAIL", "g43dtt893wuk@outlook.com"))
    ap.add_argument("--password", default=os.getenv("VERIFY_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("VERIFY_DEVICE_ID", ""))
    ap.add_argument("--name", default=os.getenv("VERIFY_NAME", "TesNama2"))
    args = ap.parse_args()
    if not args.password:
        bot = fetch_bot_by_email(os.getenv("SUPABASE_URL", ""),
                                 os.getenv("SUPABASE_SERVICE_KEY", ""),
                                 args.email)
        if not bot or not bot.get("password"):
            print("[!] bot tidak ketemu / tidak ada password.")
            return 1
        args.password = bot["password"]
        args.device_id = bot.get("device_id") or args.device_id
    if not args.device_id:
        args.device_id = "9f31c2d4-probe-device"

    hasil = {"email": args.email, "name": args.name,
             "server_name": None, "error": None,
             "wallet": None, "spent": None, "verdict": "UNKNOWN"}
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        user_jwt, alloc = await L.login_and_allocate(
            args.email, args.password, args.device_id, session)
        if not alloc:
            print("[!] login/alokasi gagal.")
            hasil["verdict"] = "LOGIN_GAGAL"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        srv, rm = parse_gsid(alloc.get("gameServerId", ""))
        print(f"[+] alokasi OK: server={srv} room={rm}")
        if not srv or rm is None:
            hasil["verdict"] = "ALOKASI_GAGAL"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        host, port = L.find_host_port(alloc)
        if not host or not port:
            hasil["verdict"] = "ALOKASI_GAGAL"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        sock, _pid = L.tcp_connect(host, port)
        if sock is None:
            print("[!] TCP connect gagal.")
            hasil["verdict"] = "TCP_GAGAL"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        try:
            sock.sendall(L.build_login_frame(user_jwt))
        except Exception as exc:
            print(f"[!] kirim login frame gagal: {exc}")
            hasil["verdict"] = "TCP_GAGAL"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        fr = G.FrameReader(sock)
        print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
        got, closed = fr.read_until(
            lambda f: G._is_tbugv(f) or G._is_joinedroom(f), 3)
        if closed:
            print("[!] koneksi tertutup saat tunggu login.")
            hasil["verdict"] = "TCP_GAGAL"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        if got is not None and G._is_tbugv(got):
            time.sleep(1)
        print(f"[*] kirim UpdateNickname {args.name!r} LAGI "
              f"(tunggu update_name_result, 5 dtk)...")
        try:
            sock.sendall(L.build_room_event_varint(
                "profile_update", build_update_nickname(args.name)))
        except Exception as exc:
            print(f"[!] kirim gagal: {exc}")
            hasil["verdict"] = "TCP_GAGAL"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 1
        fields, _ev = listen_for(fr, "update_name_result", 5)
        try:
            sock.close()
        except Exception:
            pass
        if not fields:
            print("[*] tidak ada update_name_result (5 dtk).")
            hasil["verdict"] = "TIDAK_DIKETAHUI"
            print("VERIFY_SAMENAME_JSON:"
                  + json.dumps(hasil, separators=(",", ":")))
            return 0
        nm = fields.get(1)
        err = fields.get(2)
        hasil["server_name"] = nm if isinstance(nm, str) else None
        hasil["error"] = err if isinstance(err, str) else None
        print(f"[+] Jawaban server: Name={nm!r} Error={err!r} fields={fields}")
        err_low = (err or "").lower()
        if "same" in err_low or "sama" in err_low:
            hasil["verdict"] = "NAMA_MENEMPEL"
        elif isinstance(nm, str) and nm:
            hasil["verdict"] = "NAMA_TIDAK_MENEMPEL"
        else:
            hasil["verdict"] = "TIDAK_DIKETAHUI"
    print("========== HASIL VERIFIKASI NAMA SAMA ==========")
    print(f"Bot: {args.email}  Nama dites: {args.name!r}  "
          f"Name={hasil['server_name']!r} Error={hasil['error']!r}")
    print(f"VERDICT: {hasil['verdict']}")
    print("===============================================")
    print("VERIFY_SAMENAME_JSON:" + json.dumps(hasil, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
