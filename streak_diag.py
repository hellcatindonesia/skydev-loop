# -*- coding: utf-8 -*-
"""
streak_diag.py — Probe Daily Streak (klaim harian beruntun) 1 bot tes
(pkxdshop.id@gmail.com).

Latar (analisis dump — ~/workspace/daily_streak_analysis.md §7):
- Sistem bernama Daily Streak, mekanik server ToxicBirdDailyStreakMechanic.
  RoomEvent TERPISAH per aksi (bukan satu event + sub-key):
    STRKS = cek status, STRKR = klaim, STRKF = freeze, STRKFR = reset paksa.
  SEMUA request payload KOSONG (0 byte): request message hanya punya
  TimeZone=0, RewardId="", RewardCount=0 — semuanya default proto3.
- Respons status (DailyStreakStatusResponse): f1 CurrentStreak,
  f2 StreakInformations[] (per periode: f1 StreakPeriod, f2 StreakOffset,
  f3 StreakRangeStart, f4 LootBoxIds[], f5 PetEggId[]), f3 CollectedReward
  (bool), f4 LostStreak (bool), f5 CanFreezeStreak (bool),
  f6 FreezeDaysCount (int).
- Respons klaim (DailyStreakCollectRewardResponse): f1 CurrentStreak,
  f2 LootBoxRewards[] (tiap: f1 LootBoxId, f2 LootBoxItems[]),
  f3 PetEggRewards[], f4 Wallet (Money f1=koin, f2=gems).
  TIDAK ADA flag Successful — sukses dideteksi dari isi (hadiah
  non-empty / wallet berubah / CurrentStreak naik / CollectedReward
  false -> true sesudah klaim).
- Mekanik terdaftar di ToxicBirdClient (bukan room): TIDAK perlu
  destinationUserId, TIDAK ada syarat room/activity, Bartes tidak
  perlu online. TIDAK kirim JoinRoom.
- Desain probe (§7.3): STRKS -> bila CollectedReward=false, kirim
  STRKR SEKALI -> STRKS lagi (verifikasi CollectedReward=true).
  TIDAK kirim STRKR kedua di hari yang sama pada probe pertama.
  TIDAK kirim STRKF / STRKFR di probe ini.
  Bila CollectedReward sudah true saat status pertama: JANGAN klaim.
Alur: login -> alokasi sendiri -> TCP login -> tunggu jendela login
(tbugv, maks 3 dtk) -> SendDeviceId ('Analytics/deviceId') ->
STRKS -> (bila belum diklaim: STRKR 1x) -> STRKS verifikasi ->
cetak STREAK_JSON.
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


def parse_streak_status(data):
    d = parse_protobuf(data)
    infos = d.get(2)
    return {"current_streak": d.get(1),
            "streak_informations": _jsonable(_nested(infos))
            if infos is not None else None,
            "collected_reward": bool(d.get(3)),
            "lost_streak": bool(d.get(4)),
            "can_freeze": bool(d.get(5)),
            "freeze_days": d.get(6)}


def parse_streak_claim(data):
    d = parse_protobuf(data)
    loot = d.get(2)
    eggs = d.get(3)
    wallet = d.get(4)
    return {"current_streak": d.get(1),
            "lootbox_rewards": _jsonable(_nested(loot))
            if loot is not None else None,
            "pet_egg_rewards": _jsonable(_nested(eggs))
            if eggs is not None else None,
            "wallet": _jsonable(_nested(wallet))
            if wallet is not None else None}


async def streak_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "status_awal": None, "claim": None, "claim_sent": False,
             "status_akhir": None, "error": None}
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

        def wait_event(name, timeout=10):
            """Baca frame sampai dapat RoomEvent `name` atau timeout.
            Kembalikan data protobuf (bytes) atau None."""
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    ev_name, d = G.decode_room_event(f)
                    if ev_name == name:
                        return d
            return None

        def streak_request(label, name, parse_fn, timeout=10):
            """Kirim RoomEvent `name` (payload kosong) + tunggu respons
            dengan nama yang sama; maks 1 kirim ulang."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint(name, b""))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} ('{name}', kosong), "
                      f"percobaan {attempt}/2, tunggu respons maks "
                      f"{timeout} dtk...")
                data = wait_event(name, timeout)
                if data is not None:
                    print(f"[+] respons '{name}' diterima untuk {label} "
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
                print(f"[!] tidak ada respons '{name}' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        # 1. Status awal.
        hasil["status_awal"] = streak_request(
            "Status (STRKS)", "STRKS", parse_streak_status)
        if isinstance(hasil["status_awal"], dict):
            print(f"[+] Status awal: {hasil['status_awal']}")
        else:
            print("[!] Status awal: TIDAK ADA respons — berhenti.")
            hasil["error"] = "tidak ada respons status awal"
            return hasil
        if "parse_error" in hasil["status_awal"]:
            hasil["error"] = "parse status awal gagal"
            return hasil

        # 2. Klaim SEKALI — hanya bila belum diklaim hari ini.
        if hasil["status_awal"].get("collected_reward"):
            print("[*] CollectedReward sudah true — JANGAN klaim hari ini.")
        else:
            print("[*] Belum diklaim hari ini — kirim STRKR SEKALI.")
            hasil["claim_sent"] = True
            hasil["claim"] = streak_request(
                "Klaim (STRKR)", "STRKR", parse_streak_claim)
            if isinstance(hasil["claim"], dict):
                print(f"[+] Klaim: {hasil['claim']}")
            else:
                print("[!] Klaim: TIDAK ADA respons.")

        # 3. Status akhir (verifikasi CollectedReward false -> true).
        hasil["status_akhir"] = streak_request(
            "Status verifikasi (STRKS)", "STRKS", parse_streak_status)
        if isinstance(hasil["status_akhir"], dict):
            print(f"[+] Status akhir: {hasil['status_akhir']}")
        else:
            print("[!] Status akhir: TIDAK ADA respons.")
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
            hasil = await streak_once(
                session, args.email, args.password, args.device_id)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "server": "", "room": None, "player_id": None,
                     "login_ok": False, "deviceid_sent": False,
                     "status_awal": None, "claim": None, "claim_sent": False,
                     "status_akhir": None,
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] streak_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL DAILY STREAK ==========")
    st0 = hasil.get("status_awal") or {}
    cl = hasil.get("claim") or {}
    st1 = hasil.get("status_akhir") or {}
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    print(f"Status awal: CurrentStreak={st0.get('current_streak')} "
          f"Collected={st0.get('collected_reward')} "
          f"Lost={st0.get('lost_streak')} "
          f"CanFreeze={st0.get('can_freeze')} "
          f"FreezeDays={st0.get('freeze_days')}")
    print(f"  StreakInformations: {st0.get('streak_informations')}")
    if hasil.get("claim_sent"):
        print(f"Klaim: CurrentStreak={cl.get('current_streak')} "
              f"LootBox={cl.get('lootbox_rewards')} "
              f"PetEgg={cl.get('pet_egg_rewards')} "
              f"Wallet={cl.get('wallet')}")
    else:
        print("Klaim: TIDAK dikirim (sudah diklaim / status gagal).")
    print(f"Status akhir: CurrentStreak={st1.get('current_streak')} "
          f"Collected={st1.get('collected_reward')} "
          f"Lost={st1.get('lost_streak')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("STREAK_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
