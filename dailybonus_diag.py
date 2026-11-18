# -*- coding: utf-8 -*-
"""
dailybonus_diag.py — Probe Daily Bonus (klaim harian 5 hari) 1 bot tes
(pkxdshop.id@gmail.com).

Latar (analisis dump_3 + disassembly libil2cpp.so lama — 2026-11-18):
- Sistem bernama Daily Bonus, mekanik server ToxicBirdDailyBonusMechanic.
  TERBUKTI via .rela.dyn: SendGetDailyBonusCount DAN SendCollectReward
  sama-sama mengirim RoomEvent "daily_bonus" (slot statik 0x8904228,
  addend 0x8B3E9C8 = "daily_bonus"). Sub-key "A"/"B" BUKAN nama event —
  yang membedakan count vs klaim adalah f1 EventId di dalam protobuf.
- Request DailyBonusRewardRequest: f1 EventId (enum: 0=CollectWithReward
  [iklan ditonton], 1=CollectWithoutReward [gratis, tanpa iklan],
  2=GetCount), f2-f6 Day1-5 RewardId, f7-f11 Day1-5 Ad RewardId.
  Byte: Count = `08 02` (+ 10 RewardId bila ada); Klaim gratis =
  `08 01` (+ 10 RewardId); Klaim iklan = hanya 10 RewardId (f1=0).
  RewardId aslinya dari Remote Config ("reward_config"), TAPI server
  meng-echo RewardId yang ia miliki di respons count — jadi probe ini
  mengambil RewardId dari respons count lalu mengirimnya balik saat
  klaim. Percobaan pertama: count dikirim TANPA RewardId (`08 02` saja).
- Respons count (DailyBonusStatusResponse): f1 Count (int), f2-f6
  Reward1-5 (string), f7-f11 RewardAd1-5 (string). Count=-1 berarti
  bonus TIDAK tersedia (hanya muncul untuk akun yang lama tidak login
  — koreksi Bartes 2026-11-15).
- Respons klaim (DailyBonusRewardResponse): f1 Reward Money, f2 Items
  (repeated int, packed), f3 Wallet Money, f4 isLootBox (bool).
  TIDAK ADA flag Successful — sukses dideteksi dari isi (Reward/Wallet
  berubah).
- Mekanik terdaftar di ToxicBirdClient (bukan room): TIDAK perlu
  destinationUserId, TIDAK ada syarat room/activity, Bartes tidak
  perlu online. TIDAK kirim JoinRoom. TIDAK ada varian iklan di probe
  ini (klaim selalu EventId=1 = gratis).
- Desain probe: Count ("daily_bonus" `08 02`) -> bila Count >= 0, klaim
  sekali ("daily_bonus" `08 01` + RewardId dari count); bila Count=-1,
  kirim SATU klaim paksa (`08 01`, tanpa RewardId) untuk melihat respons
  server (pola yang sama seperti Daily Box --force-collect); bila count
  TIDAK ada respons, berhenti (jangan klaim buta).
Alur: login -> alokasi sendiri -> TCP login -> tunggu jendela login
(tbugv, maks 3 dtk) -> SendDeviceId ('Analytics/deviceId') ->
Count -> (klaim 1x) -> cetak DAILYBONUS_JSON.
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
EVENT = "daily_bonus"


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


def _packed_ints(v):
    """Wire-2 chunk berisi varint berurutan (repeated int packed)."""
    if isinstance(v, (bytes, bytearray)):
        out = []
        i = 0
        buf = bytes(v)
        while i < len(buf):
            val, m = L.read_varint(buf, i)
            if val is None:
                break
            out.append(val)
            i += m
        return out
    return v


def _varint_field(field_no, value):
    """Encode satu field varint (tag + varint)."""
    tag = bytes([(field_no << 3) | 0])
    out = bytearray(tag)
    v = value
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _str_field(field_no, value):
    """Encode satu field string (tag + len varint + utf-8)."""
    tag = bytes([(field_no << 3) | 2])
    raw = value.encode("utf-8")
    ln = _varint_field(0, len(raw))[1:]  # varint panjang saja
    return tag + ln + raw


def parse_bonus_count(data):
    d = parse_protobuf(data)
    rewards = {}
    for f in range(2, 12):
        if f in d:
            rewards[f] = d[f]
    return {"count": d.get(1),
            "reward_ids": _jsonable(rewards) if rewards else None}


def parse_bonus_claim(data):
    d = parse_protobuf(data)
    reward = d.get(1)
    items = d.get(2)
    wallet = d.get(3)
    return {"reward": _jsonable(_nested(reward))
            if reward is not None else None,
            "items": _packed_ints(items) if items is not None else None,
            "wallet": _jsonable(_nested(wallet))
            if wallet is not None else None,
            "is_lootbox": bool(d.get(4)) if d.get(4) is not None else None}


async def bonus_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "count": None, "claim": None, "claim_sent": False,
             "claim_forced": False, "error": None}
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

        def bonus_request(label, payload, parse_fn, timeout=10):
            """Kirim RoomEvent 'daily_bonus' + tunggu respons dengan nama
            yang sama; maks 1 kirim ulang."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint(EVENT, payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} ({payload.hex()}), "
                      f"percobaan {attempt}/2, tunggu respons maks "
                      f"{timeout} dtk...")
                data = wait_event(EVENT, timeout)
                if data is not None:
                    print(f"[+] respons '{EVENT}' diterima untuk {label} "
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
                print(f"[!] tidak ada respons '{EVENT}' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        # 1. Count — ada bonus atau tidak (EventId=2, tanpa RewardId).
        hasil["count"] = bonus_request(
            "Count (EventId=2)", bytes.fromhex("0802"), parse_bonus_count)
        if isinstance(hasil["count"], dict):
            print(f"[+] Count: {hasil['count']}")
        else:
            print("[!] Count: TIDAK ADA respons — berhenti (tanpa klaim).")
            hasil["error"] = "tidak ada respons count"
            return hasil
        if "parse_error" in hasil["count"]:
            hasil["error"] = "parse count gagal"
            return hasil

        # 2. Klaim gratis SEKALI (EventId=1 = CollectWithoutReward).
        # RewardId diambil dari respons count (f2-f11, urutan dijaga).
        cnt = hasil["count"].get("count")
        rids = hasil["count"].get("reward_ids") or {}
        payload = bytearray(bytes.fromhex("0801"))
        for f in range(2, 12):
            v = rids.get(str(f)) or rids.get(f)
            if isinstance(v, str) and v:
                payload += _str_field(f, v)
        if isinstance(cnt, int) and cnt >= 0:
            print(f"[*] Count={cnt} — kirim klaim gratis (EventId=1).")
        else:
            print(f"[*] Count={cnt} (bonus tidak tersedia) — kirim SATU "
                  "klaim paksa untuk melihat respons server.")
            hasil["claim_forced"] = True
        hasil["claim_sent"] = True
        hasil["claim"] = bonus_request(
            "Klaim gratis (EventId=1)", bytes(payload), parse_bonus_claim)
        if isinstance(hasil["claim"], dict):
            print(f"[+] Klaim: {hasil['claim']}")
        else:
            print("[!] Klaim: TIDAK ADA respons.")
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
            hasil = await bonus_once(
                session, args.email, args.password, args.device_id)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "server": "", "room": None, "player_id": None,
                     "login_ok": False, "deviceid_sent": False,
                     "count": None, "claim": None, "claim_sent": False,
                     "claim_forced": False,
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] bonus_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL DAILY BONUS ==========")
    co = hasil.get("count") or {}
    cl = hasil.get("claim") or {}
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    print(f"Count: {co.get('count')}  RewardIds: {co.get('reward_ids')}")
    if hasil.get("claim_sent"):
        print(f"Klaim (forced={hasil.get('claim_forced')}): "
              f"Reward={cl.get('reward')} Items={cl.get('items')} "
              f"Wallet={cl.get('wallet')} IsLootBox={cl.get('is_lootbox')}")
    else:
        print("Klaim: TIDAK dikirim (count gagal / tidak ada respons).")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("DAILYBONUS_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
