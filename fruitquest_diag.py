# -*- coding: utf-8 -*-
"""
fruitquest_diag.py — Probe Fruit Quest (quest kumpulkan buah) 1 bot tes
(pkxdshop.id@gmail.com). BACA-SAJA + 1x Request lalu Cancel.

Latar (2026-11-19, dump_3):
- RoomEvent "fruit", enum FruitEventId: Request=0, Start=1, Cancel=2,
  GetCount=3, Recharge=4, MarkFinished=5, GetReward=6.
- RequestFruitQuestRequest: f1 EventId=0, f2 RewardIds
  (QuestRewardStarsAdIds, opsional), f3 IsThereXpToReceive (bool).
- CreateFruitQuestResponse ("fruit-quest-created"): f1 QuestId (str),
  f2 Items (repeated int), f3 Amounts (repeated int), f4 Rewards,
  f5 Timeout (detik), f6 XpToReceive, f7 BuffStatus.
- QuestGetMoneyRewardResponse: f1 Wallet, f2 QuestReward — QuestReward
  yang SAMA dengan Crazy Run (XpEarned + CurrentPetXp). Jadi hadiah
  quest buah MEMANG berisi XP pet.
- Respons server berupa RoomEvent terpisah: "fruit-quest-created",
  "fruit-quest-count", "fruit-quest-cooldown", "fruit-quest-limit",
  "fruit-quest-recharge", "fruit-quest-stars", "fruit-quest-reward".
- Alur probe: login -> alokasi -> TCP login -> tunggu tbugv (3 dtk)
  -> SendDeviceId -> GetCount -> Request (xp=true) -> bila quest
  dibuat: Cancel -> GetCount lagi -> cetak FRUITQUEST_JSON.
- TIDAK kirim Start / MarkFinished / GetReward: buah harus dikumpulkan
  di dunia (jalan ke posisinya) — bagian itu belum terpecahkan, dan
  probe ini hanya mengukur angka-angkanya: xpToReceive, jumlah buah,
  timeout, dan limit harian (apakah Request memakan jatah harian).
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

FRUIT_RESP_NAMES = {
    "fruit", "fruit-quest-created", "fruit-quest-count",
    "fruit-quest-cooldown", "fruit-quest-limit",
    "fruit-quest-recharge", "fruit-quest-stars", "fruit-quest-reward",
}

EV_REQUEST = 0
EV_START = 1
EV_CANCEL = 2
EV_GETCOUNT = 3
EV_RECHARGE = 4
EV_MARKFINISHED = 5
EV_GETREWARD = 6


def _add(out, field, val):
    if field in out:
        if isinstance(out[field], list):
            out[field].append(val)
        else:
            out[field] = [out[field], val]
    else:
        out[field] = val


def _try_str(chunk):
    try:
        s = chunk.decode("utf-8")
    except Exception:
        return chunk
    if s and all(c.isprintable() for c in s):
        return s
    return chunk


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


def _packed_varints(chunk):
    """Bytes repeated-int yang ter-packing (wire-2) -> list int."""
    vals = []
    i = 0
    n = len(chunk)
    while i < n:
        v = 0
        s = 0
        while i < n:
            b = chunk[i]
            i += 1
            v |= (b & 0x7F) << s
            if not (b & 0x80):
                break
            s += 7
        vals.append(v)
    return vals


def _jsonable(v):
    if isinstance(v, dict):
        return {str(k): _jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return v


def _ints(v):
    """Field repeated int: bisa varint lepas, list varint, atau bytes
    packed — normalisasi jadi list int."""
    if v is None:
        return []
    if isinstance(v, int):
        return [v]
    if isinstance(v, (bytes, bytearray)):
        return _packed_varints(bytes(v))
    if isinstance(v, list):
        out = []
        for x in v:
            if isinstance(x, int):
                out.append(x)
            elif isinstance(x, (bytes, bytearray)):
                out.extend(_packed_varints(bytes(x)))
        return out
    return []


def parse_created(data):
    d = parse_protobuf(data)
    return {"quest_id": d.get(1),
            "items": _ints(d.get(2)),
            "amounts": _ints(d.get(3)),
            "timeout": d.get(5),
            "xp_to_receive": d.get(6),
            "buff_status": d.get(7),
            "raw": _jsonable(d)}


async def fruitquest_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "login_ok": False, "deviceid_sent": False,
             "count_awal": None, "count_akhir": None,
             "quest": None, "cancel_sent": False,
             "cancel_ok": None, "error": None}
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

        def wait_fruit(timeout=12):
            """Baca frame sampai dapat RoomEvent fruit* atau timeout.
            Kembalikan (nama_event, data_bytes) atau (None, None)."""
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name in FRUIT_RESP_NAMES:
                        return name, d
            return None, None

        def fruit_request(label, payload, timeout=12):
            """Kirim RoomEvent 'fruit' + tunggu respons; maks 1 ulang."""
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("fruit", payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None, None
                print(f"[*] kirim {label} ({payload.hex()}), "
                      f"percobaan {attempt}/2, tunggu maks {timeout} dtk...")
                name, data = wait_fruit(timeout)
                if data is not None:
                    print(f"[+] respons '{name}' untuk {label} "
                          f"({len(data)} B): {data.hex()}")
                    return name, data
                print(f"[!] tidak ada respons fruit untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None, None

        # 1. GetCount — batas harian sebelum Request.
        name, data = fruit_request("GetCount", b"\x08" + L.varint(EV_GETCOUNT))
        if data is not None:
            hasil["count_awal"] = {"event": name,
                                   "fields": _jsonable(parse_protobuf(data)),
                                   "raw_hex": data.hex()}
            print(f"[+] Count awal: {hasil['count_awal']}")
        else:
            print("[!] GetCount: TIDAK ADA respons.")

        # 2. Request quest (xp=true). Percobaan 1 tanpa f2 RewardIds;
        # bila tak ada respons, percobaan 2 dengan f2 = pesan kosong.
        name, data = fruit_request(
            "Request (xp=true)",
            b"\x08" + L.varint(EV_REQUEST) + b"\x18\x01")
        if data is None:
            print("[*] Request tanpa RewardIds tak dijawab — coba dengan "
                  "f2 = pesan kosong.")
            name, data = fruit_request(
                "Request (xp=true, f2 kosong)",
                b"\x08" + L.varint(EV_REQUEST) + b"\x12\x00\x18\x01")
        if data is not None and name == "fruit-quest-created":
            hasil["quest"] = parse_created(data)
            print(f"[+] Quest dibuat: {hasil['quest']}")
        elif data is not None:
            hasil["quest"] = {"event": name, "raw_hex": data.hex(),
                              "fields": _jsonable(parse_protobuf(data))}
            print(f"[!] Request dijawab '{name}' (bukan created): "
                  f"{hasil['quest']}")
        else:
            print("[!] Request: TIDAK ADA respons.")

        # 3. Cancel quest yang dibuat (bersih-bersih; jangan sampai
        # timer jalan / quest nyangkut).
        qid = None
        if isinstance(hasil.get("quest"), dict):
            qid = hasil["quest"].get("quest_id")
        if isinstance(qid, str) and qid:
            qb = qid.encode("utf-8")
            payload = (b"\x08" + L.varint(EV_CANCEL)
                       + b"\x12" + L.varint(len(qb)) + qb)
            hasil["cancel_sent"] = True
            name, data = fruit_request("Cancel", payload, timeout=6)
            hasil["cancel_ok"] = data is not None
            print(f"[+] Cancel: respons={name}, data="
                  f"{data.hex() if data else None}")
        else:
            print("[*] Tidak ada quest_id — Cancel TIDAK dikirim.")

        # 4. GetCount lagi — apakah Request memakan jatah harian?
        name, data = fruit_request("GetCount (sesudah)",
                                   b"\x08" + L.varint(EV_GETCOUNT))
        if data is not None:
            hasil["count_akhir"] = {"event": name,
                                    "fields": _jsonable(parse_protobuf(data)),
                                    "raw_hex": data.hex()}
            print(f"[+] Count akhir: {hasil['count_akhir']}")
        else:
            print("[!] GetCount sesudah: TIDAK ADA respons.")
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
            hasil = await fruitquest_once(
                session, args.email, args.password, args.device_id)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "login_ok": False, "deviceid_sent": False,
                     "count_awal": None, "count_akhir": None,
                     "quest": None, "cancel_sent": False,
                     "cancel_ok": None,
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] fruitquest_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL FRUIT QUEST ==========")
    q = hasil.get("quest") or {}
    print(f"Login OK: {hasil.get('login_ok')}")
    print(f"Count awal: {hasil.get('count_awal')}")
    print(f"Quest: id={q.get('quest_id')} items={q.get('items')} "
          f"amounts={q.get('amounts')} timeout={q.get('timeout')} "
          f"xp_to_receive={q.get('xp_to_receive')}")
    print(f"Cancel dikirim: {hasil.get('cancel_sent')} "
          f"(respons: {hasil.get('cancel_ok')})")
    print(f"Count akhir: {hasil.get('count_akhir')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("FRUITQUEST_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
