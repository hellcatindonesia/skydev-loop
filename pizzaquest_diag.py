# -*- coding: utf-8 -*-
"""
pizzaquest_diag.py — Probe Pizza Quest (antar pizza) 1 bot tes
(pkxdshop.id@gmail.com). MENYELESAIKAN quest sungguhan (bukan baca-saja)
karena hadiahnya hanya keluar bila quest diselesaikan.

Latar (2026-11-19, dump_3 + pengalaman mod Bartes):
- RoomEvent "pizza", PizzaEventType: Started=0, Finished=1, Created=2.
- Alur: Create (f1=2) -> Start/Get (f1=0: RankSTime, IsThereXp,
  Waypoints, RewardIds) -> tunggu -> Finish (f1=1: QuestId, TotalTime,
  QuestCompleted, AdWatched, IsThereXp, RewardIds).
- Respons: "pizza_created" (rank rewards), "pizza_started"
  (f1 QuestId, f2 PlayerId, f3 XpToReceive, f4 Waypoints, f5 Rewards),
  "pizza_finished" (f1 QuestId, f2 PlayerId, f3 Wallet, f4 Reward,
  f5 ScoreReward) — Reward = QuestReward yang SAMA dengan Crazy Run
  (XpEarned + CurrentPetXp). Jadi quest pizza MEMBERI XP pet.
- Pengalaman Bartes (mod): accept quest lalu paksa finish LANGSUNG =
  0 reward; dengan jeda 5 dtk = berhasil. Berarti server memvalidasi
  TotalTime (minimal ~5 dtk). Probe ini memakai TotalTime=6.
- TIDAK ada GetCount/limit/cooldown di mekanik pizza (tidak seperti
  fruit quest 3/hari) — probe menyelesaikan 2 quest berurutan untuk
  melihat apakah quest ke-2 masih dibayar (uji batas).
- Pet aktif saat ini (type 2) SUDAH MAX (XP 320) — XpEarned tetap
  terbaca dari Reward, dan xpToReceive diumumkan di Started response.
  CurrentPetXp diharapkan tetap 320 (cap).
Alur probe: login -> alokasi -> TCP login -> tunggu tbugv (3 dtk)
-> SendDeviceId -> quest 1 (Create/Start/tunggu 6 dtk/Finish)
-> quest 2 (sama) -> cetak PIZZAQUEST_JSON.
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

PIZZA_RESP_NAMES = {"pizza", "pizza_created", "pizza_started",
                    "pizza_finished"}

EV_STARTED = 0
EV_FINISHED = 1
EV_CREATED = 2

WAIT_AFTER_START = 6      # jeda validasi server (~5 dtk) + margin
FINISH_TOTAL_TIME = 6


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


def _nested(v, depth=0):
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


def _ints(v):
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


def parse_started(data):
    d = parse_protobuf(data)
    return {"quest_id": d.get(1),
            "player_id": d.get(2),
            "xp_to_receive": d.get(3),
            "waypoints": _ints(d.get(4)),
            "raw": _jsonable(_nested(d))}


def parse_finished(data):
    d = parse_protobuf(data)
    reward = d.get(4)
    rew = _nested(reward) if reward is not None else None
    xp_earned = None
    cur_pet_xp = None
    if isinstance(rew, dict):
        xp_earned = rew.get(3)
        cur_pet_xp = rew.get(4)
    return {"quest_id": d.get(1),
            "player_id": d.get(2),
            "wallet": _jsonable(_nested(d.get(3)))
            if d.get(3) is not None else None,
            "xp_earned": xp_earned,
            "current_pet_xp": cur_pet_xp,
            "reward_raw": _jsonable(rew),
            "raw": _jsonable(_nested(d))}


async def pizzaquest_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "login_ok": False, "deviceid_sent": False,
             "quests": [], "error": None}
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

        def wait_pizza(timeout=12):
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name in PIZZA_RESP_NAMES:
                        return name, d
            return None, None

        def pizza_request(label, payload, timeout=12):
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("pizza", payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None, None
                print(f"[*] kirim {label} ({payload.hex()}), "
                      f"percobaan {attempt}/2, tunggu maks {timeout} dtk...")
                name, data = wait_pizza(timeout)
                if data is not None:
                    print(f"[+] respons '{name}' untuk {label} "
                          f"({len(data)} B): {data.hex()}")
                    return name, data
                print(f"[!] tidak ada respons pizza untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None, None

        def build_start(rank_s_time, waypoints):
            p = b"\x08" + L.varint(EV_STARTED)
            p += b"\x10" + L.varint(int(rank_s_time))
            p += b"\x18\x01"  # IsThereXpToReceive = true
            for w in waypoints:
                p += b"\x20" + L.varint(int(w))
            return p

        def build_finish(quest_id, total_time):
            qb = quest_id.encode("utf-8")
            return (b"\x08" + L.varint(EV_FINISHED)
                    + b"\x12" + L.varint(len(qb)) + qb
                    + b"\x18" + L.varint(int(total_time))
                    + b"\x20\x01"   # QuestCompleted = true
                    + b"\x28\x00"   # AdWatched = false
                    + b"\x30\x01")  # IsThereXpToReceive = true

        def do_quest(n):
            q = {"n": n, "created": None, "started": None,
                 "finished": None, "seconds": None}
            t0 = time.time()
            # 1. Create
            name, data = pizza_request(
                f"Create #{n}", b"\x08" + L.varint(EV_CREATED))
            if data is not None:
                d = parse_protobuf(data)
                q["created"] = {"event": name,
                                "fields": _jsonable(_nested(d)),
                                "raw_hex": data.hex()}
                print(f"[+] Create #{n}: {q['created']}")
            else:
                print(f"[!] Create #{n}: TIDAK ADA respons.")
                q["seconds"] = round(time.time() - t0, 1)
                return q
            # Waypoints kandidat dari respons Create (bila ada field
            # repeated-int).
            cand_wp = []
            for _k, _v in (q["created"]["fields"] or {}).items():
                ints = _ints(_v)
                if ints and all(isinstance(x, int) for x in ints):
                    cand_wp = ints
                    break
            # 2. Start — coba RankSTime=0 + waypoints kosong; bila tak
            # dijawab, coba RankSTime=90; lalu waypoints dari Create.
            started = None
            for rank_s_time, wps, tag in (
                    (0, [], "RankSTime=0"),
                    (90, [], "RankSTime=90"),
                    (90, cand_wp, "RankSTime=90+waypoints Create")):
                if started is not None:
                    break
                if tag.startswith("RankSTime=90+waypoints") and not cand_wp:
                    continue
                name, data = pizza_request(
                    f"Start #{n} ({tag})", build_start(rank_s_time, wps))
                if data is not None and name == "pizza_started":
                    started = parse_started(data)
                    started["rank_s_time_sent"] = rank_s_time
                    started["waypoints_sent"] = wps
            if started is None:
                print(f"[!] Start #{n}: TIDAK ADA respons pizza_started.")
                q["seconds"] = round(time.time() - t0, 1)
                return q
            q["started"] = started
            print(f"[+] Started #{n}: {started}")
            # 3. Tunggu validasi server (~5 dtk), lalu Finish.
            print(f"[*] tunggu {WAIT_AFTER_START} dtk (validasi server)...")
            time.sleep(WAIT_AFTER_START)
            qid = started.get("quest_id")
            if not isinstance(qid, str) or not qid:
                print(f"[!] Start #{n} tanpa quest_id — Finish batal.")
                q["seconds"] = round(time.time() - t0, 1)
                return q
            name, data = pizza_request(
                f"Finish #{n} (TotalTime={FINISH_TOTAL_TIME})",
                build_finish(qid, FINISH_TOTAL_TIME))
            if data is not None and name == "pizza_finished":
                q["finished"] = parse_finished(data)
                print(f"[+] Finished #{n}: {q['finished']}")
            elif data is not None:
                q["finished"] = {"event": name, "raw_hex": data.hex()}
                print(f"[!] Finish #{n} dijawab '{name}': {q['finished']}")
            else:
                print(f"[!] Finish #{n}: TIDAK ADA respons.")
            q["seconds"] = round(time.time() - t0, 1)
            return q

        hasil["quests"].append(do_quest(1))
        # Quest ke-2: uji apakah masih dibayar (batas harian?).
        if hasil["quests"][0].get("finished"):
            hasil["quests"].append(do_quest(2))
        else:
            print("[*] Quest 1 gagal — quest 2 tidak dicoba.")
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
            hasil = await pizzaquest_once(
                session, args.email, args.password, args.device_id)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "login_ok": False, "deviceid_sent": False,
                     "quests": [],
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] pizzaquest_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL PIZZA QUEST ==========")
    for q in hasil.get("quests") or []:
        st = q.get("started") or {}
        fi = q.get("finished") or {}
        print(f"Quest #{q.get('n')}: {q.get('seconds')} dtk | "
              f"xp_to_receive={st.get('xp_to_receive')} | "
              f"xp_earned={fi.get('xp_earned')} | "
              f"current_pet_xp={fi.get('current_pet_xp')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=========================================")
    print("PIZZAQUEST_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
