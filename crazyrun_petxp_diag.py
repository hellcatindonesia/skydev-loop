"""Diag Crazy Run + XP pet — SATU bot, SATU run.

Alur (sesuai dump_3, diverifikasi 2026-11-19):
  login -> TCP login -> baca XP pet SEBELUM (RoomEvent "pet" PageRequest,
    PetData f13 = ExperiencePoints)
  -> ACTIVATE pet (RoomEvent "pet", ActivatePetRequest: f1 EventId=4,
    f2 petId) — ini "equip pet" versi bot: pet jadi ActivePet (user
    variable 'ap'), syarat WasWithPet() true
  -> Crazy Run: Count -> JoinGame -> tunggu Start -> GameEnd dengan
    IsThereXpToReceive=TRUE (EndCrazyRunRequest f4) -> baca PlayerResult
    (QuestReward: XpEarned + CurrentPetXp)
  -> QuestTrigger CRAZY_RUN_FINISHED dengan wasWithPet=TRUE
  -> Count SESUDAH -> baca XP pet SESUDAH -> HASIL (delta XP).

Pet default: FalconNeon pkxdshop 378425aa-4d7e-4f14-84bd-4ac551c6c1cf
(stage 2, training gym-nya baru selesai 2026-11-19 10:20).

Kredensial TIDAK disimpan di file ini (input workflow / env).
"""
import argparse
import asyncio
import os
import socket
import struct
import sys
import time
import uuid

import lookup_uuid as L

VERBOSE = True

EV_JOIN = 0
EV_FINISH = 1
EV_COUNT = 4
EV_LEAVE = 6

PET_CMD = "pet"
PET_EV_ACTIVATE = 4
FALCONNEON = "378425aa-4d7e-4f14-84bd-4ac551c6c1cf"


def vlog(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs, flush=True)


def build_cr_request(event_id):
    return b"\x08" + L.varint(event_id)


def build_cr_end(game_id, run_time, is_xp=True):
    g = game_id.encode("utf-8")
    return (b"\x08" + L.varint(EV_FINISH)
            + b"\x12" + L.varint(len(g)) + g
            + b"\x1d" + struct.pack("<f", float(run_time))
            + b"\x20" + L.varint(1 if is_xp else 0))


def build_pet_page_request(page_size=100, page_key=""):
    """PageRequest utk list pet (dari pet_list_diag.py)."""
    inner = b"\x08" + L.varint(0) + b"\x12" + L.varint(0)
    pk = page_key.encode("utf-8")
    out = b"\x08" + L.varint(0)
    out += b"\x12" + L.varint(len(pk)) + pk
    out += b"\x18" + L.varint(page_size)
    out += b"\x22" + L.varint(len(inner)) + inner
    return out


def build_pet_activate(pet_id):
    """ActivatePetRequest: f1 EventId=4, f2 petId (string)."""
    p = pet_id.encode("utf-8")
    return b"\x08" + L.varint(PET_EV_ACTIVATE) + b"\x12" + L.varint(len(p)) + p


def _pb_value_number(num):
    return b"\x11" + struct.pack("<d", float(num))


def _pb_value_string(s):
    b = s.encode("utf-8")
    return b"\x1a" + L.varint(len(b)) + b


def _pb_value_bool(flag):
    return b"\x20" + L.varint(1 if flag else 0)


def _pb_map_entry(key, value_bytes):
    k = key.encode("utf-8")
    return (b"\x0a" + L.varint(len(k)) + k
            + b"\x12" + L.varint(len(value_bytes)) + value_bytes)


def build_quest_trigger(run_time, quest_id="48fca873-32d5-4d01-bc2c-094ee2a32daf",
                        was_with_pet=True):
    attrs = [
        ("activeQuestId", _pb_value_string(quest_id)),
        ("timeSpent", _pb_value_number(run_time)),
        ("starsCollected", _pb_value_number(0)),
        ("fellInWater", _pb_value_bool(False)),
        ("playerCount", _pb_value_number(1)),
        ("wasWithPet", _pb_value_bool(was_with_pet)),
        ("powerUpUsed", _pb_value_number(0)),
    ]
    out = b"\x08" + L.varint(3)
    t = b"CRAZY_RUN_FINISHED"
    out += b"\x12" + L.varint(len(t)) + t
    for key, val in attrs:
        entry = _pb_map_entry(key, val)
        out += b"\x1a" + L.varint(len(entry)) + entry
    return out


def _raw_fields(buf):
    """Yield (field_no, wire_type, value) generik (tahan truncated)."""
    i = 0
    n = len(buf)
    while i < n:
        tag = buf[i]
        i += 1
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            val = 0
            shift = 0
            while i < n:
                b = buf[i]
                i += 1
                val |= (b & 0x7F) << shift
                shift += 7
                if not (b & 0x80):
                    break
            yield field, wire, val
        elif wire == 2:
            ln = 0
            shift = 0
            while i < n:
                b = buf[i]
                i += 1
                ln |= (b & 0x7F) << shift
                shift += 7
                if not (b & 0x80):
                    break
            if ln < 0 or i + ln > n:
                break
            chunk = buf[i:i + ln]
            i += ln
            yield field, wire, chunk
        elif wire == 5:
            raw = buf[i:i + 4]
            if len(raw) < 4:
                break
            i += 4
            yield field, wire, struct.unpack("<f", raw)[0]
        elif wire == 1:
            raw = buf[i:i + 8]
            if len(raw) < 8:
                break
            i += 8
            yield field, wire, struct.unpack("<d", raw)[0]
        else:
            break


def parse_protobuf_fields(data):
    out = []
    for fno, wt, v in _raw_fields(data):
        if wt == 2:
            try:
                v = bytes(v).decode("utf-8")
            except Exception:
                v = bytes(v).hex()
            out.append((fno, "string/bytes", v))
        elif wt == 0:
            out.append((fno, "varint", v))
        elif wt == 5:
            out.append((fno, "float32", v))
        elif wt == 1:
            out.append((fno, "float64", v))
    return out


def _parse_petdata(data):
    """PetData: f3 PetId, f4 DisplayName, f6 Stage, f12 Type,
    f13 ExperiencePoints (varint)."""
    pet = {"pet_id": None, "display_name": None, "type": None,
           "owner_id": None, "stage": None, "xp": None}
    for f3, w3, v3 in _raw_fields(bytes(data)):
        if f3 == 1 and w3 == 2:
            try:
                pet["owner_id"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 3 and w3 == 2:
            try:
                pet["pet_id"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 4 and w3 == 2:
            try:
                pet["display_name"] = bytes(v3).decode("utf-8")
            except Exception:
                pass
        elif f3 == 6 and w3 == 0:
            pet["stage"] = v3
        elif f3 == 12 and w3 == 0:
            pet["type"] = v3
        elif f3 == 13 and w3 == 0:
            pet["xp"] = v3
    return pet


def extract_pets_from_event(data):
    pets = []
    for fno, wt, v in _raw_fields(data):
        if fno == 1 and wt == 0 and v == 8:
            for f2, w2, v2 in _raw_fields(data):
                if f2 == 4 and w2 == 2:
                    p = _parse_petdata(bytes(v2))
                    if p["pet_id"]:
                        pets.append(p)
            return pets
        if fno == 3 and wt == 2:
            for f2, w2, v2 in _raw_fields(bytes(v)):
                if f2 == 4 and w2 == 2:
                    p = _parse_petdata(bytes(v2))
                    if p["pet_id"]:
                        pets.append(p)
    return pets


def decode_room_event(frame):
    if len(frame) < 2 or frame[0] != 0x08:
        return None, None
    ln, m = L.read_varint(frame, 1)
    if ln is None:
        return None, None
    name = frame[1 + m:1 + m + ln].decode("utf-8", "replace")
    return name, frame[1 + m + ln:]


def _is_tbugv(frame):
    if len(frame) >= 2 and frame[0] == 0x05:
        ln, m = L.read_varint(frame, 1)
        if ln is not None and ln < 0x80 and frame[1 + m:1 + m + ln] == b"tbugv":
            return True
    return False


def _is_joinedroom(frame):
    return len(frame) >= 6 and frame[0] == 0x06 and frame[1] == 1


class FrameReader:
    def __init__(self, sock):
        self.s = sock
        self.buf = bytearray()

    def read_until(self, pred, seconds):
        deadline = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            while len(self.buf) >= 4:
                ln = struct.unpack(">I", self.buf[:4])[0]
                if ln > 262144:
                    return None, False
                if len(self.buf) - 4 < ln:
                    break
                fr = bytes(self.buf[4:4 + ln])
                del self.buf[:4 + ln]
                if fr == b"\x01":
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
                if pred(fr):
                    return fr, False
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return None, True
            if chunk == b"":
                return None, True
            self.buf += chunk
        return None, False

    def drain(self, seconds):
        out = []
        deadline = time.time() + seconds
        self.s.settimeout(0.5)
        while time.time() < deadline:
            while len(self.buf) >= 4:
                ln = struct.unpack(">I", self.buf[:4])[0]
                if ln > 262144:
                    return out
                if len(self.buf) - 4 < ln:
                    break
                fr = bytes(self.buf[4:4 + ln])
                del self.buf[:4 + ln]
                if fr == b"\x01":
                    try:
                        self.s.sendall(struct.pack(">I", 1) + b"\x02")
                    except Exception:
                        pass
                    continue
                out.append(fr)
            try:
                chunk = self.s.recv(65536)
            except socket.timeout:
                continue
            except Exception:
                return out
            if chunk == b"":
                return out
            self.buf += chunk
        return out


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("CRAZYRUN_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("CRAZYRUN_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("CRAZYRUN_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--pet-id",
                    default=os.getenv("CRAZYRUN_PET_ID", FALCONNEON))
    ap.add_argument("--command",
                    default=os.getenv("CRAZYRUN_COMMAND", "crazy-run"))
    ap.add_argument("--time", type=float,
                    default=float(os.getenv("CRAZYRUN_TIME", "12")))
    ap.add_argument("--wait-start", type=float,
                    default=float(os.getenv("CRAZYRUN_WAIT_START", "10")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    pet_id = args.pet_id
    print(f"[*] Diag Crazy Run + XP pet — bot {args.email} | pet {pet_id} "
          f"| finish {args.time} dtk")
    L.QUIET = False
    hasil = {"pet_id": pet_id, "xp_sebelum": None, "xp_sesudah": None,
             "activate_events": [], "count_sebelum": None,
             "count_sesudah": None, "game_id": None,
             "player_result": None, "player_result_hex": None,
             "quest_sent": None}
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        user_jwt, alloc = await L.login_and_allocate(
            args.email, args.password, args.device_id, session)
        if not alloc:
            print("[!] login/alokasi gagal.")
            return 1
        host, port = L.find_host_port(alloc)
        if not host or not port:
            print("[!] host/port tidak ketemu di allocation.")
            return 1
        print(f"[*] alokasi OK: {host}:{port}")
        sock, _pid = L.tcp_connect(host, port)
        if sock is None:
            print("[!] TCP connect gagal.")
            return 1
        try:
            sock.sendall(L.build_login_frame(user_jwt))
        except Exception as exc:
            print(f"[!] kirim login frame gagal: {exc}")
            return 1
        fr = FrameReader(sock)
        print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
        got, closed = fr.read_until(
            lambda f: _is_tbugv(f) or _is_joinedroom(f), 3)
        if closed:
            print("[!] koneksi tertutup saat tunggu login.")
            return 1
        if got is not None and _is_tbugv(got):
            time.sleep(1)

        def kirim_room(cmd, data_pb, label):
            print(f"[*] kirim {label} via RoomEvent '{cmd}' ...")
            try:
                sock.sendall(L.build_room_event_varint(cmd, data_pb))
                return True
            except Exception as exc:
                print(f"[!] kirim {label} gagal: {exc}")
                return False

        def baca_xp_pet(label):
            """Kirim PageRequest, kumpulkan XP pet target. Return XP/None."""
            req = build_pet_page_request()
            for cmd in ("pet", "pets"):
                try:
                    sock.sendall(L.build_room_event_varint(cmd, req))
                except Exception:
                    continue
                print(f"[*] PageRequest pet ({label}) via '{cmd}', "
                      f"tunggu 6 dtk...")
                frames = fr.drain(6)
                for f in frames:
                    name, data = decode_room_event(f)
                    if name in ("pet", "pets") and data:
                        for p in extract_pets_from_event(data):
                            if p["pet_id"] == pet_id:
                                print(f"[+] XP pet {label}: xp={p['xp']} "
                                      f"stage={p['stage']} type={p['type']}")
                                return p["xp"]
                # bila respons datang tapi pet tak ketemu, jangan ulang cmd
                break
            print(f"[!] pet {pet_id} tidak ketemu di daftar ({label}).")
            return None

        # 0) XP SEBELUM
        hasil["xp_sebelum"] = baca_xp_pet("SEBELUM")

        # 1) ACTIVATE pet (equip)
        if not kirim_room(PET_CMD, build_pet_activate(pet_id),
                          f"ActivatePet {pet_id}"):
            return 1
        frames = fr.drain(5)
        for f in frames:
            name, data = decode_room_event(f)
            if name in ("pet", "pets") and data:
                hx = data.hex()
                hasil["activate_events"].append(hx)
                print(f"[+] respons 'pet' Activate ({len(data)} B): {hx[:300]}")
                for fno, wt, v in parse_protobuf_fields(data):
                    print(f"      field {fno} ({wt}): {v!r}")

        # 2) Crazy Run — Count SEBELUM
        def tunggu(seconds, label):
            print(f"[*] tunggu respons {label} ({seconds} dtk)...")
            frames = fr.drain(seconds)
            for f in frames:
                name, data = decode_room_event(f)
                if name is None:
                    continue
                vlog(f"[evt] {name!r} {data.hex() if data else ''}")
                if not data:
                    continue
                fields = parse_protobuf_fields(data)
                if name == "crazy-run-count":
                    d = dict((fn, v) for fn, _w, v in fields)
                    if 2 in d and 3 in d:
                        if hasil["count_sebelum"] is None:
                            hasil["count_sebelum"] = d[3]
                            print(f"[+] Count SEBELUM: RunCount={d[3]} "
                                  f"energi={d[2]}")
                        else:
                            hasil["count_sesudah"] = d[3]
                            print(f"[+] Count SESUDAH: RunCount={d[3]} "
                                  f"energi={d[2]}")
                if name == "crazy-run-result":
                    d = dict((fn, v) for fn, _w, v in fields)
                    hasil["player_result"] = {
                        "run_count": d.get(9), "run_time": d.get(7)}
                    hasil["player_result_hex"] = data.hex()
                    print(f"[+] PlayerResult: RunCount={d.get(9)} "
                          f"RunTime={d.get(7)}")
                    print(f"    RAW HEX: {data.hex()}")
                    for fn, wt, v in fields:
                        print(f"      field {fn} ({wt}): {v!r}")
            return frames

        kirim_room(args.command, build_cr_request(EV_COUNT),
                   "GetUserData Count-sebelum")
        tunggu(4, "Count-sebelum")

        # 3) JoinGame
        kirim_room(args.command, build_cr_request(EV_JOIN), "JoinGame")
        frames = tunggu(6, "JoinGame")
        for f in frames:
            name, data = decode_room_event(f)
            if name != "crazy-run-joined" or not data:
                continue
            for fno, wt, v in parse_protobuf_fields(data):
                if fno == 2 and wt == "string/bytes" and isinstance(v, str):
                    hasil["game_id"] = v
                    print(f"[+] GameId: {v}")
                    break
            if hasil["game_id"]:
                break

        # 4) Tunggu Start
        print(f"[*] tunggu Start ({args.wait_start} dtk)...")
        tunggu(args.wait_start, "Start")

        # 5) GameEnd dengan IsThereXpToReceive=TRUE
        gid = hasil["game_id"] or ""
        kirim_room(args.command, build_cr_end(gid, args.time, True),
                   f"GameEnd(time={args.time}, isXp=TRUE)")
        tunggu(6, "Finish/PlayerResult")

        # 6) QuestTrigger wasWithPet=TRUE
        print("[*] kirim QuestTrigger 'CRAZY_RUN_FINISHED' "
              "(wasWithPet=TRUE) via RoomEvent 'quests' ...")
        try:
            sock.sendall(L.build_room_event_varint(
                "quests", build_quest_trigger(args.time, was_with_pet=True)))
            hasil["quest_sent"] = True
        except Exception as exc:
            print(f"[!] kirim QuestTrigger gagal: {exc}")
            hasil["quest_sent"] = False
        tunggu(4, "QuestTrigger")

        # 7) Count SESUDAH
        kirim_room(args.command, build_cr_request(EV_COUNT),
                   "GetUserData Count-sesudah")
        tunggu(4, "Count-sesudah")

        # 8) LeaveGame
        kirim_room(args.command, build_cr_request(EV_LEAVE), "LeaveGame")
        tunggu(2, "Leave")

        # 9) XP SESUDAH (sesi masih sama)
        hasil["xp_sesudah"] = baca_xp_pet("SESUDAH")
        try:
            sock.close()
        except Exception:
            pass

    print("=" * 56)
    print("HASIL CRAZY RUN + XP PET")
    print(f"  Pet: {pet_id}")
    print(f"  XP SEBELUM: {hasil['xp_sebelum']}")
    print(f"  XP SESUDAH: {hasil['xp_sesudah']}")
    if hasil["xp_sebelum"] is not None and hasil["xp_sesudah"] is not None:
        d = hasil["xp_sesudah"] - hasil["xp_sebelum"]
        print(f"  DELTA XP: {d} {'(NAIK — BERHASIL)' if d > 0 else '(TIDAK NAIK)'}")
    print(f"  Activate respons 'pet': {len(hasil['activate_events'])} event")
    print(f"  RunCount: {hasil['count_sebelum']} -> {hasil['count_sesudah']}")
    print(f"  PlayerResult: {hasil['player_result']}")
    if hasil["player_result_hex"]:
        print(f"  PlayerResult RAW: {hasil['player_result_hex']}")
    print(f"  QuestTrigger terkirim: {hasil['quest_sent']}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
