#!/usr/bin/env python3
"""Diagnosa Pet Gym training (pkxdshop) — cek batas iklan accelerate.

Wire (dump_3, TERBUKTI struktur):
- RoomEvent "petGym" (ToxicBirdPetGymMechanic, COMMAND_KEY="petGym").
- EventId client/server: TrainPet=0, CurrentTraining=1, Acceleration=2,
  CompleteTraining=3.
- AccelerationType: Ads=0, Currency=1.
- GetCurrentTraining: payload 08 01 (f1=1).
- Accelerate (Ads): f1=2, f2=0 (dihilangkan, proto3 default), f3=petId
  -> 08 02 1a <len> <petId>.
- TrainPet: f1=0 (dihilangkan), f2=petId -> 1a <len> <petId>.
- Respons Acceleration: f1 eventId, f2 success, f3 TrainingData
  (PetGymTrainingPetData), f4 spent (Money), f5 wallet (Money).
- PetGymTrainingPetData: f1 petId, f2 remainingTime (long), f3 progress
  (fixed32 float), f4 numberOfWatchedAds (int), f5 accelerationPrice.
- Dump: PetGymTabWindow.TIME_TO_ACCELERATE=60 (menit), MAX_ADS=3.
  Tes ini membuktikan apakah batas iklan benar 3 atau bisa lebih.

Pakai: --status-only | --accelerate N | --train PET_ID
"""
import argparse
import asyncio
import json
import os
import struct
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lookup_uuid as L
import gift_diag as G

EVENT = "petGym"
BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "petgym-diag"
CREDS_FILE = os.path.expanduser("~/.bot_test_account")


def read_creds():
    email = os.environ.get("DB_EMAIL")
    password = os.environ.get("DB_PASSWORD")
    if email and password:
        return email, password
    if os.path.exists(CREDS_FILE):
        with open(CREDS_FILE) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" in line:
                    e, p = line.split(":", 1)
                    return e.strip(), p.strip()
    return None, None


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
            val = 0
            shift = 0
            while i < n:
                b = buf[i]
                i += 1
                val |= (b & 0x7F) << shift
                shift += 7
                if not (b & 0x80):
                    break
            _add(out, str(field), val)
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
            chunk = buf[i:i + ln]
            i += ln
            _add(out, str(field), _try_str(chunk))
        elif wire == 5:
            raw = buf[i:i + 4]
            i += 4
            _add(out, str(field), struct.unpack("<f", raw)[0])
        elif wire == 1:
            raw = buf[i:i + 8]
            i += 8
            _add(out, str(field), struct.unpack("<d", raw)[0])
        else:
            break
    return out


def _varint_bytes(value):
    out = bytearray()
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def _str_field(field_no, value):
    b = value.encode("utf-8")
    return bytes([(field_no << 3) | 2]) + _varint_bytes(len(b)) + b


def _money_str(m):
    if not isinstance(m, dict):
        return str(m)
    parts = []
    if m.get("1"):
        parts.append(f"{m['1']} koin")
    if m.get("2"):
        parts.append(f"{m['2']} gems")
    if m.get("3"):
        parts.append(f"{m['3']} tiket")
    return " + ".join(parts) if parts else "0"


def parse_training_data(d):
    """PetGymTrainingPetData (nested dict dari parse generik)."""
    if not isinstance(d, dict):
        return d
    out = {}
    if "1" in d:
        out["pet_id"] = d["1"]
    if "2" in d:
        out["remaining_time"] = d["2"]
    if "3" in d:
        out["progress"] = d["3"]
    if "4" in d:
        out["ads_watched"] = d["4"]
    if "5" in d:
        price = d["5"]
        if isinstance(price, bytes):
            price = parse_protobuf(price)
        out["accel_price"] = _money_str(price)
    return out


def parse_accel_response(data):
    d = parse_protobuf(data)
    out = {"event_id": d.get("1"), "success": d.get("2")}
    td = d.get("3")
    if isinstance(td, bytes):
        td = parse_protobuf(td)
    if isinstance(td, dict):
        out["training"] = parse_training_data(td)
    spent = d.get("4")
    if isinstance(spent, bytes):
        spent = parse_protobuf(spent)
    out["spent"] = _money_str(spent) if spent is not None else None
    wallet = d.get("5")
    if isinstance(wallet, bytes):
        wallet = parse_protobuf(wallet)
    out["wallet"] = _money_str(wallet) if wallet is not None else None
    return out


def parse_current_response(data):
    """Respons GetCurrentTraining — parse generik, trainingData di f3
    bila ada (struktur sama seperti accel)."""
    d = parse_protobuf(data)
    out = {"event_id": d.get("1"), "success": d.get("2")}
    td = d.get("3")
    if isinstance(td, bytes):
        td = parse_protobuf(td)
    if isinstance(td, dict):
        out["training"] = parse_training_data(td)
    out["raw"] = {k: (v if not isinstance(v, bytes) else v.hex())
                  for k, v in d.items()}
    return out


async def petgym_once(session, email, password, device_id,
                      accelerate=0, train_pet_id=None):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "login_ok": False, "deviceid_sent": False,
             "current": None, "accelerations": [], "train": None,
             "error": None}
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

        def wait_event(name, timeout=20):
            end = time.time() + timeout
            while time.time() < end:
                for f in fr.drain(0.5):
                    ev_name, d = G.decode_room_event(f)
                    if ev_name is not None:
                        print(f"[.] event lewat: '{ev_name}' "
                              f"({len(d) if d else 0} B)")
                    if ev_name == name:
                        return d
            return None

        def petgym_request(label, payload, parse_fn, timeout=20):
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint(EVENT, payload))
                except Exception as exc:
                    print(f"[!] kirim {label} gagal: {exc}")
                    return None
                print(f"[*] kirim {label} ({payload.hex()}), "
                      f"percobaan {attempt}/2, tunggu respons '{EVENT}' "
                      f"maks {timeout} dtk...")
                data = wait_event(EVENT, timeout)
                if data is not None:
                    print(f"[+] respons '{EVENT}' diterima untuk {label} "
                          f"({len(data)} B): {data.hex()}")
                    try:
                        return parse_fn(data)
                    except Exception as exc:
                        print(f"[!] parse respons {label} gagal: {exc!r}")
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons '{EVENT}' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        # 1. Status training saat ini.
        hasil["current"] = petgym_request(
            "GetCurrentTraining (EventId=1)", bytes.fromhex("0801"),
            parse_current_response)
        cur = hasil["current"] or {}
        tr = cur.get("training") or {}
        pet_id = tr.get("pet_id")
        print(f"[+] Current: success={cur.get('success')} "
              f"pet={pet_id} sisa={tr.get('remaining_time')} "
              f"ads={tr.get('ads_watched')} "
              f"progress={tr.get('progress')}")

        # 2. Mulai training bila diminta dan belum ada yg training.
        if train_pet_id and not pet_id:
            payload = _str_field(2, train_pet_id)
            hasil["train"] = petgym_request(
                f"TrainPet {train_pet_id}", payload,
                parse_current_response)
            pet_id = train_pet_id
            print(f"[+] TrainPet: {hasil['train']}")

        # 3. Accelerate via iklan (Ads=0), berulang.
        if accelerate and pet_id:
            for n in range(1, accelerate + 1):
                payload = bytes.fromhex("0802") + _str_field(3, pet_id)
                r = petgym_request(
                    f"Accelerate-Ads #{n} pet={pet_id}", payload,
                    parse_accel_response)
                hasil["accelerations"].append(r)
                t = (r or {}).get("training") or {}
                print(f"[+] Accel #{n}: success={(r or {}).get('success')} "
                      f"sisa={t.get('remaining_time')} "
                      f"ads={t.get('ads_watched')} "
                      f"spent={(r or {}).get('spent')}")
                if r is None or (r or {}).get("success") is False:
                    print(f"[!] Accelerate #{n} GAGAL/tiada respons "
                          f"— berhenti di sini.")
                    break
                time.sleep(1)
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--status-only", action="store_true")
    ap.add_argument("--accelerate", type=int, default=0)
    ap.add_argument("--train", default=None)
    ap.add_argument("--email", default=None)
    ap.add_argument("--password", default=None)
    ap.add_argument("--device-id", default=None)
    args = ap.parse_args()
    email = args.email or os.environ.get("DB_EMAIL")
    password = args.password or os.environ.get("DB_PASSWORD")
    if not email or not password:
        email, password = read_creds()
    if not email or not password:
        print("[!] kredensial tidak ditemukan.")
        return
    device_id = args.device_id or os.environ.get(
        "DB_DEVICE_ID") or DEFAULT_DEVICE_ID
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await petgym_once(
            session, email, password, device_id,
            accelerate=args.accelerate, train_pet_id=args.train)
    print("PETGYM_JSON_BEGIN")
    print(json.dumps(hasil, ensure_ascii=False, default=str))
    print("PETGYM_JSON_END")


if __name__ == "__main__":
    asyncio.run(main())
