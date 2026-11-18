# -*- coding: utf-8 -*-
"""
gumball_diag.py — Probe Gumball (mesin gacha) READ-ONLY, 1 bot tes
(pkxdshop.id@gmail.com). TIDAK ADA pembelian (Buy TIDAK PERNAH dikirim).

Latar (analisis dump_3 + disassembly libil2cpp.so lama — 2026-11-18):
- Mekanik server ToxicBirdArcadeMachineMechanic, COMMAND_KEY "gumball".
  Satu-satunya tipe mesin adalah GUMBALL (ArcadeMachineType cuma punya
  nilai itu). Tiap mesin: MachineId (string), Price (Money — koin/gems/
  tiket/event currency, diputuskan server), Cap (max beli + sisa + timer
  reset).
- Request (protobuf, RoomEvent "gumball"):
  * GetInfo: GetGumballInfoRequest, f1 EventId=2 -> `08 02`.
  * GetProbabilityInfo: GetGumballProbabilityInfoRequest, f1 EventId=1,
    f2 MachineId (string) -> `08 01` + `12 <len> <machineId>`.
    (TERBUKTI via disassembly: SendMachineProbabilityInfoRequest
    membangun GetGumballProbabilityInfoRequest + set_MachineId, BUKAN
    GumballRequest — GumballRequest hanya punya f1 EventId.)
  * Buy: BuyGumballRequest, f1 EventId=0, f2 MachineId -> TIDAK DIPAKAI
    di probe ini.
- Respons GetInfo (GetGumballInfoResponse): f1 EventId (=2), f2
  Successful (bool), f3 ErrorMessage (string), f4 infos (repeated
  GumballInfo: f1 machineId, f2 price Money, f3 cap GumballCap
  {f1 timestamp long, f2 max int, f3 remaining int}).
- Respons Probability (GetGumballProbabilityInfoResponse): f1 EventId
  (=1), f2 Successful, f3 ErrorMessage, f4 items (repeated
  LootBoxItemProbabilityInfo: f1 LootBoxItem {f1 type enum, f2 id,
  f3 quantity, f4 money Money, f5 unique, f6 presentation, f7 pet
  repeated, f8 quality enum}, f2 probability float).
  LootBoxItemType: 0 Empty, 1 MoneyReward, 2 ItemReward, 3 PetReward,
  4 VehicleReward. Quality: 0 Trash, 1 Common, 2 Uncommon, 3 Rare,
  4 Epic, 5 Legendary.
- Respons ditunggu di RoomEvent "gumball" (mekanik ini tidak punya
  sub-key seperti Daily Bonus); semua nama event yang lewat dicatat
  untuk jaga-jaga bila respons datang dengan nama lain.
- Mekanik terdaftar di ToxicBirdClient (bukan room): TIDAK perlu
  destinationUserId, TIDAK ada syarat room/activity, Bartes tidak
  perlu online. TIDAK kirim JoinRoom.
Alur: login -> alokasi sendiri -> TCP login -> tunggu jendela login
(tbugv, maks 3 dtk) -> SendDeviceId ('Analytics/deviceId') ->
GetInfo -> untuk tiap mesin (maks 10) GetProbabilityInfo ->
cetak GUMBALL_JSON.
Tiap request ditunggu responsnya maks 20 dtk, maks 1 kirim ulang
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
EVENT = "gumball"
MAX_MACHINES_PROB = 10

TYPE_NAMES = {0: "Empty", 1: "MoneyReward", 2: "ItemReward",
              3: "PetReward", 4: "VehicleReward"}
QUALITY_NAMES = {0: "Trash", 1: "Common", 2: "Uncommon", 3: "Rare",
                 4: "Epic", 5: "Legendary"}


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


def _varint_bytes(value):
    out = bytearray()
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
    return tag + _varint_bytes(len(raw)) + raw


def _money_str(m):
    """Money dict {1: koin, 2: gems, 3: tiket, 4: event} -> teks ringkas."""
    if not isinstance(m, dict):
        return str(m)
    parts = []
    if m.get(1):
        parts.append(f"{m[1]} koin")
    if m.get(2):
        parts.append(f"{m[2]} gems")
    if m.get(3):
        parts.append(f"{m[3]} tiket")
    ev = m.get(4)
    if ev:
        evs = ev if isinstance(ev, list) else [ev]
        for e in evs:
            if isinstance(e, dict):
                parts.append(f"{e.get(2)} {e.get(1)}")
    return " + ".join(parts) if parts else "gratis/0"


def parse_gumball_info(data):
    d = parse_protobuf(data)
    machines = []
    infos = d.get(4)
    if infos is not None:
        infos = infos if isinstance(infos, list) else [infos]
        for raw in infos:
            m = _jsonable(_nested(raw))
            cap = m.get(3) if isinstance(m, dict) else None
            machines.append({
                "machine_id": m.get(1) if isinstance(m, dict) else None,
                "price": m.get(2) if isinstance(m, dict) else None,
                "cap": cap if isinstance(cap, dict) else cap,
            })
    return {"event_id": d.get(1), "successful": d.get(2),
            "error": d.get(3), "machines": machines}


def parse_gumball_prob(data):
    d = parse_protobuf(data)
    items = []
    raw_items = d.get(4)
    if raw_items is not None:
        raw_items = raw_items if isinstance(raw_items, list) else [raw_items]
        for raw in raw_items:
            it = _jsonable(_nested(raw))
            loot = it.get(1) if isinstance(it, dict) else None
            if not isinstance(loot, dict):
                items.append({"raw": it,
                              "probability": it.get(2)
                              if isinstance(it, dict) else None})
                continue
            items.append({
                "type": TYPE_NAMES.get(loot.get(1), loot.get(1)),
                "id": loot.get(2),
                "quantity": loot.get(3),
                "money": loot.get(4),
                "unique": loot.get(5),
                "pet": loot.get(7),
                "quality": QUALITY_NAMES.get(loot.get(8), loot.get(8)),
                "probability": it.get(2),
            })
    return {"event_id": d.get(1), "successful": d.get(2),
            "error": d.get(3), "items": items}


async def gumball_once(session, email, password, device_id):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "info": None, "probabilities": {}, "error": None}
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

        def wait_event(name, timeout=20):
            """Baca frame sampai dapat RoomEvent `name` atau timeout.
            Mencatat SEMUA nama RoomEvent yang lewat (untuk melihat bila
            respons datang dengan nama lain). Kembalikan data protobuf
            (bytes) atau None."""
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

        def gumball_request(label, payload, parse_fn, timeout=20):
            """Kirim RoomEvent 'gumball' + tunggu respons 'gumball';
            maks 1 kirim ulang."""
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
                        print(f"[!] parse respons {label} gagal: {exc!r} "
                              f"(raw hex di atas)")
                        import traceback
                        traceback.print_exc()
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons '{EVENT}' untuk {label} "
                      f"(percobaan {attempt}/2).")
            return None

        # 1. GetInfo — daftar semua mesin + harga + cap.
        hasil["info"] = gumball_request(
            "GetInfo (EventId=2)", bytes.fromhex("0802"),
            parse_gumball_info)
        if not isinstance(hasil["info"], dict):
            print("[!] GetInfo: TIDAK ADA respons — berhenti.")
            hasil["error"] = "tidak ada respons GetInfo"
            return hasil
        if "parse_error" in hasil["info"]:
            hasil["error"] = "parse GetInfo gagal"
            return hasil
        machines = hasil["info"].get("machines") or []
        print(f"[+] GetInfo: successful={hasil['info'].get('successful')} "
              f"mesin={len(machines)}")
        for m in machines:
            cap = m.get("cap") or {}
            print(f"    - {m.get('machine_id')}: "
                  f"harga={_money_str(m.get('price'))} "
                  f"cap max={cap.get(2)} sisa={cap.get(3)}")

        # 2. GetProbabilityInfo per mesin (maks 10) — isi + peluang.
        for m in machines[:MAX_MACHINES_PROB]:
            mid = m.get("machine_id")
            if not mid:
                continue
            payload = bytes.fromhex("0801") + _str_field(2, mid)
            prob = gumball_request(
                f"GetProbabilityInfo ({mid})", payload,
                parse_gumball_prob)
            hasil["probabilities"][mid] = prob
            if isinstance(prob, dict) and "items" in prob:
                print(f"[+] Prob {mid}: {len(prob['items'])} item")
                for it in prob["items"][:12]:
                    print(f"      {it.get('type')} id={it.get('id')} "
                          f"x{it.get('quantity')} "
                          f"money={_money_str(it.get('money'))} "
                          f"q={it.get('quality')} "
                          f"p={it.get('probability')}")
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
            hasil = await gumball_once(
                session, args.email, args.password, args.device_id)
        except Exception as exc:
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "server": "", "room": None, "player_id": None,
                     "login_ok": False, "deviceid_sent": False,
                     "info": None, "probabilities": {},
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] gumball_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL GUMBALL (READ-ONLY) ==========")
    info = hasil.get("info") or {}
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    print(f"GetInfo successful: {info.get('successful')}  "
          f"mesin: {len(info.get('machines') or [])}")
    for m in (info.get("machines") or []):
        cap = m.get("cap") or {}
        print(f"  {m.get('machine_id')}: {_money_str(m.get('price'))} "
              f"(cap {cap.get(3)}/{cap.get(2)})")
        for it in ((hasil.get("probabilities") or {}).get(
                m.get("machine_id")) or {}).get("items", [])[:8]:
            print(f"    - {it.get('type')} id={it.get('id')} "
                  f"x{it.get('quantity')} q={it.get('quality')} "
                  f"p={it.get('probability')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("=================================================")
    print("GUMBALL_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
