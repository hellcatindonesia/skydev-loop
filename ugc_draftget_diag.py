# -*- coding: utf-8 -*-
"""
ugc_draftget_diag.py — Probe ambil data SATU DRAFT map UGC milik akun
login (READ-ONLY, tidak ada pembuatan/edit/publish).

Tujuan: melihat isi draft yang belum di-publish — item apa saja
yang sudah ditaruh client asli (ID, posisi, rotasi, color_id) dan
room-nya — supaya tes edit berikutnya memakai data yang persis
sama seperti client.

Wire format (TERBUKTI via stringliteral.json + dump_3):
- Request: RoomEvent "activityDraft/get", payload = protobuf
  GetActivityDraftRequest { f1 ActivityId string }.
  (stringliteral 0x8B37B48 = 'activityDraft/get';
  GetActivityDraftRequest.ActivityIdFieldNumber = 1.)
- Respons sukses = RoomEvent "get-activity-draft" (0x8B41700),
  error = "get-activity-draft-error" (0x8B41708).
- Payload "get-activity-draft" = protobuf GetActivityDraftResponse:
  f1 Data (ActivityData: f1 OwnerId, f2 ActivityId, f3 PublicationId,
  f4 Type=TerrainType, f5 Items repeated ActivityItem,
  f6 Rooms repeated ActivityRoom),
  f2 Metadata (ActivityDraftMetadata: f1 Id, f2 Name, f3 ThumbnailUrl,
  f4 ActivityType, f5 Tags).
  ActivityItem: f1 Id, f2 Position Vector3, f3 Rotation Vector3,
  f4 ColorId, f5 EquippedItems repeated int32.
  ActivityRoom: f1 Floor, f2 Walls repeated int32.
  Vector3: f1 X, f2 Y, f3 Z (float fixed32).

Alur: login -> alokasi default -> TCP connect -> login frame ->
tunggu JoinedRoom/tbugv -> kirim init + "activityDraft/get"
(payload GetActivityDraftRequest) -> tunggu "get-activity-draft"
maks 20 dtk -> parse -> cetak isi draft + UGCDRAFTGET_JSON.
CATATAN: ini TIDAK join world mana pun dan TIDAK menambah counter
kunjungan (hanya baca draft milik akun).
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G
import ugc_fetch_diag as F

REQ_EVENT = "activityDraft/get"
RESP_EVENT = "get-activity-draft"
RESP_ERROR_EVENT = "get-activity-draft-error"


def _enc_varint(v):
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _enc_string_field(field_no, s):
    b = s.encode("utf-8")
    return bytes([(field_no << 3) | 2]) + _enc_varint(len(b)) + b


def build_get_draft_request(activity_id):
    return _enc_string_field(1, activity_id)


def parse_get_draft_response(data):
    """GetActivityDraftResponse: f1 Data (ActivityData), f2 Metadata."""
    f = F._parse_fields(data)
    raw_data = F._first(f, 1, 2)
    if raw_data is None:
        raise ValueError("GetActivityDraftResponse: field 1 (Data) "
                         "tidak ada")
    ad = F._parse_activity_data(raw_data)
    raw_meta = F._first(f, 2, 2)
    meta = None
    if raw_meta is not None:
        mf = F._parse_fields(raw_meta)
        meta = {"id": F._str_field(mf, 1),
                "name": F._str_field(mf, 2),
                "thumbnail_url": F._str_field(mf, 3),
                "activity_type": F._int_field(mf, 4)}
    return {"activity_data": ad, "metadata": meta}


async def ugc_draftget_once(session, email, password, device_id,
                            activity_id):
    hasil = {"email": email, "login_ok": False,
             "joined": False, "get_sent": False,
             "activity_id_target": activity_id,
             "draft": None, "error": None, "events_seen": []}
    user_jwt, _alloc = await L.login_and_allocate(
        email, password, device_id, session)
    if not user_jwt:
        hasil["error"] = "login/alokasi awal gagal"
        print("[!] login/alokasi awal gagal.")
        return hasil
    hasil["login_ok"] = True
    body = _alloc
    host, port = L.find_host_port(body)
    if not host or not port:
        hasil["error"] = "host/port tidak ketemu di allocation default"
        print("[!] host/port default tidak ketemu.")
        return hasil
    srv, rm = F.parse_gsid(body.get("gameServerId", ""))
    print(f"[+] server={srv} room={rm} host={host}:{port}")
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
    try:
        print("[*] tunggu JoinedRoom/tbugv (maks ~5 dtk)...")
        joined, via_tbugv, fr = F.wait_joined(sock, rm)
        if not joined:
            hasil["error"] = "JoinedRoom/tbugv tidak terkonfirmasi"
            print("[!] JoinedRoom/tbugv tidak terkonfirmasi.")
            return hasil
        hasil["joined"] = True
        print(f"[+] joined (via_tbugv={via_tbugv}).")
        try:
            for name in ["init_pos", "init_day_night", "preset",
                         "fetch_events"]:
                sock.sendall(L.build_room_event_varint(name, b""))
            time.sleep(0.3)
            payload = build_get_draft_request(activity_id)
            sock.sendall(L.build_room_event_varint(REQ_EVENT, payload))
            hasil["get_sent"] = True
            print(f"[+] '{REQ_EVENT}' terkirim "
                  f"(activityId={activity_id}).")
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId",
                (device_id or F.DEFAULT_DEVICE_ID).encode("utf-8")))
            print("[+] init + SendDeviceId terkirim.")
        except Exception as exc:
            hasil["error"] = f"kirim init gagal: {exc}"
            print(f"[!] kirim init gagal: {exc}")
            return hasil

        print(f"[*] tunggu respons '{RESP_EVENT}' maks 20 dtk...")
        data = None
        err_data = None
        end = time.time() + 20
        while time.time() < end:
            for f in fr.drain(0.5):
                ev_name, d = G.decode_room_event(f)
                if ev_name is not None:
                    hasil["events_seen"].append(ev_name)
                    print(f"[.] event lewat: '{ev_name}' "
                          f"({len(d) if d else 0} B)")
                    if ev_name == RESP_EVENT:
                        data = d
                        break
                    if ev_name == RESP_ERROR_EVENT:
                        err_data = d
                        break
            if data is not None or err_data is not None:
                break
        if err_data is not None:
            hasil["error"] = (f"server balas '{RESP_ERROR_EVENT}' "
                              f"({len(err_data)} B): {err_data.hex()}")
            print(f"[!] {hasil['error']}")
            return hasil
        if data is None:
            hasil["error"] = (f"tidak ada respons '{RESP_EVENT}' "
                              f"dalam 20 dtk")
            print(f"[!] {hasil['error']}")
            return hasil
        print(f"[+] respons '{RESP_EVENT}' diterima ({len(data)} B).")
        print("UGCDRAFTGET_RAW_HEX:" + data.hex())
        try:
            dump = F._dump_generic(data)
            print("UGCDRAFTGET_DUMP_JSON:" + json.dumps(
                F._jsonable(dump), separators=(",", ":")))
        except Exception as exc:
            print(f"[!] dump generik gagal: {exc!r}")
        try:
            parsed = parse_get_draft_response(data)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil["error"] = f"parse respons gagal: {exc!r}"
            hasil["raw_hex_len"] = len(data)
            print(f"[!] parse gagal: {exc!r}")
            return hasil
        ad = parsed["activity_data"]
        draft = {"activity_id": ad["activity_id"],
                 "owner_id": ad["owner_id"],
                 "publication_id": ad["publication_id"],
                 "terrain_type": ad["type"],
                 "placed_items": ad["items"],
                 "rooms": ad["rooms"],
                 "metadata": parsed["metadata"]}
        hasil["draft"] = draft
        print(f"[+] DRAFT: activity={ad['activity_id']} "
              f"owner={ad['owner_id']} type={ad['type']} "
              f"items={len(ad['items'])} rooms={len(ad['rooms'])} "
              f"name={parsed['metadata'] and parsed['metadata'].get('name')!r}")
        for it in ad["items"]:
            print(f"    item id={it.get('id')} pos={it.get('position')} "
                  f"rot={it.get('rotation')} color={it.get('color_id')} "
                  f"eq={it.get('equipped_items')}")
        for r in ad["rooms"]:
            print(f"    room floor={r.get('floor')} "
                  f"walls={r.get('walls')}")
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
    ap.add_argument("--activity-id", default=os.getenv(
        "DB_ACTIVITY_ID", ""))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    if not args.activity_id:
        print("[!] activity_id wajib diisi.")
        return 1
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await ugc_draftget_once(
                session, args.email, args.password, args.device_id,
                args.activity_id)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "login_ok": False,
                     "joined": False, "get_sent": False,
                     "activity_id_target": args.activity_id,
                     "draft": None,
                     "error": f"{type(exc).__name__}: {exc}",
                     "events_seen": []}
            print(f"[!] ugc_draftget_once gagal: "
                  f"{type(exc).__name__}: {exc}")
    hasil = F._jsonable(hasil)
    print("")
    print("========== HASIL UGC DRAFT GET (READ-ONLY) ==========")
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"joined: {hasil.get('joined')}  "
          f"get sent: {hasil.get('get_sent')}")
    print(f"ActivityId target: {hasil.get('activity_id_target')}")
    d = hasil.get("draft") or {}
    print(f"ActivityId respons: {d.get('activity_id')}")
    print(f"OwnerId: {d.get('owner_id')}  "
          f"TerrainType: {d.get('terrain_type')}  "
          f"items: {len(d.get('placed_items') or [])}  "
          f"rooms: {len(d.get('rooms') or [])}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print(f"Events terlihat: {hasil.get('events_seen')}")
    print("=====================================================")
    if d:
        print("UGCDRAFTGET_JSON:" + json.dumps(
            d, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
