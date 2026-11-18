# -*- coding: utf-8 -*-
"""
ugc_drafts_diag.py — Probe daftar DRAFT map UGC milik akun login
(READ-ONLY, tidak ada pembuatan/edit/publish).

Tujuan: menemukan ActivityId map draft yang belum di-publish
(metadata API hanya menampilkan world yang sudah tayang, jadi
draft tidak kelihatan dari searchByUser).

Wire format (TERBUKTI via stringliteral.json dump LAMA 2026-11-18):
- Command key "activityDraft/list" (alamat 0x8B37B50).
- TIDAK ADA class ListActivityDraftsRequest di dump -> request
  adalah payload KOSONG (pola EMPTY_REQUEST seperti "activity/get").
- Respons sukses = RoomEvent "list-activity-drafts" (0x8B44760),
  error = "list-activity-drafts-error" (0x8B44768).
- Payload "list-activity-drafts" = protobuf ListActivityDraftsResponse:
  f1 Activities repeated ActivityDraftMetadata.
  ActivityDraftMetadata:
  f1 Id string, f2 Name string, f3 ThumbnailUrl string,
  f4 ActivityType int (= TerrainType), f5 Tags repeated ActivityTag.

Alur: login -> alokasi default -> TCP connect -> login frame ->
tunggu JoinedRoom/tbugv -> kirim init + "activityDraft/list"
(payload kosong) -> tunggu "list-activity-drafts" maks 20 dtk
-> parse -> cetak daftar draft + UGCDRAFTS_JSON.
CATATAN: ini TIDAK join world mana pun dan TIDAK menambah counter
kunjungan (hanya daftar draft milik akun).
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G
import ugc_fetch_diag as F

REQ_EVENT = "activityDraft/list"
RESP_EVENT = "list-activity-drafts"
RESP_ERROR_EVENT = "list-activity-drafts-error"


def _parse_activity_draft_metadata(buf):
    f = F._parse_fields(buf)
    tags = []
    for (w, c) in f.get(5, []):
        if w == 2:
            try:
                tf = F._parse_fields(c)
                tags.append({"id": F._int_field(tf, 1),
                             "name": F._str_field(tf, 2)})
            except Exception:
                tags.append({"raw_hex": c.hex()})
    return {"id": F._str_field(f, 1),
            "name": F._str_field(f, 2),
            "thumbnail_url": F._str_field(f, 3),
            "activity_type": F._int_field(f, 4),
            "tags": tags}


def parse_list_drafts_response(data):
    """ListActivityDraftsResponse: f1 = repeated ActivityDraftMetadata."""
    f = F._parse_fields(data)
    drafts = []
    for (w, c) in f.get(1, []):
        if w != 2:
            continue
        try:
            drafts.append(_parse_activity_draft_metadata(c))
        except Exception as exc:
            drafts.append({"parse_error": repr(exc), "raw_hex": c.hex()})
    return {"drafts": drafts}


async def ugc_drafts_once(session, email, password, device_id):
    hasil = {"email": email, "login_ok": False,
             "joined": False, "list_sent": False,
             "drafts": None, "error": None, "events_seen": []}
    user_jwt, _alloc = await L.login_and_allocate(
        email, password, device_id, session)
    if not user_jwt:
        hasil["error"] = "login/alokasi awal gagal"
        print("[!] login/alokasi awal gagal.")
        return hasil
    hasil["login_ok"] = True
    body, label = _alloc, "default"
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
            sock.sendall(L.build_room_event_varint(REQ_EVENT, b""))
            hasil["list_sent"] = True
            print(f"[+] '{REQ_EVENT}' terkirim (payload kosong).")
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
        print("UGCDRAFTS_RAW_HEX:" + data.hex())
        try:
            dump = F._dump_generic(data)
            print("UGCDRAFTS_DUMP_JSON:" + json.dumps(
                F._jsonable(dump), separators=(",", ":")))
        except Exception as exc:
            print(f"[!] dump generik gagal: {exc!r}")
        try:
            parsed = parse_list_drafts_response(data)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil["error"] = f"parse respons gagal: {exc!r}"
            hasil["raw_hex_len"] = len(data)
            print(f"[!] parse gagal: {exc!r}")
            return hasil
        hasil["drafts"] = parsed["drafts"]
        print(f"[+] DRAFTS: {len(parsed['drafts'])} draft ditemukan.")
        for d in parsed["drafts"]:
            print(f"    - id={d.get('id')} name={d.get('name')!r} "
                  f"type={d.get('activity_type')} "
                  f"tags={len(d.get('tags') or [])}")
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
            hasil = await ugc_drafts_once(
                session, args.email, args.password, args.device_id)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "login_ok": False,
                     "joined": False, "list_sent": False,
                     "drafts": None,
                     "error": f"{type(exc).__name__}: {exc}",
                     "events_seen": []}
            print(f"[!] ugc_drafts_once gagal: "
                  f"{type(exc).__name__}: {exc}")
    hasil = F._jsonable(hasil)
    print("")
    print("========== HASIL UGC DRAFTS (READ-ONLY) ==========")
    print(f"Login OK: {hasil.get('login_ok')}  "
          f"joined: {hasil.get('joined')}  "
          f"list sent: {hasil.get('list_sent')}")
    print(f"Drafts: {len(hasil.get('drafts') or [])}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print(f"Events terlihat: {hasil.get('events_seen')}")
    print("===================================================")
    if hasil.get("drafts") is not None:
        print("UGCDRAFTS_JSON:" + json.dumps(
            hasil["drafts"], separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
