#!/usr/bin/env python3
# DIAG edit layout UGC ke world/draft yang SUDAH ADA (tanpa create baru).
# Dipakai untuk menyalin tata letak TROPlCAL PARADlSE ke W0RLD WAR 3
# (activity 40c65049-38b7-4f67-ab82-9326e37f7b6a, terrain 4706, kosong)
# karena create draft baru SELALU ditolak server
# (DOES_NOT_POSSESS_ACTIVITY_TYPE, 6x) meski terrain sudah dibeli di
# game — edit melewati langkah create/terrain sepenuhnya.
#
# Wire (dump_3, TERBUKTI):
# - edit "activityDraft/edit" (slot 0x8900A90) -> "edit-activity-draft";
#   EditActivityDraftRequest: f1 ActivityId string, f2 Items repeated
#   XDActivityItem, f3 Rooms repeated XDActivityRoom, f4 StoreTreeId.
#   TIDAK ada field terrain — terrain hanya diset saat create.
# - Respons sukses = event "edit-activity-draft" tiba; error =
#   "edit-activity-draft-error" (f1 ActivityDraftErrorType; 3 =
#   MissingNeededItems bila server memvalidasi kepemilikan item).
# - Target boleh draft ATAU world publish (W0RLD WAR 3 publish, type
#   4706) — belum terbukti server menerima edit atas activity publish;
#   bila ditolak, fallback-nya edit salah satu draft pkxdshop
#   (Pulau XP / XP Arena, perlu activityId-nya dari draft list).
# - TIDAK kirim 'C' spawn (bukan kunjungan); TIDAK publish ulang —
#   W0RLD WAR 3 sudah publish, edit langsung mengubah world yang live.
# - Nama world TIDAK diubah kecuali --world-name diisi.
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import gift_diag as G
import ugc_copy_diag as C

BOT_USER_ID = C.BOT_USER_ID
DEFAULT_DEVICE_ID = C.DEFAULT_DEVICE_ID
DEFAULT_SOURCE_JSON = "ugc_fetch_5dcf211e.json"
DEFAULT_STORE_TREE_ID = C.DEFAULT_STORE_TREE_ID
STEP_TIMEOUT = C.STEP_TIMEOUT


def filter_source(src, include_types, rooms_mode):
    """Filter layout sumber untuk tes kepemilikan.
    include_types: "ALL" (semua item), "NONE" (tanpa item),
    atau "id1,id2,..." (hanya tipe itu).
    rooms_mode: "source" (room sumber) atau "empty" (tanpa room).
    Kembalikan (items, rooms, included_type_ids)."""
    all_items = src.get("placed_items") or []
    all_rooms = src.get("rooms") or []
    spec = (include_types or "ALL").strip().upper()
    if spec == "ALL":
        items = list(all_items)
        tids = sorted({it["id"] for it in items})
    elif spec == "NONE":
        items = []
        tids = []
    else:
        wanted = set()
        for part in spec.split(","):
            part = part.strip()
            if part:
                wanted.add(int(part))
        items = [it for it in all_items if it.get("id") in wanted]
        tids = sorted(wanted)
    rooms = list(all_rooms) if (rooms_mode or "source") == "source" else []
    return items, rooms, tids


async def ugc_edit_once(session, email, password, device_id, src,
                        target_id, store_tree_id, world_name=None,
                        include_types="ALL", rooms_mode="source"):
    items, rooms, tids = filter_source(src, include_types, rooms_mode)
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "source_activity_id": src.get("activity_id"),
             "source_terrain_type": src.get("terrain_type"),
             "target_activity_id": target_id,
             "store_tree_id": store_tree_id,
             "world_name": world_name,
             "include_types": tids,
             "items_count": len(items),
             "rooms_mode": rooms_mode,
             "steps": {}, "error": None, "events_seen": []}
    print(f"[*] Target edit: {target_id}")
    print(f"[*] Layout uji: {len(items)} item "
          f"(tipe {tids if len(tids) <= 10 else str(tids[:10]) + '...'}), "
          f"{len(rooms)} room (mode {rooms_mode}).")
    user_jwt, alloc = await L.login_and_allocate(
        email, password, device_id, session)
    if not alloc:
        hasil["error"] = "login/alokasi gagal"
        print("[!] login/alokasi gagal.")
        return hasil
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
    server, room = C.parse_gsid(
        (alloc or {}).get("gameServerId", "") or "")
    joined, via_tbugv, fr = C.wait_joined(sock, room)
    if not joined:
        hasil["error"] = "tidak JoinedRoom/tbugv sesudah login"
        print("[!] tidak JoinedRoom/tbugv sesudah login.")
        return hasil
    print("[+] JoinedRoom/tbugv diterima.")
    try:
        sock.sendall(L.build_room_event_varint(
            "init", b""))
        sock.sendall(L.build_room_event_varint(
            "Analytics/deviceId",
            (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
        print("[+] init + SendDeviceId terkirim (tanpa 'C' spawn).")
    except Exception as exc:
        print(f"[!] kirim init/SendDeviceId gagal: {exc}")

    # 1) EDIT layout ke target.
    payload = C.build_edit_payload(target_id, items, rooms,
                                   store_tree_id)
    ok, data, err = C.send_step(
        sock, fr, C.EV_EDIT, payload, STEP_TIMEOUT,
        hasil["events_seen"], "1/2 EDIT layout")
    hasil["steps"]["edit"] = {"ok": ok, "error": err}
    if not ok:
        hasil["error"] = f"EDIT gagal: {err}"
        return hasil

    # 2) Nama (opsional — bila --world-name diisi).
    if world_name:
        payload = C.build_edit_name_payload(target_id, world_name)
        ok, data, err = C.send_step(
            sock, fr, C.EV_EDIT_NAME, payload, STEP_TIMEOUT,
            hasil["events_seen"], "2/2 EDIT nama")
        hasil["steps"]["edit_name"] = {"ok": ok, "error": err}
        if not ok:
            hasil["error"] = f"EDIT nama gagal: {err}"
            return hasil
    else:
        hasil["steps"]["edit_name"] = {"ok": None,
                                       "error": "dilewati (nama dipertahankan)"}
        print("[*] EDIT nama dilewati (nama world dipertahankan).")
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("DB_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("DB_DEVICE_ID", ""))
    ap.add_argument("--target-id", required=True,
                    help="ActivityId world/draft tujuan edit")
    ap.add_argument("--source-json", default=os.getenv(
        "UGC_SOURCE_JSON", DEFAULT_SOURCE_JSON))
    ap.add_argument("--store-tree-id", default=os.getenv(
        "UGC_STORE_TREE_ID", DEFAULT_STORE_TREE_ID))
    ap.add_argument("--world-name", default=os.getenv(
        "UGC_WORLD_NAME", ""))
    ap.add_argument("--include-types", default=os.getenv(
        "UGC_INCLUDE_TYPES", "ALL"),
        help="ALL (semua item) | NONE (tanpa item) | id1,id2,...")
    ap.add_argument("--rooms", default=os.getenv(
        "UGC_ROOMS", "source"),
        help="source (room sumber) | empty (tanpa room)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        src = C.load_source(args.source_json)
    except Exception as exc:
        print(f"[!] JSON sumber tidak valid: {exc}")
        return 1
    items, rooms, tids = filter_source(
        src, args.include_types, args.rooms)
    payload = C.build_edit_payload(
        args.target_id, items, rooms, args.store_tree_id)
    print("========== RENCANA EDIT (UGC EDIT DIAG) ==========")
    print(f"Sumber: {src.get('activity_id')} "
          f"(terrain {src.get('terrain_type')}, "
          f"{len(src.get('placed_items') or [])} item, "
          f"{len(src.get('rooms') or [])} room)")
    print(f"Uji: {len(items)} item (tipe {tids}), {len(rooms)} room "
          f"(mode {args.rooms})")
    print(f"Target: {args.target_id}")
    print(f"Nama baru: {args.world_name or '(dipertahankan)'}")
    print(f"Payload EDIT ({len(payload)} B): "
          f"{payload[:60].hex()}...")
    print("===================================================")
    if args.dry_run:
        print("[*] DRY-RUN: tidak ada jaringan game, selesai.")
        return 0
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi (mode live).")
        return 1
    L.apply_panel_versions()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await ugc_edit_once(
                session, args.email, args.password, args.device_id,
                src, args.target_id, args.store_tree_id,
                args.world_name or None,
                args.include_types, args.rooms)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "source_activity_id": src.get("activity_id"),
                     "source_terrain_type": src.get("terrain_type"),
                     "target_activity_id": args.target_id,
                     "store_tree_id": args.store_tree_id,
                     "world_name": args.world_name or None,
                     "steps": {}, "events_seen": [],
                     "error": f"{type(exc).__name__}: {exc}"}
            print(f"[!] ugc_edit_once gagal: "
                  f"{type(exc).__name__}: {exc}")
    print("")
    print("========== HASIL EDIT UGC ==========")
    print(f"Target: {hasil.get('target_activity_id')}")
    for step, res in (hasil.get("steps") or {}).items():
        print(f"  {step}: ok={res.get('ok')} "
              f"error={res.get('error')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print(f"Events terlihat: {hasil.get('events_seen')}")
    print("====================================")
    print("UGCEDIT_JSON:" + json.dumps(C._jsonable(hasil),
                                       separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
