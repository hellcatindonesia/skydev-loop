# -*- coding: utf-8 -*-
"""
ugc_copy_diag.py — Copy builder UGC antar akun (LANGKAH 2), 1 bot tes
(pkxdshop.id@gmail.com) sebagai akun TARGET. Membuat draft BARU di
akun target, mengisi dengan TerrainData dari JSON sumber (hasil
langkah 1), lalu publish. World sumber = milik Bartes sendiri di
akun satunya; target = akun pkxdshop Bartes.

Wire format (TERBUKTI via disassembly libil2cpp.so LAMA + dump lama,
2026-11-18 — bukan tebakan):
- Mekanik ToxicBirdUgcDraftMechanic (TypeDefIndex 7945). Command key
  dibuktikan .rela.dyn (slot -> stringliteral.json):
  create   "activityDraft/create"   (slot 0x8900A40)
  edit     "activityDraft/edit"     (slot 0x8900A90)
  editName "activityDraft/editName" (slot 0x8900A98)
  editTags "activityDraft/editTags" (slot 0x8900AB0)
  publish  "activityDraft/publish"  (slot 0x8900AB8)
  get      "activityDraft/get"      (slot 0x8900A58)
  list     "activityDraft/list"     (slot 0x8900A18)
  Respons sukses (didaftarkan di Install, slot 0x89009xx):
  "create-activity-draft", "edit-activity-draft",
  "edit-name-activity-draft", "edit-tags-activity-draft",
  "publish-activity-draft", "get-activity-draft",
  "list-activity-drafts"; error = nama sama + "-error".
- Payload = protobuf mentah (MessageExtensions.ToByteArray):
  CreateActivityDraftRequest: f1 ActivityType int32.
    ActivityType = ID item terrain — DIBUKTIKAN:
    UgcHelper.CreateActivityDraft(int itemId) -> state machine
    d__24 field itemId @0x28, MoveNext ldr w22,[x19,#0x28] lalu
    mov w1,w22 -> UgcService.CreateActivityDraft(activityType).
    Jadi untuk copy, ActivityType = terrain_type JSON sumber (4706).
  EditActivityDraftRequest: f1 ActivityId string, f2 Items repeated
    ActivityItem, f3 Rooms repeated ActivityRoom, f4 StoreTreeId
    string. TIDAK ada field terrain — terrain hanya diset saat create.
    StoreTreeId default "ugc-builder-in-game-store-1_91_0"
    (StoreTreeConstants.UGC_CONTEXT_STORE_NAME — SAMA di dump lama
    dan dump_3; store tree builder di dalam game). Bila server
    menolak, coba "" sebagai fallback di tes live berikutnya.
  EditActivityNameRequest: f1 ActivityId, f2 ActivityName.
  EditActivityTagsRequest: f1 ActivityId, f2 Tags repeated
    ActivityTag {f1 TagType string, f2 TagId string}.
  PublishActivityDraftRequest: f1 ActivityId.
  CreateActivityDraftResponse: f1 ActivityId string (draft baru).
  PublishActivityDraftResponse: tanpa field (event tiba = sukses).
- ActivityItem (FieldNumber dump lama = identik dgn bukti live
  langkah 1): f1 Id int32, f2 Position Vector3, f3 Rotation
  Vector3, f4 ColorId int32, f5 EquippedItems repeated int32.
  ActivityRoom: f1 Floor int32, f2 Walls repeated int32.
  Vector3: f1 X, f2 Y, f3 Z float fixed32.
- Encode mengikuti proto3 (Google.Protobuf): varint int32 negatif
  = 10 byte sign-extend 64-bit (ColorId -1); repeated int32 =
  PACKED (wire 2); field default (0 / "" / kosong) dihilangkan,
  kecuali message field yang selalu ditulis bila diset.

Alur live: login (default allocation, room biasa — draft TIDAK
butuh masuk world UGC) -> TCP connect -> login frame -> tunggu
JoinedRoom/tbugv -> init events + SendDeviceId (tanpa 'C' spawn,
supaya tidak tercatat sebagai kunjungan) -> 5 langkah berurutan,
BERHENTI di kegagalan pertama (jangan retry write):
  1. create  -> tunggu "create-activity-draft" (20 dtk) -> draft id
  2. edit    -> tunggu "edit-activity-draft" (20 dtk)
  3. editName-> tunggu "edit-name-activity-draft" (20 dtk)
  4. editTags-> tunggu "edit-tags-activity-draft" (20 dtk)
  5. publish -> tunggu "publish-activity-draft" (20 dtk)
Mode --dry-run: hanya validasi JSON + bangun & cetak semua payload
(hex) tanpa jaringan game sama sekali (tanpa login).

Peringatan live: pertanyaan penentu — apakah server memvalidasi
kepemilikan item di akun target — hanya bisa dijawab tes live ini.
Bila edit/publish ditolak, log error mentah (hex + dump generik)
akan menunjukkan alasannya.
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
DEFAULT_SOURCE_JSON = "ugc_fetch_0d2522bd.json"
DEFAULT_STORE_TREE_ID = "ugc-builder-in-game-store-1_91_0"
DEFAULT_WORLD_NAME = "Salinan"
MIN_NAME_LENGTH = 4
MAX_FRAME = 8 * 1024 * 1024
STEP_TIMEOUT = 20

# (command key, success event, error event)
EV_CREATE = ("activityDraft/create", "create-activity-draft",
             "create-activity-draft-error")
EV_EDIT = ("activityDraft/edit", "edit-activity-draft",
           "edit-activity-draft-error")
EV_EDIT_NAME = ("activityDraft/editName", "edit-name-activity-draft",
                "edit-name-activity-draft-error")
EV_EDIT_TAGS = ("activityDraft/editTags", "edit-tags-activity-draft",
                "edit-tags-activity-draft-error")
EV_PUBLISH = ("activityDraft/publish", "publish-activity-draft",
              "publish-activity-draft-error")


# ---------------------------------------------------------------- protobuf
def _varint(buf, i):
    """Varint sampai 64-bit (10 byte). L.read_varint berhenti di
    shift 35 (maks 5 byte) sehingga varint 64-bit (mis. nilai
    negatif int32 yang di-sign-extend) akan dikira terpotong."""
    val = 0
    shift = 0
    n = len(buf)
    start = i
    while i < n and shift < 70:
        b = buf[i]
        val |= (b & 0x7F) << shift
        i += 1
        if not (b & 0x80):
            return val, i - start
        shift += 7
    return None, 0


def _read_varint(buf, i):
    v, m = _varint(buf, i)
    if v is None:
        raise ValueError("varint terpotong")
    return v, m


def _parse_fields(buf):
    """Parse protobuf generik -> {field: [(wire, value), ...]}."""
    out = {}
    i = 0
    n = len(buf)
    while i < n:
        tag, m = _read_varint(buf, i)
        i += m
        field = tag >> 3
        wire = tag & 7
        if wire == 0:
            v, m = _read_varint(buf, i)
            i += m
            out.setdefault(field, []).append((0, v))
        elif wire == 2:
            ln, m = _read_varint(buf, i)
            i += m
            chunk = bytes(buf[i:i + ln])
            i += ln
            out.setdefault(field, []).append((2, chunk))
        elif wire == 5:
            if i + 4 > n:
                raise ValueError("fixed32 terpotong")
            out.setdefault(field, []).append(
                (5, struct.unpack("<f", buf[i:i + 4])[0]))
            i += 4
        elif wire == 1:
            if i + 8 > n:
                raise ValueError("fixed64 terpotong")
            out.setdefault(field, []).append(
                (1, struct.unpack("<d", buf[i:i + 8])[0]))
            i += 8
        else:
            raise ValueError(f"wire type {wire} tidak dikenal")
    return out


def _first(fields, field, wire=None):
    for w, v in fields.get(field, []):
        if wire is None or w == wire:
            return v
    return None


def _str_field(fields, field):
    v = _first(fields, field, 2)
    if v is None:
        return None
    return v.decode("utf-8", "replace")


def _dump_generic(buf, depth=0):
    """Dump struktur protobuf generik untuk diagnosis."""
    try:
        f = _parse_fields(buf)
    except Exception as exc:
        return {"parse_error": repr(exc),
                "hex_head": bytes(buf[:64]).hex()}
    out = {}
    for field, vals in sorted(f.items()):
        shown = []
        for w, v in vals:
            if w == 2:
                try:
                    s = bytes(v).decode("utf-8")
                    if s and all(32 <= ord(c) < 127 for c in s):
                        shown.append(s)
                        continue
                except Exception:
                    pass
                if depth < 2 and len(v) >= 2:
                    shown.append(_dump_generic(v, depth + 1))
                else:
                    shown.append({"bytes_len": len(v),
                                  "hex_head": bytes(v[:32]).hex()})
            else:
                shown.append(v)
        out[str(field)] = shown if len(shown) > 1 else shown[0]
    return out


def _jsonable(v):
    if isinstance(v, dict):
        return {str(k): _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).hex()
    return v


# ------------------------------------------------------------ encode (kirim)
def enc_varint(value):
    """Varint unsigned (nilai negatif int32 -> & 0xFFFFFFFFFFFFFFFF,
    jadi 10 byte — sama seperti Google.Protobuf)."""
    v = value & 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


def enc_tag(field, wire):
    return enc_varint((field << 3) | wire)


def enc_varint_field(field, value):
    if value == 0:
        return b""  # proto3: default dihilangkan
    return enc_tag(field, 0) + enc_varint(value)


def enc_string_field(field, value):
    if not value:
        return b""
    raw = value.encode("utf-8")
    return enc_tag(field, 2) + enc_varint(len(raw)) + raw


def enc_bytes_field(field, raw):
    if raw is None:
        return b""
    return enc_tag(field, 2) + enc_varint(len(raw)) + raw


def enc_packed_int_field(field, values):
    """Repeated int32 packed (wire 2). Dihilangkan bila kosong."""
    if not values:
        return b""
    body = b"".join(enc_varint(v) for v in values)
    return enc_tag(field, 2) + enc_varint(len(body)) + body


def enc_vector3(vec):
    """Vector3: f1 X, f2 Y, f3 Z fixed32; komponen 0.0 dihilangkan."""
    out = bytearray()
    for field, key in ((1, "x"), (2, "y"), (3, "z")):
        v = (vec or {}).get(key, 0.0)
        if v != 0.0:
            out += enc_tag(field, 5) + struct.pack("<f", float(v))
    return bytes(out)


def enc_activity_item(item):
    out = bytearray()
    out += enc_varint_field(1, item.get("id", 0))
    pos = item.get("position")
    if pos is not None:
        out += enc_bytes_field(2, enc_vector3(pos))
    rot = item.get("rotation")
    if rot is not None:
        out += enc_bytes_field(3, enc_vector3(rot))
    if item.get("color_id", 0) != 0:
        out += enc_tag(4, 0) + enc_varint(item["color_id"])
    out += enc_packed_int_field(5, item.get("equipped_items") or [])
    return bytes(out)


def enc_activity_room(room):
    out = bytearray()
    out += enc_varint_field(1, room.get("floor", 0))
    out += enc_packed_int_field(2, room.get("walls") or [])
    return bytes(out)


def build_create_payload(activity_type):
    """CreateActivityDraftRequest: f1 ActivityType int32."""
    return enc_varint_field(1, activity_type)


def build_edit_payload(activity_id, items, rooms, store_tree_id):
    """EditActivityDraftRequest: f1 ActivityId, f2 Items, f3 Rooms,
    f4 StoreTreeId."""
    out = bytearray()
    out += enc_string_field(1, activity_id)
    for it in items:
        out += enc_bytes_field(2, enc_activity_item(it))
    for rm in rooms:
        out += enc_bytes_field(3, enc_activity_room(rm))
    out += enc_string_field(4, store_tree_id)
    return bytes(out)


def build_edit_name_payload(activity_id, name):
    """EditActivityNameRequest: f1 ActivityId, f2 ActivityName."""
    out = bytearray()
    out += enc_string_field(1, activity_id)
    out += enc_string_field(2, name)
    return bytes(out)


def build_edit_tags_payload(activity_id, tags):
    """EditActivityTagsRequest: f1 ActivityId, f2 Tags repeated
    ActivityTag {f1 TagType string, f2 TagId string}."""
    out = bytearray()
    out += enc_string_field(1, activity_id)
    for tag_type, tag_id in tags:
        msg = bytearray()
        msg += enc_string_field(1, tag_type)
        msg += enc_string_field(2, tag_id)
        out += enc_bytes_field(2, bytes(msg))
    return bytes(out)


def build_publish_payload(activity_id):
    """PublishActivityDraftRequest: f1 ActivityId."""
    return enc_string_field(1, activity_id)


def parse_create_response(data):
    """CreateActivityDraftResponse: f1 ActivityId string."""
    f = _parse_fields(data)
    return _str_field(f, 1)


# ------------------------------------------------------------------- frame
class BigFrameReader(G.FrameReader):
    """FrameReader yang sama, tapi batas frame 8 MB."""

    def _pump(self):
        while len(self.buf) >= 4:
            ln = struct.unpack(">I", self.buf[:4])[0]
            if ln > MAX_FRAME:
                return None
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
            return fr
        return None


def parse_gsid(gsid):
    parts = (gsid or "").split("::")
    server = parts[1] if len(parts) >= 2 else ""
    room = None
    if len(parts) >= 3:
        try:
            room = int(parts[2])
        except Exception:
            room = None
    return server, room


def wait_joined(sock, rm):
    """Tunggu JoinedRoom (standalone 0x06 0x01 / batch 0x0d) atau
    tbugv, maks ~5 dtk. Kembalikan (joined_bool, tbugv_bool, fr)."""
    fr = BigFrameReader(sock)
    needle = (bytes([0x06, 0x01]) + rm.to_bytes(4, "big")
              if rm is not None else b"")

    def _is_join_or_tbugv(f):
        if G._is_tbugv(f):
            return True
        if rm is not None and len(f) >= 6 and f[0] == 0x06 and f[1] == 1 \
                and int.from_bytes(f[2:6], "big") == rm:
            return True
        if rm is not None and f and f[0] == 0x0d and needle in bytes(f):
            return True
        return False

    got, closed = fr.read_until(_is_join_or_tbugv, 3)
    if closed:
        return False, False, fr
    if got is not None and G._is_tbugv(got):
        time.sleep(1)
        return True, True, fr
    if got is not None:
        return True, False, fr
    if rm is not None:
        for f in fr.drain(2):
            raw = bytes(f)
            if (len(raw) >= 6 and raw[0] == 0x06 and raw[1] == 1
                    and int.from_bytes(raw[2:6], "big") == rm) \
                    or (raw and raw[0] == 0x0d and needle in raw):
                return True, False, fr
    return False, False, fr


def wait_for_event(fr, ok_names, err_names, timeout_s, events_seen):
    """Tunggu salah satu event (sukses/error) maks timeout_s.
    Kembalikan (kind, name, data): kind 'ok' / 'err' / None."""
    end = time.time() + timeout_s
    while time.time() < end:
        for f in fr.drain(0.5):
            ev_name, d = G.decode_room_event(f)
            if ev_name is None:
                continue
            events_seen.append(ev_name)
            print(f"[.] event lewat: '{ev_name}' "
                  f"({len(d) if d else 0} B)")
            if ev_name in ok_names:
                return "ok", ev_name, d
            if ev_name in err_names:
                return "err", ev_name, d
    return None, None, None


def send_step(sock, fr, ev, payload, timeout_s, events_seen, label):
    """Kirim satu langkah draft, tunggu respons sukses/error.
    Kembalikan (ok_bool, data, error_text)."""
    cmd, ok_name, err_name = ev
    print(f"[*] {label}: kirim '{cmd}' ({len(payload)} B)...")
    try:
        sock.sendall(L.build_room_event_varint(cmd, payload))
    except Exception as exc:
        return False, None, f"kirim '{cmd}' gagal: {exc}"
    kind, name, data = wait_for_event(
        fr, {ok_name}, {err_name}, timeout_s, events_seen)
    if kind == "ok":
        print(f"[+] {label}: '{name}' diterima "
              f"({len(data) if data else 0} B).")
        if data:
            print(f"    payload hex: {bytes(data).hex()}")
        return True, data, None
    if kind == "err":
        dump = _dump_generic(data) if data else {}
        err = (f"server balas '{name}' ({len(data) if data else 0} B): "
               f"{bytes(data).hex() if data else ''} "
               f"dump={json.dumps(_jsonable(dump))}")
        print(f"[!] {label}: {err}")
        return False, data, err
    err = f"tidak ada respons '{ok_name}' dalam {timeout_s} dtk"
    print(f"[!] {label}: {err}")
    return False, None, err


# ------------------------------------------------------------------ validasi
def load_source(path):
    with open(path, encoding="utf-8") as fh:
        src = json.load(fh)
    if not isinstance(src, dict):
        raise ValueError("JSON sumber bukan object")
    terrain_type = src.get("terrain_type")
    if not isinstance(terrain_type, int) or terrain_type <= 0:
        raise ValueError("terrain_type JSON sumber tidak valid "
                         f"(dapat: {terrain_type!r})")
    items = src.get("placed_items")
    rooms = src.get("rooms")
    if not isinstance(items, list) or not isinstance(rooms, list):
        raise ValueError("placed_items/rooms JSON sumber tidak valid")
    for it in items:
        if not isinstance(it, dict) or not isinstance(it.get("id"), int):
            raise ValueError(f"item tidak valid: {it!r}")
        for key in ("position", "rotation"):
            v = it.get(key)
            if v is not None:
                for axis in ("x", "y", "z"):
                    if not isinstance(v.get(axis), (int, float)):
                        raise ValueError(
                            f"item {it.get('id')} {key}.{axis} "
                            f"tidak valid: {v!r}")
        if it.get("color_id") is not None and not isinstance(
                it.get("color_id"), int):
            raise ValueError(f"item {it.get('id')} color_id tidak valid")
    for rm in rooms:
        if not isinstance(rm, dict) or not isinstance(rm.get("floor"), int):
            raise ValueError(f"room tidak valid: {rm!r}")
    return src


def parse_tags(raw):
    """'type:id,type:id' -> [(type, id), ...]. Kosong -> []."""
    tags = []
    if not raw:
        return tags
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            t, i = part.split(":", 1)
            tags.append((t.strip(), i.strip()))
        else:
            tags.append(("", part))
    return tags


def build_plan(src, world_name, tags, store_tree_id, activity_type):
    """Bangun semua payload (tanpa jaringan). Kembalikan dict plan."""
    if len(world_name) < MIN_NAME_LENGTH:
        raise ValueError(
            f"nama world minimal {MIN_NAME_LENGTH} karakter "
            f"(dapat: {world_name!r})")
    atype = activity_type or src["terrain_type"]
    items = src.get("placed_items") or []
    rooms = src.get("rooms") or []
    plan = {
        "activity_type": atype,
        "terrain_type_source": src["terrain_type"],
        "world_name": world_name,
        "tags": [[t, i] for t, i in tags],
        "store_tree_id": store_tree_id,
        "items_count": len(items),
        "rooms_count": len(rooms),
        "payloads": {
            "create": build_create_payload(atype).hex(),
            "edit": None,  # butuh draft id — diisi sesudah create
            "edit_name": None,
            "edit_tags": None,
            "publish": None,
        },
    }
    return plan


# --------------------------------------------------------------------- live
async def ugc_copy_once(session, email, password, device_id, src,
                        world_name, tags, store_tree_id, activity_type):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "source_activity_id": src.get("activity_id"),
             "source_terrain_type": src.get("terrain_type"),
             "world_name": world_name,
             "store_tree_id": store_tree_id,
             "draft_id": None,
             "steps": {}, "error": None, "events_seen": []}
    plan = build_plan(src, world_name, tags, store_tree_id, activity_type)
    items = src.get("placed_items") or []
    rooms = src.get("rooms") or []
    user_jwt, _alloc = await L.login_and_allocate(
        email, password, device_id, session)
    if not user_jwt:
        hasil["error"] = "login/alokasi awal gagal"
        print("[!] login/alokasi awal gagal.")
        return hasil
    print("[+] login + alokasi OK.")
    host, port = L.find_host_port(_alloc)
    if not host or not port:
        hasil["error"] = "host/port tidak ketemu di allocation"
        print("[!] host/port tidak ketemu di allocation.")
        return hasil
    srv, rm = parse_gsid(_alloc.get("gameServerId", ""))
    print(f"[+] server={srv} room={rm} host={host}:{port}")
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
    try:
        print("[*] tunggu JoinedRoom/tbugv (maks ~5 dtk)...")
        joined, via_tbugv, fr = wait_joined(sock, rm)
        if not joined:
            hasil["error"] = "JoinedRoom/tbugv tidak terkonfirmasi"
            print("[!] JoinedRoom/tbugv tidak terkonfirmasi.")
            return hasil
        print(f"[+] joined (via_tbugv={via_tbugv}).")
        try:
            for name in ["init_pos", "init_day_night", "preset",
                         "fetch_events"]:
                sock.sendall(L.build_room_event_varint(name, b""))
            time.sleep(0.3)
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId",
                (device_id or DEFAULT_DEVICE_ID).encode("utf-8")))
            print("[+] init + SendDeviceId terkirim (tanpa 'C' spawn).")
        except Exception as exc:
            hasil["error"] = f"kirim init gagal: {exc}"
            print(f"[!] kirim init gagal: {exc}")
            return hasil

        # Langkah 1 — CREATE draft.
        ok, data, err = send_step(
            sock, fr, EV_CREATE,
            build_create_payload(plan["activity_type"]),
            STEP_TIMEOUT, hasil["events_seen"],
            "1/5 CREATE draft")
        hasil["steps"]["create"] = {"ok": ok, "error": err}
        if not ok:
            hasil["error"] = f"CREATE gagal: {err}"
            return hasil
        try:
            draft_id = parse_create_response(data)
        except Exception as exc:
            hasil["error"] = (f"parse respons CREATE gagal: {exc!r} "
                              f"(hex: {bytes(data).hex() if data else ''})")
            print(f"[!] {hasil['error']}")
            return hasil
        if not draft_id:
            hasil["error"] = ("respons CREATE tanpa ActivityId "
                              f"(hex: {bytes(data).hex() if data else ''})")
            print(f"[!] {hasil['error']}")
            return hasil
        hasil["draft_id"] = draft_id
        print(f"[+] DRAFT BARU: {draft_id}")

        # Langkah 2 — EDIT terrain (items + rooms + storeTreeId).
        ok, data, err = send_step(
            sock, fr, EV_EDIT,
            build_edit_payload(draft_id, items, rooms, store_tree_id),
            STEP_TIMEOUT, hasil["events_seen"],
            "2/5 EDIT terrain")
        hasil["steps"]["edit"] = {"ok": ok, "error": err}
        if not ok:
            hasil["error"] = f"EDIT terrain gagal: {err}"
            return hasil

        # Langkah 3 — EDIT nama.
        ok, data, err = send_step(
            sock, fr, EV_EDIT_NAME,
            build_edit_name_payload(draft_id, world_name),
            STEP_TIMEOUT, hasil["events_seen"],
            "3/5 EDIT nama")
        hasil["steps"]["edit_name"] = {"ok": ok, "error": err}
        if not ok:
            hasil["error"] = f"EDIT nama gagal: {err}"
            return hasil

        # Langkah 4 — EDIT tags (selalu dikirim; kosong = tanpa tag).
        ok, data, err = send_step(
            sock, fr, EV_EDIT_TAGS,
            build_edit_tags_payload(draft_id, tags),
            STEP_TIMEOUT, hasil["events_seen"],
            "4/5 EDIT tags")
        hasil["steps"]["edit_tags"] = {"ok": ok, "error": err}
        if not ok:
            hasil["error"] = f"EDIT tags gagal: {err}"
            return hasil

        # Langkah 5 — PUBLISH.
        ok, data, err = send_step(
            sock, fr, EV_PUBLISH,
            build_publish_payload(draft_id),
            STEP_TIMEOUT, hasil["events_seen"],
            "5/5 PUBLISH")
        hasil["steps"]["publish"] = {"ok": ok, "error": err}
        if not ok:
            hasil["error"] = f"PUBLISH gagal: {err}"
            return hasil
        print(f"[+] WORLD TERBIT di akun target, draft {draft_id}.")
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("DB_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("DB_DEVICE_ID", ""))
    ap.add_argument("--source-json", default=os.getenv(
        "DB_SOURCE_JSON", DEFAULT_SOURCE_JSON))
    ap.add_argument("--world-name", default=os.getenv(
        "DB_WORLD_NAME", DEFAULT_WORLD_NAME))
    ap.add_argument("--tags", default=os.getenv("DB_TAGS", ""))
    ap.add_argument("--store-tree-id", default=os.getenv(
        "DB_STORE_TREE_ID", DEFAULT_STORE_TREE_ID))
    ap.add_argument("--activity-type", type=int, default=int(
        os.getenv("DB_ACTIVITY_TYPE", "0") or 0))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    try:
        src = load_source(args.source_json)
    except Exception as exc:
        print(f"[!] JSON sumber tidak bisa dibaca: {exc}")
        return 1
    try:
        tags = parse_tags(args.tags)
        plan = build_plan(src, args.world_name, tags,
                          args.store_tree_id, args.activity_type)
    except Exception as exc:
        print(f"[!] validasi gagal: {exc}")
        return 1

    print("========== RENCANA COPY (UGC COPY) ==========")
    print(f"Sumber activity: {src.get('activity_id')} "
          f"(owner {src.get('owner_id')})")
    print(f"TerrainType sumber: {src.get('terrain_type')} -> "
          f"ActivityType create: {plan['activity_type']}")
    print(f"Items: {plan['items_count']}  Rooms: {plan['rooms_count']}")
    print(f"Nama baru: {plan['world_name']!r}  Tags: {plan['tags']}")
    print(f"StoreTreeId: {plan['store_tree_id']!r}")
    print(f"Payload CREATE ({len(bytes.fromhex(plan['payloads']['create']))} B): "
          f"{plan['payloads']['create']}")
    # Contoh payload EDIT memakai draft id placeholder agar bentuknya
    # bisa diperiksa tanpa login (id asli baru ada sesudah create).
    demo_id = "00000000-0000-0000-0000-000000000000"
    demo_edit = build_edit_payload(
        demo_id, src.get("placed_items") or [], src.get("rooms") or [],
        args.store_tree_id)
    print(f"Payload EDIT contoh ({len(demo_edit)} B, draft id placeholder): "
          f"{demo_edit.hex()}")
    print(f"Payload EDIT_NAME contoh: "
          f"{build_edit_name_payload(demo_id, args.world_name).hex()}")
    print(f"Payload EDIT_TAGS contoh: "
          f"{build_edit_tags_payload(demo_id, tags).hex()}")
    print(f"Payload PUBLISH contoh: "
          f"{build_publish_payload(demo_id).hex()}")
    print("=============================================")
    if args.dry_run:
        print("[*] DRY-RUN: tidak ada jaringan game, selesai.")
        print("UGCCOPY_JSON:" + json.dumps(
            {"dry_run": True, "plan": plan}, separators=(",", ":")))
        return 0

    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi (mode live).")
        return 1
    L.apply_panel_versions()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        try:
            hasil = await ugc_copy_once(
                session, args.email, args.password, args.device_id,
                src, args.world_name, tags, args.store_tree_id,
                args.activity_type)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            hasil = {"email": args.email, "bot_user_id": BOT_USER_ID,
                     "source_activity_id": src.get("activity_id"),
                     "source_terrain_type": src.get("terrain_type"),
                     "world_name": args.world_name,
                     "store_tree_id": args.store_tree_id,
                     "draft_id": None, "steps": {},
                     "error": f"{type(exc).__name__}: {exc}",
                     "events_seen": []}
            print(f"[!] ugc_copy_once gagal: {type(exc).__name__}: {exc}")
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL UGC COPY ==========")
    print(f"Draft baru: {hasil.get('draft_id')}")
    for step, info in (hasil.get("steps") or {}).items():
        print(f"  {step}: ok={info.get('ok')} "
              f"error={info.get('error')}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print(f"Events terlihat: {hasil.get('events_seen')}")
    print("====================================")
    print("UGCCOPY_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
