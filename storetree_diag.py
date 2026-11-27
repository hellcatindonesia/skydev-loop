"""Diag Store Tree v2 (read-only, 1 bot) — FORMAT PROTOBUF BENAR.

Revisi dari v1 (24 varian JSON/string mentah -> 0 balasan): dari dump
ditemukan pesan protobuf persisnya (namespace Subscription):

  GetStoreTreeRequest        { f1 EventId varint, f2 StoreTreeId string }
  StoreTreeEventId           { Get=0, CheckVersions=1 }
  GetStoreTreeResponse       { f1 Successful bool, f2 ErrorMessage string,
                               f3 StoreTreeData }
  StoreTreeData              { f1 Id, f2 Version, f3 ProductKey,
                               f4 repeated StoreTreeItem }
  StoreTreeItem              { f1 Kind enum(Item=0,Group=1), f2 ItemId,
                               f3 Category, f4 Name, f5 Tags, f6 SoftPrice,
                               f7 HardPrice, f8 ArcadePrice, f9 GroupId,
                               f10 SubItems, f11 EventCurrencies }
  CheckStoreTreeVersionRequest { f1 EventId=1, f2 repeated StoreIds }
  CheckStoreTreeVersionResponse{ f1 Successful, f2 ErrorMessage,
                                 f3 VersionHash }
  PetStoreTreeRequest        { f1 EventId=0 SAJA — storeVersion TIDAK
                               dikirim; server memakai versi dari sesi }
  GetPetStoreTreeResponse    { f1 Successful, f2 ErrorMessage,
                               f3 EventId, f4 PetStoreTreeData }

Event name dicoba: "L" (StoreTreeGetCommandKey), "L2"
(CheckStoreTreeIdsCommandKey), "store_tree" (RequestKey), "PST"
(respons pet), "pet_store_tree" (request key pet).

Kontrol (literal dari dump): "ugc-builder-in-game-store-1_91_0".
Kandidat versi lama: "...-1_80_0", "...-1_80_1"; kontrol Bartes:
"...-1_94_0", "...-1_94_4".

HANYA baca. Hasil: pohon terurai + STORETREE_JSON di log.
"""

import argparse
import asyncio
import json
import os
import re
import struct
import time

import lookup_uuid as L
from gift_diag import FrameReader, _is_tbugv, _is_joinedroom


def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def read_varint(buf, pos):
    out = 0
    shift = 0
    while pos < len(buf):
        b = buf[pos]
        pos += 1
        out |= (b & 0x7F) << shift
        if not (b & 0x80):
            return out, pos
        shift += 7
    return None, pos


def pb_fields(data):
    """Parse protobuf generik -> [(field, wiretype, value)]."""
    pos = 0
    out = []
    while pos < len(data):
        tag, pos = read_varint(data, pos)
        if tag is None:
            break
        f, wt = tag >> 3, tag & 7
        if f == 0:
            break
        if wt == 0:
            v, pos = read_varint(data, pos)
            if v is None:
                break
            out.append((f, 0, v))
        elif wt == 2:
            ln, pos = read_varint(data, pos)
            if ln is None or pos + ln > len(data):
                break
            out.append((f, 2, data[pos:pos + ln]))
            pos += ln
        elif wt == 5:
            if pos + 4 > len(data):
                break
            out.append((f, 5, data[pos:pos + 4]))
            pos += 4
        elif wt == 1:
            if pos + 8 > len(data):
                break
            out.append((f, 1, data[pos:pos + 8]))
            pos += 8
        else:
            break
    return out


def s(b):
    try:
        return b.decode("utf-8")
    except Exception:
        return b.decode("utf-8", "replace")


def parse_item(data, depth=0):
    it = {"kind": None, "itemId": None, "category": None, "name": None,
          "soft": None, "hard": None, "arcade": None, "groupId": None,
          "sub": []}
    for f, wt, v in pb_fields(data):
        if f == 1 and wt == 0:
            it["kind"] = v
        elif f == 2 and wt == 0:
            it["itemId"] = v
        elif f == 3 and wt == 2:
            it["category"] = s(v)
        elif f == 4 and wt == 2:
            it["name"] = s(v)
        elif f == 6 and wt == 0:
            it["soft"] = v
        elif f == 7 and wt == 0:
            it["hard"] = v
        elif f == 8 and wt == 0:
            it["arcade"] = v
        elif f == 9 and wt == 2:
            it["groupId"] = s(v)
        elif f == 10 and wt == 2 and depth < 3:
            it["sub"].append(parse_item(v, depth + 1))
    return it


def parse_tree_data(data):
    td = {"id": None, "version": None, "productKey": None, "items": []}
    for f, wt, v in pb_fields(data):
        if f == 1 and wt == 2:
            td["id"] = s(v)
        elif f == 2 and wt == 2:
            td["version"] = s(v)
        elif f == 3 and wt == 2:
            td["productKey"] = s(v)
        elif f == 4 and wt == 2:
            td["items"].append(parse_item(v))
    return td


def try_parse_response(data, wanted_ids):
    """Coba urai sebagai salah satu respons store-tree. Kembalikan dict
    atau None. Guard: id pohon harus cocok dengan yang diminta."""
    fields = pb_fields(data)
    if not fields:
        return None
    f1 = [v for (f, wt, v) in fields if f == 1 and wt == 0]
    if not f1 or f1[0] not in (0, 1):
        return None
    ok = bool(f1[0])
    # GetStoreTreeResponse: f3 = StoreTreeData
    for (f, wt, v) in fields:
        if f == 3 and wt == 2:
            td = parse_tree_data(v)
            if td["id"] and any(w in td["id"] for w in wanted_ids):
                err = next((s(v2) for (f2, wt2, v2) in fields
                            if f2 == 2 and wt2 == 2), "")
                return {"kind": "GetStoreTreeResponse", "successful": ok,
                        "error": err, "tree": td}
            if td["id"] and "store" in td["id"]:
                err = next((s(v2) for (f2, wt2, v2) in fields
                            if f2 == 2 and wt2 == 2), "")
                return {"kind": "GetStoreTreeResponse?", "successful": ok,
                        "error": err, "tree": td}
    # CheckStoreTreeVersionResponse: f3 = VersionHash string
    for (f, wt, v) in fields:
        if f == 3 and wt == 2 and len(v) >= 6:
            err = next((s(v2) for (f2, wt2, v2) in fields
                        if f2 == 2 and wt2 == 2), "")
            return {"kind": "CheckStoreTreeVersionResponse",
                    "successful": ok, "error": err,
                    "versionHash": s(v)}
    # GetPetStoreTreeResponse: f4 = PetStoreTreeData (urai generik)
    for (f, wt, v) in fields:
        if f == 4 and wt == 2:
            strs = [s(v2) for (f2, wt2, v2) in pb_fields(v)
                    if wt2 == 2 and len(v2) >= 2]
            if strs:
                err = next((s(v2) for (f2, wt2, v2) in fields
                            if f2 == 2 and wt2 == 2), "")
                return {"kind": "GetPetStoreTreeResponse?", "successful": ok,
                        "error": err, "strings": strs[:10]}
    return None


def decode_frame(frame):
    if not frame:
        return None, b"", None
    t = frame[0]
    if t == 0x08 and len(frame) >= 2:
        ln, n = read_varint(frame, 1)
        if ln is not None and 1 + n + ln <= len(frame):
            try:
                name = frame[1 + n:1 + n + ln].decode("utf-8")
            except Exception:
                name = None
            return name, frame[1 + n + ln:], t
    return None, frame, t


def preview(data, n=300):
    return s(data).replace("\n", "\\n").replace("\r", "\\r")[:n]


def req_get(tree_id):
    tb = tree_id.encode("utf-8")
    return b"\x08\x00\x12" + varint(len(tb)) + tb


def req_check(ids):
    out = bytearray(b"\x08\x01")
    for tid in ids:
        tb = tid.encode("utf-8")
        out += b"\x12" + varint(len(tb)) + tb
    return bytes(out)


def build_variants(tree_ids):
    variants = []
    for tid in tree_ids:
        variants.append((f"GET L {tid}", "L", req_get(tid)))
        variants.append((f"GET store_tree {tid}", "store_tree",
                         req_get(tid)))
    variants.append((f"CHECK L2 {len(tree_ids)} ids", "L2",
                     req_check(tree_ids)))
    variants.append((f"CHECK L {len(tree_ids)} ids", "L",
                     req_check(tree_ids)))
    variants.append((f"CHECK store_tree {len(tree_ids)} ids", "store_tree",
                     req_check(tree_ids)))
    variants.append(("PET pet_store_tree", "pet_store_tree", b"\x08\x00"))
    variants.append(("PET PST", "PST", b"\x08\x00"))
    return variants


async def probe_once(session, email, password, device_id,
                     tree_ids, wait_seconds):
    hasil = {"email": email, "login_ok": False, "variants": [], "hits": []}
    user_jwt, alloc = await L.login_and_allocate(
        email, password, device_id, session)
    if not alloc:
        print("[!] login/alokasi gagal.")
        return hasil
    host, port = L.find_host_port(alloc)
    if not host or not port:
        print("[!] host/port tidak ketemu di allocation.")
        return hasil
    print(f"[*] alokasi OK: {host}:{port}")
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        print("[!] TCP connect gagal.")
        return hasil
    hasil["login_ok"] = True
    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        print(f"[!] kirim login frame gagal: {exc}")
        return hasil
    fr = FrameReader(sock)
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: _is_tbugv(f) or _is_joinedroom(f), 3)
    if closed:
        print("[!] koneksi tertutup saat tunggu login.")
        return hasil
    print("[*] jendela login lewat — mulai kirim.")

    wanted = ["ugc-builder", "store_tree", "pet_store"]
    variants = build_variants(tree_ids)
    for label, ev_name, payload in variants:
        rec = {"label": label, "event": ev_name,
               "sent_hex": payload.hex(), "frames": 0, "hits": 0}
        try:
            sock.sendall(L.build_room_event_varint(ev_name, payload))
        except Exception as exc:
            print(f"[!] kirim {label} gagal: {exc}")
            rec["error"] = str(exc)
            hasil["variants"].append(rec)
            continue
        print(f"[*] kirim {label} ({len(payload)} B) — drain {wait_seconds} dtk")
        for f in fr.drain(wait_seconds):
            name, data, t = decode_frame(f)
            rec["frames"] += 1
            parsed = try_parse_response(data, wanted)
            if parsed is not None:
                rec["hits"] += 1
                hit = {"variant": label, "event": name, "parsed": parsed}
                hasil["hits"].append(hit)
                print(f"    [!!] HIT ev={name!r}: "
                      f"{json.dumps(parsed, ensure_ascii=False)[:600]}")
            elif name in ("L", "L2", "PST", "store_tree", "pet_store_tree"):
                print(f"    [?] frame bernama ev={name!r} (bukan respons "
                      f"pohon): {preview(data, 120)}")
        hasil["variants"].append(rec)
        print(f"    -> {rec['frames']} frame, hit: {rec['hits']}")

    print("[*] drain akhir 10 dtk...")
    for f in fr.drain(10):
        name, data, t = decode_frame(f)
        parsed = try_parse_response(data, wanted)
        if parsed is not None:
            hasil["hits"].append({"variant": "straggler", "event": name,
                                 "parsed": parsed})
            print(f"    [!!] straggler HIT: "
                  f"{json.dumps(parsed, ensure_ascii=False)[:600]}")
    try:
        sock.close()
    except Exception:
        pass
    return hasil


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("STORETREE_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("STORETREE_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("STORETREE_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--tree-ids",
                    default=os.getenv("STORETREE_TREE_IDS",
                                      "ugc-builder-in-game-store-1_91_0"))
    ap.add_argument("--pet-versions",
                    default=os.getenv("STORETREE_PET_VERSIONS", ""))
    ap.add_argument("--wait", type=int,
                    default=int(os.getenv("STORETREE_WAIT", "6")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    tree_ids = [t.strip() for t in args.tree_ids.split(",") if t.strip()]
    print(f"[*] Diag Store Tree v2 — bot {args.email} | tree_ids={tree_ids} "
          f"| tunggu {args.wait} dtk/varian")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await probe_once(
            session, args.email, args.password, args.device_id,
            tree_ids, args.wait)
    print("=" * 56)
    print("HASIL STORE TREE DIAG v2")
    print(f"  Login OK: {hasil['login_ok']}")
    print(f"  Varian dicoba: {len(hasil['variants'])}")
    for v in hasil["variants"]:
        print(f"    - {v['label']}: {v['frames']} frame, hit={v['hits']}")
    print(f"  HIT total: {len(hasil['hits'])}")
    for h in hasil["hits"]:
        p = h["parsed"]
        if "tree" in p:
            td = p["tree"]
            print(f"    - [{h['variant']}] {p['kind']} ok={p['successful']} "
                  f"err={p['error']!r} id={td['id']!r} "
                  f"version={td['version']!r} product={td['productKey']!r} "
                  f"items={len(td['items'])}")
            for it in td["items"][:8]:
                print(f"        item kind={it['kind']} id={it['itemId']} "
                      f"cat={it['category']!r} name={it['name']!r} "
                      f"soft={it['soft']} hard={it['hard']} "
                      f"sub={len(it['sub'])}")
        else:
            print(f"    - [{h['variant']}] {p}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("STORETREE_JSON:" + json.dumps(hasil, ensure_ascii=False)[:30000])
    print("=" * 56)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
