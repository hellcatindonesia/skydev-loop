"""Diag Store Tree (read-only, 1 bot).

Tujuan: membuktikan jalur pengambilan katalog toko LEWAT TCP game server
(mechanic ToxicBirdStoreTreeMechanic / ToxicBirdPetStoreTreeMechanic),
bukan lewat HTTPS game-store-data (403 anonim).

Yang dikirim (semua HANYA baca, tidak mengubah apa pun):
  - RoomEvent "L"  (StoreTreeGetCommandKey)  -> GetStoreTree(storeTreeId)
  - RoomEvent "L2" (CheckStoreTreeIdsCommandKey) -> CheckAllStoreTreeIds(ids)
  - RoomEvent "pet_store_tree" / "PST" -> GetPetStoreTree(storeVersion)

Format payload TIDAK terbukti dari dump (bodi method kosong), jadi tiap
varian dicoba beberapa bentuk payload (JSON, string mentah, protobuf
field-1-string) dan SEMUA frame balasan dicatat mentah. Kontrol:
tree id "ugc-builder-in-game-store-1_91_0" (literal dari dump).

Pola koneksi mengikuti gift_diag.py: login_and_allocate -> tcp_connect
-> login frame -> tunggu tbugv/JoinedRoom -> kirim RoomEvent -> drain.

Hasil: ringkasan manusia + baris STORETREE_JSON:{...} di log.
Dibuat 2026-11-27 atas permintaan Bartes (probe katalog versi lama).
"""

import argparse
import asyncio
import json
import os
import socket
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


def decode_frame(frame):
    """Kembalikan (nama_event_atau_None, data_bytes, tipe)."""
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


def preview(data, n=400):
    try:
        s = data.decode("utf-8")
    except Exception:
        s = data.decode("utf-8", "replace")
    s = s.replace("\n", "\\n").replace("\r", "\\r")
    return s[:n]


def is_interesting(name, data, needles):
    if name in ("L", "L2", "PST", "store_tree", "pet_store_tree"):
        return True
    low = data.lower()
    for nd in needles:
        if nd in low:
            return True
    return False


def build_variants(tree_ids, pet_versions):
    variants = []
    for tid in tree_ids:
        tb = tid.encode("utf-8")
        variants.append((f"L json store_tree={tid}", "L",
                         json.dumps({"store_tree": tid}).encode("utf-8")))
        variants.append((f"L raw {tid}", "L", tb))
        variants.append((f"L protobuf-f1 {tid}", "L",
                         b"\x0a" + varint(len(tb)) + tb))
        variants.append((f"store_tree json {tid}", "store_tree",
                         json.dumps({"store_tree": tid}).encode("utf-8")))
        variants.append((f"L2 json list {tid}", "L2",
                         json.dumps({"store_tree": [tid]}).encode("utf-8")))
        variants.append((f"L2 json storesIds {tid}", "L2",
                         json.dumps({"storesIds": [tid]}).encode("utf-8")))
    for ver in pet_versions:
        vb = ver.encode("utf-8")
        variants.append((f"pet_store_tree json ver={ver}", "pet_store_tree",
                         json.dumps({"pet_store_tree": ver}).encode("utf-8")))
        variants.append((f"pet_store_tree raw ver={ver}", "pet_store_tree", vb))
        variants.append((f"PST json ver={ver}", "PST",
                         json.dumps({"pet_store_tree": ver}).encode("utf-8")))
        variants.append((f"PST raw ver={ver}", "PST", vb))
    return variants


async def probe_once(session, email, password, device_id,
                     tree_ids, pet_versions, wait_seconds):
    hasil = {"email": email, "login_ok": False, "variants": [],
             "interesting": []}
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
    print("[*] jendela login lewat (tbugv/JoinedRoom/timeout) — mulai kirim.")

    needles = [b"store_tree", b"pet_store_tree", b"group", b"item",
               b"version", b"pst"]
    for tid in tree_ids:
        needles.append(tid.encode("utf-8").lower())
    for ver in pet_versions:
        needles.append(ver.encode("utf-8").lower())

    variants = build_variants(tree_ids, pet_versions)
    for label, ev_name, payload in variants:
        rec = {"label": label, "event": ev_name, "sent_bytes": len(payload),
               "frames": 0, "names": [], "interesting": []}
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
            if name:
                rec["names"].append(name)
            if is_interesting(name, data, needles):
                item = {"event": name, "type": t,
                        "preview": preview(data),
                        "hex": data[:120].hex()}
                rec["interesting"].append(item)
                hasil["interesting"].append({"variant": label, **item})
                print(f"    [!!] frame menarik ev={name!r} type={t}: "
                      f"{preview(data, 200)}")
        hasil["variants"].append(rec)
        print(f"    -> {rec['frames']} frame, nama: {rec['names'][:10]}, "
              f"menarik: {len(rec['interesting'])}")

    print("[*] drain akhir 10 dtk (tangkapan terlambat)...")
    for f in fr.drain(10):
        name, data, t = decode_frame(f)
        if is_interesting(name, data, needles):
            item = {"event": name, "type": t, "preview": preview(data),
                    "hex": data[:120].hex()}
            hasil["interesting"].append({"variant": "straggler", **item})
            print(f"    [!!] straggler ev={name!r}: {preview(data, 200)}")
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
                                      "ugc-builder-in-game-store-1_91_0,"
                                      "ugc-builder-in-game-store"))
    ap.add_argument("--pet-versions",
                    default=os.getenv("STORETREE_PET_VERSIONS",
                                      "1_94_0,1_91_0,1.91.0"))
    ap.add_argument("--wait", type=int,
                    default=int(os.getenv("STORETREE_WAIT", "6")))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    tree_ids = [t.strip() for t in args.tree_ids.split(",") if t.strip()]
    pet_versions = [v.strip() for v in args.pet_versions.split(",")
                    if v.strip()]
    print(f"[*] Diag Store Tree — bot {args.email} | tree_ids={tree_ids} | "
          f"pet_versions={pet_versions} | tunggu {args.wait} dtk/varian")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await probe_once(
            session, args.email, args.password, args.device_id,
            tree_ids, pet_versions, args.wait)
    print("=" * 56)
    print("HASIL STORE TREE DIAG")
    print(f"  Login OK: {hasil['login_ok']}")
    print(f"  Varian dicoba: {len(hasil['variants'])}")
    for v in hasil["variants"]:
        print(f"    - {v['label']}: {v['frames']} frame, "
              f"menarik={len(v['interesting'])}")
    print(f"  Frame menarik total: {len(hasil['interesting'])}")
    for it in hasil["interesting"][:20]:
        print(f"    - [{it['variant']}] ev={it['event']!r}: "
              f"{it['preview'][:220]}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("STORETREE_JSON:" + json.dumps(hasil, ensure_ascii=False)[:6000])
    print("=" * 56)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
