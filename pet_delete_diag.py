# -*- coding: utf-8 -*-
"""
pet_delete_diag.py — Diag hapus pet dari inventory via TCP (tanpa mod).

Tes: apakah bot bisa menghapus (send-off) satu pet milik akun, lewat
RoomEvent "pet" — dan apakah pet itu benar-benar hilang dari daftar
sesudahnya.

Temuan dump (dump_3_n5ic.cs, game 1.94.x):
  PetRequestEventId.DeletePet = 17; PetResponseEventId.DeletePetResponse = 22.
  DeletePetRequest (protobuf): f1 EventId (varint, 17),
    f2 OwnerId (string, UUID pemilik), f3 PetId (string, UUID instance).
  DeletePetResponse: f1 EventId (22), f2 PetId, f3 Success (bool),
    f4 Wallet (Money), f5 Reward (Money, hadiah send-off).
  Wire: 08 11 12 <len ownerId> <ownerId> 1A <len petId> <petId>
  (pola sama seperti ActivatePetRequest yang sudah terbukti di
  crazyrun_maxpet_diag.py: RoomEvent "pet", payload = request ter-serialize
  langsung). Respons bisa lambat (~11 dtk, pola probe lain) — timeout 20 dtk;
  send yang timeout pun bisa tetap diproses server, jadi verifikasi lewat
  daftar ulang, JANGAN kirim ulang membabi-buta.

PENGAMAN (jangan dilonggarkan tanpa perintah eksplisit Bartes):
  - Tanpa PET_ID + CONFIRM=1, script HANYA menampilkan daftar (list-only).
  - Pet HARUS ada di daftar segar yang baru dibaca di run yang sama.
  - Tanpa FORCE=1, tolak: telur (stage 0/1), pet MAX stage 4,
    dan bentuk spesial (stage >= 5 / XP negatif). Yang boleh dihapus
    tanpa force: stage 2 (Baby) / 3 (Child) biasa.
  - Hapus dikirim SEKALI saja, lalu daftar ulang untuk verifikasi.
"""
import argparse
import asyncio
import json
import os
import time

import lookup_uuid as L
import pet_list_diag as PL


def build_delete_pet_request(owner_id, pet_id):
    """DeletePetRequest: f1 EventId=17, f2 OwnerId, f3 PetId."""
    out = b"\x08" + L.varint(17)
    o = owner_id.encode("utf-8")
    out += b"\x12" + L.varint(len(o)) + o
    p = pet_id.encode("utf-8")
    out += b"\x1a" + L.varint(len(p)) + p
    return out


def parse_delete_response(data):
    """Parse DeletePetResponse dari payload RoomEvent. Kembalikan dict
    atau None bila f1 != 22."""
    resp = {"event_id": None, "pet_id": None, "success": None,
            "wallet_hex": None, "reward_hex": None}
    for fno, wt, v in PL._raw_fields(bytes(data)):
        if fno == 1 and wt == 0:
            if v != 22:
                return None
            resp["event_id"] = v
        elif fno == 2 and wt == 2:
            try:
                resp["pet_id"] = bytes(v).decode("utf-8")
            except Exception:
                resp["pet_id"] = bytes(v).hex()
        elif fno == 3 and wt == 0:
            resp["success"] = bool(v)
        elif fno == 4 and wt == 2:
            resp["wallet_hex"] = bytes(v).hex()
        elif fno == 5 and wt == 2:
            resp["reward_hex"] = bytes(v).hex()
    if resp["event_id"] != 22:
        return None
    return resp


async def delete_diag_once(session, email, password, device_id,
                           pet_id, confirm, force):
    hasil = {"email": email, "mode": "list_only", "pet_id": pet_id or None,
             "pets_before": [], "pets_after": [], "sent": False,
             "response": None, "verified_gone": None, "refused": None,
             "error": None, "events": {}}
    user_jwt, alloc = await L.login_and_allocate(email, password, device_id,
                                                 session)
    if not alloc:
        hasil["error"] = "login/alokasi gagal"
        return hasil
    host, port = L.find_host_port(alloc)
    if not host or not port:
        hasil["error"] = "host/port tidak ketemu di allocation"
        return hasil
    print(f"[*] alokasi OK: {host}:{port}")
    sock, _pid = L.tcp_connect(host, port)
    if sock is None:
        hasil["error"] = "TCP connect gagal"
        return hasil

    def _catat(frame, sink):
        name, data = PL.decode_room_event(frame)
        if name is None or not data:
            return
        hasil["events"][name] = hasil["events"].get(name, 0) + 1
        pets, ok, err, _nxt = PL.extract_pets_from_page_response(data)
        if pets:
            sink.extend(pets)
        pets2, _ok2, _err2 = PL.extract_pet_from_getpets_response(data)
        seen = {p["pet_id"] for p in sink}
        for p in pets2:
            if p["pet_id"] not in seen:
                sink.append(p)
                seen.add(p["pet_id"])

    def _daftar(label, sink):
        req = PL.build_pet_page_request()
        print(f"[*] {label}: kirim PageRequest via RoomEvent 'pet' ...")
        sock.sendall(L.build_room_event_varint("pet", req))
        frames = fr.drain(6)
        print(f"[*] {label}: {len(frames)} frame diterima.")
        for f in frames:
            _catat(f, sink)
        print(f"[*] {label}: {len(sink)} pet.")

    try:
        sock.sendall(L.build_login_frame(user_jwt))
    except Exception as exc:
        hasil["error"] = f"kirim login frame gagal: {exc}"
        return hasil
    fr = PL.FrameReader(sock)
    print("[*] tunggu jendela login (tbugv/JoinedRoom, maks 3 dtk)...")
    got, closed = fr.read_until(
        lambda f: PL._is_tbugv(f) or PL._is_joinedroom(f), 3)
    if closed:
        hasil["error"] = "koneksi tertutup saat tunggu login"
        return hasil
    if got is not None and PL._is_tbugv(got):
        print("[*] tbugv diterima — observasi 1 dtk lagi.")
        time.sleep(1)

    # --- daftar SEBELUM ---
    before = []
    _daftar("daftar SEBELUM", before)
    hasil["pets_before"] = before
    for p in before:
        print(f"    - id={p['pet_id']} type={p['type']} stage={p.get('stage')} "
              f"xp={p.get('xp', 0)} name={p['display_name']!r}")

    # --- gerbang pengaman: tanpa PET_ID + CONFIRM=1, berhenti di daftar ---
    if not pet_id or confirm != "1":
        print("[*] mode list-only (PET_ID kosong atau CONFIRM != 1) — "
              "TIDAK mengirim hapus.")
        return hasil
    hasil["mode"] = "delete"

    target = next((p for p in before if p["pet_id"] == pet_id), None)
    if target is None:
        hasil["refused"] = "pet_id tidak ada di daftar segar run ini"
        print(f"[!] DITOLAK: {hasil['refused']}")
        return hasil
    stage = target.get("stage")
    xp = target.get("xp", 0)
    if force != "1":
        if stage is None or stage in (0, 1):
            hasil["refused"] = "telur (stage 0/1) — perlu FORCE=1"
        elif stage == 4:
            hasil["refused"] = "pet MAX stage 4 — perlu FORCE=1"
        elif stage >= 5 or xp < 0:
            hasil["refused"] = "bentuk spesial (stage>=5/XP negatif) — perlu FORCE=1"
        if hasil["refused"]:
            print(f"[!] DITOLAK: {hasil['refused']} "
                  f"(id={pet_id} stage={stage} xp={xp})")
            return hasil

    owner_id = target.get("owner_id") or ""
    if not owner_id:
        hasil["refused"] = "owner_id target kosong di daftar"
        print(f"[!] DITOLAK: {hasil['refused']}")
        return hasil
    req = build_delete_pet_request(owner_id, pet_id)
    print(f"[*] kirim DeletePetRequest SEKALI via RoomEvent 'pet' "
          f"({len(req)} B): {req.hex()}")
    print(f"    owner={owner_id} pet={pet_id}")
    try:
        sock.sendall(L.build_room_event_varint("pet", req))
    except Exception as exc:
        hasil["error"] = f"kirim DeletePetRequest gagal: {exc}"
        return hasil
    hasil["sent"] = True

    # --- tunggu respons (lambat, maks 20 dtk) ---
    frames = fr.drain(20)
    print(f"[*] {len(frames)} frame diterima setelah hapus.")
    for f in frames:
        name, data = PL.decode_room_event(f)
        if name is None or not data:
            continue
        hasil["events"][name] = hasil["events"].get(name, 0) + 1
        resp = parse_delete_response(data)
        if resp is not None:
            hasil["response"] = resp
            print(f"[+] DeletePetResponse: pet={resp['pet_id']} "
                  f"success={resp['success']} wallet={resp['wallet_hex']} "
                  f"reward={resp['reward_hex']}")
    if hasil["response"] is None:
        print("[*] tidak ada DeletePetResponse (f1=22) dalam 20 dtk — "
              "verifikasi lewat daftar ulang (send timeout bisa tetap "
              "diproses server).")

    # --- daftar SESUDAH (kebenaran akhir) ---
    after = []
    _daftar("daftar SESUDAH", after)
    hasil["pets_after"] = after
    hasil["verified_gone"] = all(p["pet_id"] != pet_id for p in after)
    print(f"[+] verifikasi: pet hilang dari daftar = {hasil['verified_gone']} "
          f"({len(before)} -> {len(after)} pet)")
    try:
        sock.close()
    except Exception:
        pass
    return hasil


def _kirim_hasil_cepat(email, result):
    secret = os.getenv("PET_RESULT_SECRET", "")
    if not secret:
        return
    try:
        import urllib.request
        data = json.dumps({"kind": "petdelete", "email": email,
                           "result": result, "secret": secret}).encode()
        req = urllib.request.Request(
            "https://pkxd.shop/api/pet-result", data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=20) as resp:
            resp.read()
        print("[+] hasil cepat terkirim ke /api/pet-result")
    except Exception as exc:
        print(f"[*] kirim hasil cepat gagal (log tetap jadi fallback): {exc}")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("PETDELETE_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("PETDELETE_PASSWORD", ""))
    ap.add_argument("--device-id",
                    default=os.getenv("PETDELETE_DEVICE_ID", "")
                    or "9f31c2d4-probe-device")
    ap.add_argument("--pet-id", default=os.getenv("PETDELETE_PET_ID", ""))
    ap.add_argument("--confirm", default=os.getenv("PETDELETE_CONFIRM", "0"))
    ap.add_argument("--force", default=os.getenv("PETDELETE_FORCE", "0"))
    args = ap.parse_args()
    if not args.email or not args.password:
        print("[!] email/password bot wajib diisi.")
        return 1
    print(f"[*] Diag Pet Delete — bot {args.email} | pet_id={args.pet_id or '-'} "
          f"| confirm={args.confirm} force={args.force}")
    L.QUIET = False
    t0 = time.time()
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await delete_diag_once(
            session, args.email, args.password, args.device_id,
            args.pet_id, args.confirm, args.force)
    print("=" * 56)
    print("HASIL PET DELETE DIAG")
    print(f"  Mode: {hasil['mode']} | terkirim: {hasil['sent']} | "
          f"ditolak: {hasil['refused']}")
    print(f"  Respons: {hasil['response']}")
    print(f"  Terverifikasi hilang: {hasil['verified_gone']} "
          f"({len(hasil['pets_before'])} -> {len(hasil['pets_after'])} pet)")
    print(f"  RoomEvent: {hasil['events']} | error: {hasil['error']}")
    print(f"  Waktu total: {time.time() - t0:.0f} detik")
    print("=" * 56)
    result = {
        "mode": hasil["mode"], "email": hasil["email"],
        "pet_id": hasil["pet_id"], "sent": hasil["sent"],
        "refused": hasil["refused"], "response": hasil["response"],
        "verified_gone": hasil["verified_gone"],
        "count_before": len(hasil["pets_before"]),
        "count_after": len(hasil["pets_after"]),
        "error": hasil["error"],
        "pets_before": [
            {"pet_id": p["pet_id"], "type": p["type"],
             "display_name": p["display_name"], "owner_id": p["owner_id"],
             "stage": p.get("stage"), "xp": p.get("xp", 0)}
            for p in hasil["pets_before"]
        ],
        "pets_after": [
            {"pet_id": p["pet_id"], "type": p["type"],
             "display_name": p["display_name"],
             "stage": p.get("stage"), "xp": p.get("xp", 0)}
            for p in hasil["pets_after"]
        ],
    }
    print("PETDELETE_JSON: " + json.dumps(result, ensure_ascii=False))
    _kirim_hasil_cepat(hasil["email"], result)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
