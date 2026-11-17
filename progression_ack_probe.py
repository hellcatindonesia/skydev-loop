# -*- coding: utf-8 -*-
"""
progression_ack_probe.py — Probe celah klaim Progression (1 bot bernama
fresh dari panel, repo hellcatindonesia/skydev-loop).

Latar (2026-11-17, Bartes): dari analisis dump, klaim milestone jalan
lewat RoomEvent "progression" (AcknowledgeMilestoneRequest: f1 EventId=2,
f2 KpiId, f3 Version) dan server menjawab AcknowledgeMilestoneResponse
(f8 IsSuccess + hadiah + dompet + level baru). Hipotesis yang diuji —
preseden Auto ACC carry yang dulu dikira mustahil tapi server menerima:
server MUNGKIN tidak memvalidasi ulang nilai KPI saat menerima ack, jadi
klaim palsu bisa diterima.
Probe ini SATU klaim palsu, SATU KPI (default activity_visits_count,
KPI UGC, milestone 1 juta kunjungan -> CreatorUgc), di SATU bot bernama
fresh (nilai KPI = 0, level awal). PENJAGA: ack HANYA dikirim bila klaim
ini palsu (nilai KPI < goal milestone terkecil); bila bot ternyata sudah
berhak, ack dilewati. TIDAK kirim ValidateMissingRewards /
AcknowledgeStackedLevelUp (itu bisa mengklaim hal lain).
Alur: ambil bot bernama fresh (RPC get_fresh_named_bots) -> login ->
alokasi sendiri -> TCP login -> SendDeviceId -> GetInitializationInfo
(version "" lalu "1") -> catat nilai KPI + level + milestone (bukti
klaim palsu) -> AcknowledgeMilestone -> catat IsSuccess/hadiah/dompet/
level -> FetchKPIs + GetInitializationInfo ulang (apakah ada yang
menempel di server) -> PROGRESSION_ACK_JSON + VERDICT
(ACCEPTED/REJECTED/...).
Blast radius bila server menerima: bot mendapat hadiah milestone
PERTAMA KPI itu saja (yang paling kecil) — role CreatorUgc ada di
milestone paling jauh (1 juta), jadi tidak mungkin langsung dapat role
dari satu ack ini. Password bot TIDAK pernah dicetak ke log.
"""
import argparse
import asyncio
import json
import os
import struct
import time
import urllib.request

import lookup_uuid as L
import gift_diag as G

BOT_USER_ID = "71056756-53c7-478e-9356-82c87fb2dd63"
DEFAULT_DEVICE_ID = "9f31c2d4-probe-device"

KPI_TYPE_NAMES = {0: "HOUSE", 1: "PETS", 2: "LIFESTYLE", 3: "UGC"}
ROLE_NAMES = {0: "User", 1: "Influencer", 2: "Admin",
              3: "MinorInfluencer", 4: "CreatorUgc",
              5: "EventTeam01", 6: "EventTeam02", 7: "EventTeam03",
              8: "HighScorePlayer", 9: "EventPass", 10: "Undefined"}


def _varint(n):
    """Encode int non-negatif sebagai varint (untuk panjang field)."""
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)


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
    """Wire-2 chunk -> str HANYA bila isinya teks printable (nama/ID).
    Pesan nested berisi byte kontrol TIDAK boleh jadi str."""
    try:
        s = chunk.decode("utf-8")
    except Exception:
        return chunk
    if s and all(c.isprintable() for c in s):
        return s
    return chunk


def parse_protobuf(buf):
    """Parse protobuf generik. Wire 0 varint, wire 2 length-delim
    (panjang varint — BUKAN 1 byte, karena respons bisa >127 byte),
    wire 5 fixed32, wire 1 fixed64. Field berulang dikumpulkan list.
    Wire-2 yang bukan teks printable TETAP bytes (pesan nested /
    packed repeated)."""
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


def _as_list(d, field):
    v = d.get(field)
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _nested(v, depth=0):
    """Nilai wire-2 berupa bytes: coba parse sebagai protobuf nested,
    REKURSIF (maks 3 level). Bila tidak bisa diparse, kembalikan hex.
    String/int diteruskan apa adanya."""
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


def parse_money(raw):
    """Money: f1 coins, f2 gems (konvensi "fw"). Field lain (f3+)
    dicatat apa adanya."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    out = {}
    if d.get(1) is not None:
        out["coins"] = d.get(1)
    if d.get(2) is not None:
        out["gems"] = d.get(2)
    for k, v in d.items():
        if k not in (1, 2):
            out[f"f{k}"] = _jsonable(_nested(v))
    return out


def parse_item_reward(raw):
    """ItemReward: f1 Item (ItemQuantity: f1 ItemId, f2 Quantity),
    f2 Background (ItemPresentation)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    item = d.get(1)
    iq = parse_protobuf(item) if isinstance(item, (bytes, bytearray)) \
        else (item or {})
    return {"item_id": iq.get(1), "quantity": iq.get(2),
            "background": _jsonable(_nested(d.get(2)))
            if d.get(2) is not None else None}


def parse_role_reward(raw):
    """RoleReward: f1 Type (PlayerRole enum)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    role = d.get(1)
    return {"role": role,
            "role_name": ROLE_NAMES.get(role, f"Unknown({role})")}


def parse_progression_reward(raw):
    """ProgressionReward: f1 Money, f2 Items (repeated ItemReward),
    f3 Role (RoleReward). Tepat satu yang terisi (tiga constructor
    terpisah di C#), tapi semua yang ada dicatat."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    out = {}
    if d.get(1) is not None:
        out["money"] = parse_money(d.get(1))
    items = _as_list(d, 2)
    if items:
        out["items"] = [parse_item_reward(x) for x in items]
    if d.get(3) is not None:
        out["role"] = parse_role_reward(d.get(3))
    if "role" in out:
        out["kind"] = "Role"
    elif "items" in out:
        out["kind"] = "Item"
    elif "money" in out:
        out["kind"] = "Money"
    else:
        out["kind"] = "Unknown"
        out["raw"] = _jsonable(_nested(d))
    return out


def parse_milestone(raw):
    """Milestone: f1 Goal (int), f2 Reward (ProgressionReward)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    rew = d.get(2)
    return {"goal": d.get(1),
            "reward": parse_progression_reward(rew)
            if rew is not None else None}


def parse_level_info(raw):
    """LevelInfo: f1 CurrentLevel, f2 EligibleLevel."""
    if raw is None:
        return None
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    return {"current_level": d.get(1), "eligible_level": d.get(2)}


def parse_kpi(raw):
    """KPI: f1 Id, f2 Milestones, f3 Type, f4 Value,
    f5 MilestoneLevel, f6 DescKey, f7 CompleteDescKey."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    ktype = d.get(3)
    return {"id": d.get(1),
            "type": ktype,
            "type_name": KPI_TYPE_NAMES.get(ktype, f"Unknown({ktype})"),
            "value": d.get(4),
            "milestone_level": parse_level_info(d.get(5)),
            "desc_key": d.get(6),
            "complete_desc_key": d.get(7),
            "milestones": [parse_milestone(m) for m in _as_list(d, 2)]}


def parse_level_up_config(raw):
    """LevelUpConfig: f1 RequiredMilestones,
    f2 Rewards (repeated ProgressionReward)."""
    d = parse_protobuf(raw) if isinstance(raw, (bytes, bytearray)) else raw
    return {"required_milestones": d.get(1),
            "rewards": [parse_progression_reward(r)
                        for r in _as_list(d, 2)]}


def parse_init_info(data):
    """GetInitializationInfoResponse: f1 EventId, f2 Kpis, f3
    PlayerLevel (LevelInfo), f4 LevelUpConfigs, f5 RemotePlayerLevelMap
    (map<string,int> — tiap entri nested f1 key, f2 value),
    f6 IsSuccess, f7 ErrorMessage."""
    d = parse_protobuf(data)
    remote_map = {}
    for e in _as_list(d, 5):
        if isinstance(e, (bytes, bytearray)):
            ed = parse_protobuf(bytes(e))
            if ed.get(1) is not None:
                remote_map[ed.get(1)] = ed.get(2)
        elif isinstance(e, str):
            remote_map[e] = None
    return {"event_id": d.get(1) if d.get(1) is not None else 0,
            "player_level": parse_level_info(d.get(3)),
            "level_up_configs": [parse_level_up_config(c)
                                 for c in _as_list(d, 4)],
            "remote_player_level_map": remote_map,
            "is_success": bool(d.get(6)),
            "error_message": d.get(7),
            "kpis": [parse_kpi(k) for k in _as_list(d, 2)]}


def build_init_payload(version):
    """GetInitializationInfoRequest bytes.
    f1 EventId=0 (InitializationInfo) default proto3 -> dihilangkan.
    f2 Version (string) -> 12 <len> <version>.
    Bila version kosong, field 2 juga default -> payload KOSONG."""
    if not version:
        return b""
    vb = version.encode("utf-8")
    return b"\x12" + _varint(len(vb)) + vb


SKYDEV_UUID = "aae2efe1-1abb-48c2-8799-ea192692a49c"


def fetch_named_bot(supabase_url, supabase_key, limit=1):
    """Ambil 1 bot bernama fresh dari panel via RPC get_fresh_named_bots
    (pola visit_loop.py). Kembalikan dict {email, password, device_id}
    atau None. Password TIDAK pernah dicetak ke log."""
    payload = {"p_target_uuid": SKYDEV_UUID, "p_limit": limit}
    req = urllib.request.Request(
        f"{supabase_url}/rest/v1/rpc/get_fresh_named_bots",
        data=json.dumps(payload).encode("utf-8"), method="POST")
    req.add_header("apikey", supabase_key)
    req.add_header("Authorization", f"Bearer {supabase_key}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        print(f"[-] Gagal mengambil bot bernama dari database: {exc}")
        return None
    if not isinstance(data, list) or not data:
        print("[-] RPC get_fresh_named_bots kosong — tidak ada bot bernama fresh.")
        return None
    bot = data[0]
    print(f"[+] Bot bernama: {bot.get('email')} "
          f"(device_id {bot.get('device_id')})")
    return bot


def build_ack_payload(kpi_id, version):
    """AcknowledgeMilestoneRequest bytes: f1 EventId=2 (08 02),
    f2 KpiId (12 <len> <id>), f3 Version (1A <len> <version>,
    dihilangkan bila version kosong — proto3 default)."""
    kb = kpi_id.encode("utf-8")
    out = b"\x08\x02" + b"\x12" + _varint(len(kb)) + kb
    if version:
        vb = version.encode("utf-8")
        out += b"\x1a" + _varint(len(vb)) + vb
    return out


def build_fetchkpis_payload(version):
    """FetchKPIsRequest bytes: f1 EventId=3 (08 03), f2 Version
    (12 <len> <version>, dihilangkan bila kosong)."""
    out = b"\x08\x03"
    if version:
        vb = version.encode("utf-8")
        out += b"\x12" + _varint(len(vb)) + vb
    return out


def parse_ack_response(data):
    """AcknowledgeMilestoneResponse: f1 EventId, f2 KpiId, f3
    MilestoneLevel (LevelInfo), f4 Reward (ProgressionReward), f5
    PlayerLevel (LevelInfo), f6 LevelUpReward (repeated
    ProgressionReward), f7 Wallet (Money), f8 IsSuccess (bool)."""
    d = parse_protobuf(data)
    return {"event_id": d.get(1) if d.get(1) is not None else 0,
            "kpi_id": d.get(2),
            "milestone_level": parse_level_info(d.get(3)),
            "reward": parse_progression_reward(d.get(4))
            if d.get(4) is not None else None,
            "player_level": parse_level_info(d.get(5)),
            "level_up_rewards": [parse_progression_reward(r)
                                 for r in _as_list(d, 6)],
            "wallet": parse_money(d.get(7))
            if d.get(7) is not None else None,
            "is_success": bool(d.get(8))}


def parse_fetchkpis_response(data):
    """FetchKPIsResponse: f1 EventId, f2 KpiProgressMap (map<string,int>
    — tiap entri nested f1 key, f2 value)."""
    d = parse_protobuf(data)
    pmap = {}
    for e in _as_list(d, 2):
        if isinstance(e, (bytes, bytearray)):
            ed = parse_protobuf(bytes(e))
            if ed.get(1) is not None:
                pmap[ed.get(1)] = ed.get(2)
        elif isinstance(e, str):
            pmap[e] = None
    return {"event_id": d.get(1) if d.get(1) is not None else 0,
            "kpi_progress_map": pmap}


async def progression_once(session, email, password, device_id, versions,
                           kpi_id="activity_visits_count"):
    hasil = {"email": email, "bot_user_id": BOT_USER_ID,
             "server": "", "room": None, "player_id": None,
             "login_ok": False, "deviceid_sent": False,
             "versions_tried": [], "version_ok": None,
             "init_info": None, "ugc_kpis": [],
             "role_reward_milestones": [],
             "has_ugc_kpi": False, "has_creator_ugc_reward": False,
             "error": None,
             "ack_target": None, "ack_response": None,
             "ack_accepted": None, "verdict": None,
             "fetchkpis_after": None, "init_info_after": None,
             "post_check": None}
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
        # SendDeviceId (pola spawn_test / probe "fw" / streak, TERBUKTI).
        send_dev = device_id or DEFAULT_DEVICE_ID
        try:
            sock.sendall(L.build_room_event_varint(
                "Analytics/deviceId", send_dev.encode("utf-8")))
            hasil["deviceid_sent"] = True
            print("[+] SendDeviceId terkirim ('Analytics/deviceId').")
        except Exception as exc:
            print(f"[!] kirim SendDeviceId gagal: {exc}")

        def wait_progression_init(timeout=10):
            """Baca frame sampai dapat RoomEvent 'progression' yang
            responsnya ber-EventId=0 (InitializationInfo), atau timeout.
            Kembalikan (data bytes, notes) atau (None, notes). Event
            'progression' lain (mis. MilestoneReached) dicatat dan
            dilewati — bukan respons yang dicari."""
            end = time.time() + timeout
            notes = []
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name == "progression" and d is not None:
                        try:
                            dd = parse_protobuf(d)
                        except Exception:
                            dd = {}
                        if dd.get(1) in (None, 0):
                            return d, notes
                        notes.append(f"progression EventId={dd.get(1)} "
                                     f"({len(d)} B, bukan init)")
                    elif name:
                        notes.append(name)
            return None, notes

        def init_request(version, timeout=10):
            """Kirim GetInitializationInfo (version) + tunggu respons
            init. Maks 1 kirim ulang HANYA bila tidak ada respons.
            Bila server menjawab IsSuccess=false (ditolak), TIDAK
            diulang untuk version itu — kembalikan hasil parse agar
            pemanggil bisa coba version lain. Raw hex selalu dicetak
            SEBELUM parse — bila parse gagal, respons tidak hilang."""
            payload = build_init_payload(version)
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("progression",
                                                           payload))
                except Exception as exc:
                    print(f"[!] kirim GetInitializationInfo gagal: {exc}")
                    return None
                print(f"[*] kirim GetInitializationInfo (version="
                      f"{version!r}, payload "
                      f"{payload.hex() if payload else '(kosong)'}), "
                      f"percobaan {attempt}/2, tunggu respons maks "
                      f"{timeout} dtk...")
                data, notes = wait_progression_init(timeout)
                if data is not None:
                    print(f"[+] respons 'progression' (EventId=0) "
                          f"diterima ({len(data)} B): {data.hex()}")
                    try:
                        return parse_init_info(data)
                    except Exception as exc:
                        print(f"[!] parse respons GetInitializationInfo "
                              f"gagal: {exc!r} (raw hex di atas)")
                        import traceback
                        traceback.print_exc()
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons 'progression' (EventId=0) "
                      f"untuk version={version!r} (percobaan {attempt}/2).")
                if notes:
                    print(f"    (selama tunggu terlihat: {notes[:10]})")
            return None

        def wait_progression_event(want_event, timeout=10):
            """Baca frame sampai dapat RoomEvent 'progression' dengan
            EventId=want_event, atau timeout. Kembalikan (data, notes).
            Event 'progression' lain dicatat dan dilewati."""
            end = time.time() + timeout
            notes = []
            while time.time() < end:
                for f in fr.drain(0.5):
                    name, d = G.decode_room_event(f)
                    if name == "progression" and d is not None:
                        try:
                            dd = parse_protobuf(d)
                        except Exception:
                            dd = {}
                        if dd.get(1) == want_event:
                            return d, notes
                        notes.append(f"progression EventId={dd.get(1)} "
                                     f"({len(d)} B)")
                    elif name:
                        notes.append(name)
            return None, notes

        def ack_request(version, target_kpi_id, timeout=10):
            """Kirim AcknowledgeMilestoneRequest (EventId=2) + tunggu
            respons EventId=2. Maks 1 kirim ulang HANYA bila tidak ada
            respons. Raw hex selalu dicetak SEBELUM parse."""
            payload = build_ack_payload(target_kpi_id, version)
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("progression",
                                                           payload))
                except Exception as exc:
                    print(f"[!] kirim AcknowledgeMilestone gagal: {exc}")
                    return None
                print(f"[*] kirim AcknowledgeMilestone "
                      f"(kpi={target_kpi_id!r}, version={version!r}, "
                      f"payload {payload.hex()}), percobaan {attempt}/2, "
                      f"tunggu maks {timeout} dtk...")
                data, notes = wait_progression_event(2, timeout)
                if data is not None:
                    print(f"[+] respons 'progression' (EventId=2) "
                          f"diterima ({len(data)} B): {data.hex()}")
                    try:
                        return parse_ack_response(data)
                    except Exception as exc:
                        print(f"[!] parse respons AcknowledgeMilestone "
                              f"gagal: {exc!r}")
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons 'progression' (EventId=2) "
                      f"untuk ack (percobaan {attempt}/2).")
                if notes:
                    print(f"    (selama tunggu terlihat: {notes[:10]})")
            return None

        def fetchkpis_request(version, timeout=10):
            """Kirim FetchKPIsRequest (EventId=3) + tunggu respons
            EventId=3. Maks 1 kirim ulang HANYA bila tidak ada respons."""
            payload = build_fetchkpis_payload(version)
            for attempt in (1, 2):
                try:
                    sock.sendall(L.build_room_event_varint("progression",
                                                           payload))
                except Exception as exc:
                    print(f"[!] kirim FetchKPIs gagal: {exc}")
                    return None
                print(f"[*] kirim FetchKPIs (version={version!r}), "
                      f"percobaan {attempt}/2, tunggu maks {timeout} dtk...")
                data, notes = wait_progression_event(3, timeout)
                if data is not None:
                    print(f"[+] respons 'progression' (EventId=3) "
                          f"diterima ({len(data)} B): {data.hex()}")
                    try:
                        return parse_fetchkpis_response(data)
                    except Exception as exc:
                        print(f"[!] parse respons FetchKPIs gagal: {exc!r}")
                        return {"parse_error": repr(exc),
                                "raw_hex": data.hex()}
                print(f"[!] tidak ada respons 'progression' (EventId=3) "
                      f"(percobaan {attempt}/2).")
                if notes:
                    print(f"    (selama tunggu terlihat: {notes[:10]})")
            return None
        # GetInitializationInfo — coba version satu per satu.
        best_info = None
        for version in versions:
            hasil["versions_tried"].append(version)
            info = init_request(version)
            if info is None:
                print(f"[!] version={version!r}: TIDAK ADA respons.")
                continue
            if isinstance(info, dict) and "parse_error" in info:
                print(f"[!] version={version!r}: parse gagal: "
                      f"{info['parse_error']}")
                hasil["error"] = (f"parse gagal (version={version!r})")
                hasil["init_info"] = info
                best_info = info
                break
            hasil["init_info"] = info
            best_info = info
            if info.get("is_success"):
                hasil["version_ok"] = version
                print(f"[+] version={version!r}: IsSuccess=true "
                      f"({len(info.get('kpis') or [])} KPI).")
                break
            # Server menjawab tapi IsSuccess=false -> ditolak; coba
            # version lain (jangan ulang version yang sama).
            print(f"[!] version={version!r}: server menjawab "
                  f"IsSuccess=false "
                  f"(ErrorMessage={info.get('error_message')!r}) — "
                  f"coba version lain.")
            hasil["error"] = (f"IsSuccess=false (version={version!r}): "
                              f"{info.get('error_message')}")
        if hasil["version_ok"] is None and not hasil.get("error"):
            hasil["error"] = ("GetInitializationInfo tanpa respons "
                              "(semua version dicoba)")

        # Analisis init_info (yang sukses, atau terakhir diterima).
        info = best_info or hasil.get("init_info")
        if info and isinstance(info, dict) and "parse_error" not in info:
            kpis = info.get("kpis") or []
            print(f"[+] Daftar KPI diterima: {len(kpis)} KPI, "
                  f"PlayerLevel={info.get('player_level')}, "
                  f"IsSuccess={info.get('is_success')}")
            for k in kpis:
                print(f"  KPI id={k.get('id')!r} type={k.get('type')} "
                      f"({k.get('type_name')}) value={k.get('value')} "
                      f"milestone_level={k.get('milestone_level')} "
                      f"desc={k.get('desc_key')!r} "
                      f"milestones={len(k.get('milestones') or [])}")
                for idx, m in enumerate(k.get("milestones") or []):
                    rw = m.get("reward") or {}
                    detail = ""
                    if rw.get("kind") == "Role":
                        r = rw.get("role") or {}
                        detail = (f"Role={r.get('role')} "
                                  f"({r.get('role_name')})")
                    elif rw.get("kind") == "Item":
                        detail = ", ".join(
                            f"ItemId={it.get('item_id')} "
                            f"x{it.get('quantity')}"
                            for it in rw.get("items") or [])
                    elif rw.get("kind") == "Money":
                        mo = rw.get("money") or {}
                        detail = (f"coins={mo.get('coins')} "
                                  f"gems={mo.get('gems')}")
                    print(f"    milestone[{idx}] goal={m.get('goal')} "
                          f"reward={rw.get('kind')} {detail}")
            # Jawaban tegas untuk 2 pertanyaan probe.
            ugc = [k for k in kpis if k.get("type") == 3]
            hasil["ugc_kpis"] = ugc
            hasil["has_ugc_kpi"] = bool(ugc)
            role_ms = []
            for k in kpis:
                for idx, m in enumerate(k.get("milestones") or []):
                    rw = m.get("reward") or {}
                    if rw.get("kind") == "Role":
                        r = rw.get("role") or {}
                        role_ms.append({
                            "kpi_id": k.get("id"),
                            "kpi_type": k.get("type"),
                            "kpi_type_name": k.get("type_name"),
                            "milestone_index": idx,
                            "goal": m.get("goal"),
                            "role": r.get("role"),
                            "role_name": r.get("role_name")})
            hasil["role_reward_milestones"] = role_ms
            hasil["has_creator_ugc_reward"] = any(
                r.get("role") == 4 for r in role_ms)
            print(f"[*] Ada KPI UGC (type=3)? "
                  f"{'YA' if hasil['has_ugc_kpi'] else 'TIDAK'} "
                  f"({len(ugc)} KPI)")
            print(f"[*] Ada milestone berhadiah CreatorUgc (Role=4)? "
                  f"{'YA' if hasil['has_creator_ugc_reward'] else 'TIDAK'} "
                  f"({len(role_ms)} milestone Role total)")
            for r in role_ms:
                print(f"    RoleReward: KPI {r.get('kpi_id')!r} "
                      f"({r.get('kpi_type_name')}) milestone[{r.get('milestone_index')}] "
                      f"goal={r.get('goal')} -> {r.get('role_name')} "
                      f"(Role={r.get('role')})")
        # === FASE ACK (probe celah klaim) ===
        # Hanya jalan bila init sukses (version_ok ketemu). Target: KPI
        # yang diminta --kpi-id (default activity_visits_count); bila
        # tidak ada di daftar, pakai KPI UGC (type=3) pertama.
        # PENJAGA: ack HANYA bila klaim ini palsu — nilai KPI masih di
        # bawah goal milestone terkecil. Bila bot ternyata SUDAH berhak
        # (nilai >= goal), ack dilewati (verdict SKIPPED_NOT_BOGUS)
        # supaya probe tidak mengklaim hadiah yang memang berhak didapat.
        if hasil.get("version_ok") is not None:
            info_ok = hasil.get("init_info")
            if isinstance(info_ok, dict) and "parse_error" not in info_ok:
                kpis_ok = info_ok.get("kpis") or []
                target = None
                for k in kpis_ok:
                    if k.get("id") == kpi_id:
                        target = k
                        break
                if target is None:
                    ugc_ok = [k for k in kpis_ok if k.get("type") == 3]
                    if ugc_ok:
                        target = ugc_ok[0]
                        print(f"[*] KPI {kpi_id!r} tidak ada di daftar; "
                              f"pakai KPI UGC pertama: {target.get('id')!r}")
                if target is None:
                    hasil["verdict"] = "NO_TARGET_KPI"
                    print("[!] Tidak ada KPI target (bukan UGC) — ack dilewati.")
                else:
                    goals = [m.get("goal") for m in (target.get("milestones") or [])
                             if isinstance(m.get("goal"), int)]
                    min_goal = min(goals) if goals else None
                    val_before = target.get("value")
                    bogus = (isinstance(val_before, int) and min_goal is not None
                             and val_before < min_goal)
                    hasil["ack_target"] = {
                        "kpi_id": target.get("id"),
                        "type": target.get("type"),
                        "type_name": target.get("type_name"),
                        "value_before": val_before,
                        "min_goal": min_goal,
                        "player_level_before": info_ok.get("player_level"),
                        "milestone_level_before": target.get("milestone_level"),
                        "milestones": target.get("milestones"),
                    }
                    print(f"[*] Target ack: KPI {target.get('id')!r} "
                          f"value={val_before} min_goal={min_goal} "
                          f"player_level={info_ok.get('player_level')}")
                    if not bogus:
                        hasil["verdict"] = "SKIPPED_NOT_BOGUS"
                        print("[!] Nilai KPI sudah >= goal terkecil — klaim "
                              "ini bukan palsu, ack DILEWATI.")
                    else:
                        ack_resp = ack_request(hasil["version_ok"],
                                               target.get("id"))
                        hasil["ack_response"] = ack_resp
                        if ack_resp and isinstance(ack_resp, dict) \
                                and "parse_error" not in ack_resp:
                            ok = bool(ack_resp.get("is_success"))
                            hasil["ack_accepted"] = ok
                            hasil["verdict"] = ("ACCEPTED" if ok
                                                else "REJECTED")
                            print(f"[{'!' if ok else '*'}] "
                                  f"AcknowledgeMilestone -> "
                                  f"is_success={ok} "
                                  f"player_level={ack_resp.get('player_level')} "
                                  f"wallet={ack_resp.get('wallet')} "
                                  f"reward={ack_resp.get('reward')}")
                        elif ack_resp:
                            hasil["ack_accepted"] = False
                            hasil["verdict"] = "ACK_PARSE_ERROR"
                            print("[!] Respons ack gagal diparse.")
                        else:
                            hasil["ack_accepted"] = False
                            hasil["verdict"] = "ACK_NO_RESPONSE"
                            print("[!] Tidak ada respons ack dari server.")
                        # Verifikasi pasca-ack: FetchKPIs + init ulang,
                        # bandingkan nilai KPI + level sebelum/sesudah.
                        fk = fetchkpis_request(hasil["version_ok"])
                        hasil["fetchkpis_after"] = fk
                        info2 = init_request(hasil["version_ok"])
                        hasil["init_info_after"] = info2
                        post = {"value_after": None,
                                "player_level_after": None,
                                "milestone_level_after": None,
                                "changed": False}
                        if isinstance(info2, dict) and "parse_error" not in info2:
                            post["player_level_after"] = info2.get("player_level")
                            for k2 in (info2.get("kpis") or []):
                                if k2.get("id") == target.get("id"):
                                    post["value_after"] = k2.get("value")
                                    post["milestone_level_after"] = k2.get("milestone_level")
                                    break
                            if isinstance(fk, dict) and "parse_error" not in fk:
                                pm = (fk.get("kpi_progress_map") or {})
                                if target.get("id") in pm:
                                    post["value_after_fetchkpis"] = pm[target.get("id")]
                            post["changed"] = (
                                post["value_after"] != val_before
                                or post["player_level_after"] != info_ok.get("player_level"))
                        hasil["post_check"] = post
                        print(f"[*] Pasca-ack: value {val_before} -> "
                              f"{post['value_after']}, level "
                              f"{info_ok.get('player_level')} -> "
                              f"{post['player_level_after']}, "
                              f"changed={post['changed']}")
    finally:
        try:
            sock.close()
        except Exception:
            pass
    return hasil


DEFAULT_VERSIONS = ["", "1"]


async def main():
    L.apply_panel_versions()
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", default=os.getenv("PROG_EMAIL", ""))
    ap.add_argument("--password", default=os.getenv("PROG_PASSWORD", ""))
    ap.add_argument("--device-id", default=os.getenv("PROG_DEVICE_ID", ""))
    ap.add_argument("--kpi-id", default="activity_visits_count",
                    help="KPI yang di-ack (default activity_visits_count)")
    ap.add_argument("--versions", default="",
                    help="Daftar version dipisah koma; kosong = default "
                         "['', '1'] (coba '' dulu, lalu '1')")
    args = ap.parse_args()
    email, password, device_id = args.email, args.password, args.device_id
    if not email or not password:
        bot = fetch_named_bot(os.getenv("SUPABASE_URL", ""),
                              os.getenv("SUPABASE_SERVICE_KEY", ""))
        if not bot or not bot.get("email") or not bot.get("password"):
            print("[!] email/password bot wajib diisi, atau bot bernama "
                  "harus tersedia di panel (RPC get_fresh_named_bots).")
            return 1
        email, password = bot["email"], bot["password"]
        device_id = device_id or bot.get("device_id") or ""
    versions = args.versions.split(",") if args.versions \
        else DEFAULT_VERSIONS
    async with L.AsyncSession(impersonate="chrome110",
                              verify=False) as session:
        hasil = await progression_once(
            session, email, password, device_id, versions, args.kpi_id)
    hasil = _jsonable(hasil)
    print("")
    print("========== HASIL PROGRESSION ACK ==========")
    print(f"Bot: {hasil.get('email')}  Login OK: {hasil.get('login_ok')}  "
          f"player_id: {hasil.get('player_id')}")
    print(f"Versions dicoba: {hasil.get('versions_tried')}  "
          f"version_ok: {hasil.get('version_ok')!r}")
    tgt = hasil.get("ack_target") or {}
    if tgt:
        print(f"Target KPI: {tgt.get('kpi_id')!r} "
              f"[{tgt.get('type_name')}] value_before={tgt.get('value_before')} "
              f"min_goal={tgt.get('min_goal')} "
              f"player_level_before={tgt.get('player_level_before')}")
    ack = hasil.get("ack_response") or {}
    if ack and "parse_error" not in ack:
        print(f"Ack -> is_success={ack.get('is_success')} "
              f"player_level={ack.get('player_level')} "
              f"wallet={ack.get('wallet')} reward={ack.get('reward')}")
    post = hasil.get("post_check") or {}
    if post:
        print(f"Pasca-ack: value_after={post.get('value_after')} "
              f"player_level_after={post.get('player_level_after')} "
              f"changed={post.get('changed')}")
    print(f"VERDICT: {hasil.get('verdict') or 'NO_INIT'}")
    if hasil.get("error"):
        print(f"Error: {hasil.get('error')}")
    print("==========================================")
    print("PROGRESSION_ACK_JSON:" + json.dumps(hasil, separators=(",", ":")))
    return 0 if hasil.get("login_ok") and hasil.get("verdict") else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
