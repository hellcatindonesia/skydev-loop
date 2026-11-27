#!/usr/bin/env python3
"""Diag READ-ONLY: probe jalur unduhan katalog toko (game-store-data) lewat HTTPS.

Latar (dari dump_3, deklarasi saja — bodi method kosong):
- GameStoreDataUrlProvider: BaseUrl https://game-store-data.aftvrsys.com,
  FallbackBaseUrl https://game-store-data-v2.aftvrsys.com; BuildUrl(relativePath)
  hanya menggabung base + path, TANPA header auth apa pun di tumpukan unduhan
  (AssetRequestStoreFileLoader / StoreJsonLoader / GameStoreDataRequestFallback).
- Manifest: SectionDeliveryMap.FileName = "compatibility_version.json".
- File .sf di HP Bartes adalah CACHE lokal (LocalDataStoreJsonCache,
  CacheFileExtension ".sf", nama file = content hash dari header
  x-amz-meta-md5-hash); freshness dicek via RequestContentHash (HEAD).
- StoreJsonLocations: PrimaryUrl(storeId, variant, fileName),
  DefaultVariant "v1", LatestFileName "latest".

Probe ini menguji matriks: kandidat relative path x {host primer, fallback}
x {anonim, Authorization: Bearer <userToken JWT hasil login FASTER>}.
TIDAK mengubah apa pun di akun; TIDAK pernah mencetak token.
Jawaban yang dicari: path mana (bila ada) yang mengembalikan 200, dan apakah
header Bearer mengubah 403 menjadi 200 (bukti akses butuh login).
"""
import argparse
import asyncio
import json
import sys

import lookup_uuid as L


HOSTS = [
    ("primer", "https://game-store-data.aftvrsys.com"),
    ("fallback", "https://game-store-data-v2.aftvrsys.com"),
]

PATHS = [
    # kandidat manifest
    "compatibility_version.json",
    "v1/compatibility_version.json",
    "default/compatibility_version.json",
    "default/v1/compatibility_version.json",
    "protocol/compatibility_version.json",
    "1_94_0/compatibility_version.json",
    "1.94.0/compatibility_version.json",
    "store/compatibility_version.json",
    "sections/compatibility_version.json",
    "unified/compatibility_version.json",
    # kandidat JSON toko (featured / v1 / latest + publicationId 14710)
    "featured/v1/latest",
    "v1/featured/latest",
    "featured/latest",
    "featured/v1/latest.json",
    "v1/featured/latest.json",
    "featured/latest.json",
    "14710.json",
    "14710",
    "latest",
]

INTEREST_HEADERS = ("content-type", "content-length", "x-amz-meta-md5-hash",
                     "x-amz-request-id", "x-cache", "server")


def _snip(body: bytes, n: int = 250) -> str:
    try:
        txt = body[:n].decode("utf-8", "replace")
    except Exception:
        txt = repr(body[:n])
    return txt.replace("\n", "\\n").replace("\r", "")


async def _fetch(session, url, headers):
    try:
        r = await session.get(url, headers=headers, timeout=15)
        body = r.content or b""
        hdrs = {k.lower(): v for k, v in (r.headers or {}).items()}
        return {"status": r.status_code,
                "headers": {k: hdrs.get(k) for k in INTEREST_HEADERS},
                "length": len(body), "snip": _snip(body)}
    except Exception as exc:  # noqa: BLE001 - catat sebagai hasil, bukan crash
        return {"status": f"ERR:{type(exc).__name__}", "headers": {},
                "length": 0, "snip": str(exc)[:120]}


async def main_async(args):
    hasil = {"email": args.email, "login_ok": False, "attempts": []}
    L.apply_panel_versions()
    async with L.AsyncSession(impersonate="chrome110", verify=False) as session:
        jwt = await L.login_only(args.email, args.password,
                                 args.device_id or "9f31c2d4-probe-device",
                                 session)
        if not jwt:
            print("[!] login gagal.")
            print("STOREDATA_JSON:" + json.dumps(hasil))
            return
        hasil["login_ok"] = True
        for hname, base in HOSTS:
            for path in PATHS:
                url = base + "/" + path
                for mode, hdrs in (("anon", {}),
                                   ("bearer", {"Authorization": "Bearer " + jwt})):
                    res = await _fetch(session, url, hdrs)
                    att = {"host": hname, "path": path, "mode": mode, **res}
                    hasil["attempts"].append(att)
                    mark = "  <<< 200" if res["status"] == 200 else ""
                    print(f"[{hname}][{mode}] GET /{path} -> {res['status']} "
                          f"len={res['length']} {res['snip'][:120]}{mark}")
    print("STOREDATA_JSON:" + json.dumps(hasil))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--device-id", default="")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
