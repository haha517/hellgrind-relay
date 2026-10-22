#!/usr/bin/env python3
"""Google Drive -> Quark(夸克网盘) 增量同步，在 GitHub Actions runner 内运行。

读端：Google 服务账号（只读被共享的网盘文件夹）。
写端：本机 Alist 的夸克驱动（Cookie 来自仓库 Secret，绝不打印）。
幂等：夸克侧同名同大小跳过；随时重跑续传。预算到点干净退出（exit 0）。
"""
import io, json, os, secrets, subprocess, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

QUARK_COOKIE = os.environ["QUARK_COOKIE"]
SA_JSON = os.environ["GOOGLE_SA_JSON"]
DRIVE_ROOT = os.environ.get("DRIVE_ROOT_ID", "1ZtIeCOD69NZ8F_8XECr_dnz6c1z8NjUk")
BUDGET_MIN = int(os.environ.get("BUDGET_MINUTES", "300"))
INCLUDE_FILM = os.environ.get("INCLUDE_FILM", "false") == "true"
QROOT = "/HellGrind"
STAGE = "/tmp/stage"
WORKERS = 4
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, flush=True)


def budget_left():
    return BUDGET_MIN * 60 - (time.time() - T0)


# ---------------- Alist ----------------
class Alist:
    def __init__(self):
        self.pw = "hg-" + secrets.token_hex(5)
        self.token = None

    def api(self, path, body=None, raw=None, headers_extra=None, timeout=180):
        url = "http://localhost:5244" + path
        headers = {}
        if self.token:
            headers["Authorization"] = self.token
        if body is not None:
            headers["Content-Type"] = "application/json"
        headers.update(headers_extra or {})
        if raw is not None:
            r = requests.put(url, data=raw, headers=headers, timeout=timeout)
        else:
            r = requests.post(url, json=body, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def start(self):
        subprocess.run(["./alist", "admin", "set", self.pw],
                       cwd="alist-bin", capture_output=True, text=True)
        self.proc = subprocess.Popen(["./alist", "server"], cwd="alist-bin",
                                     stdout=open("alist.log", "w"),
                                     stderr=subprocess.STDOUT)
        for _ in range(40):
            time.sleep(2)
            try:
                resp = self.api("/api/auth/login",
                                {"username": "admin", "password": self.pw})
                self.token = resp["data"]["token"]
                log("alist up, logged in")
                return
            except Exception:
                continue
        raise RuntimeError("alist failed to start; see alist.log")

    def mount_quark(self):
        addition = json.dumps({"cookie": QUARK_COOKIE, "root_folder_id": "0"})
        resp = self.api("/api/admin/storage/create",
                        {"mount_path": "/quark", "driver": "Quark",
                         "addition": addition})
        log("quark storage:", resp.get("message") or resp.get("code"))
        time.sleep(3)
        lst = self.api("/api/fs/list", {"path": "/quark", "page": 1, "per_page": 1})
        if lst.get("code") != 200:
            raise RuntimeError(f"quark list failed (cookie invalid?): {lst}")
        log("quark reachable")

    _qcache = {}

    def quark_map(self, qdir):
        if qdir in self._qcache:
            return self._qcache[qdir]
        out, page = {}, 1
        while True:
            r = self.api("/api/fs/list",
                         {"path": qdir, "page": page, "per_page": 200})
            content = (r.get("data") or {}).get("content") or []
            for c in content:
                out[c["name"]] = int(c.get("size", 0))
            if len(content) < 200:
                break
            page += 1
        self._qcache[qdir] = out
        return out

    def put(self, local, qpath, size):
        with open(local, "rb") as fh:
            resp = self.api("/api/fs/put", raw=fh,
                            headers_extra={
                                "File-Path": urllib.parse.quote(qpath),
                                "Content-Length": str(size)},
                            timeout=3600)
        if resp.get("code") != 200:
            raise RuntimeError(f"put failed {qpath}: {resp}")


# ---------------- Drive ----------------
def drive_service():
    with open("/tmp/sa.json", "w") as f:
        f.write(SA_JSON)
    creds = service_account.Credentials.from_service_account_file(
        "/tmp/sa.json", scopes=["https://www.googleapis.com/auth/drive.readonly"])
    return build("drive", "v3", credentials=creds)


def drive_walk(svc, folder_id, prefix=""):
    page = None
    while True:
        r = svc.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,name,mimeType,size),nextPageToken",
            pageToken=page).execute()
        for f in r.get("files", []):
            rel = f"{prefix}/{f['name']}" if prefix else f["name"]
            if f["mimeType"] == "application/vnd.google-apps.folder":
                yield from drive_walk(svc, f["id"], rel)
            else:
                yield rel, f["id"], int(f.get("size", 0))
        page = r.get("nextPageToken")
        if not page:
            return


def main():
    os.makedirs(STAGE, exist_ok=True)
    alist = Alist()
    alist.start()
    alist.mount_quark()
    svc = drive_service()

    kids = svc.files().list(
        q=f"'{DRIVE_ROOT}' in parents and name='media' and trashed=false",
        fields="files(id)").execute().get("files", [])
    if not kids:
        raise RuntimeError("media/ not found under DRIVE_ROOT (check SA sharing)")
    items = list(drive_walk(svc, kids[0]["id"], "media"))
    if INCLUDE_FILM:
        films = svc.files().list(
            q=f"'{DRIVE_ROOT}' in parents and name contains '.mov' and trashed=false",
            fields="files(id,name,size)").execute().get("files", [])
        for f in films:
            items.append((f["name"], f["id"], int(f.get("size", 0))))
    meta = svc.files().list(
        q=f"'{DRIVE_ROOT}' in parents and name='hell_grind_metadata.zip' and trashed=false",
        fields="files(id,name,size)").execute().get("files", [])
    for f in meta:
        items.append((f["name"], f["id"], int(f.get("size", 0))))
    log(f"drive items: {len(items)}")

    n_ok = n_skip = n_fail = 0
    done_bytes = 0

    def one(item):
        rel, fid, size = item
        qpath = QROOT + "/" + rel
        have = alist.quark_map(os.path.dirname(qpath))
        if have.get(os.path.basename(qpath)) == size:
            return ("skip", rel, 0)
        local = os.path.join(STAGE, f"{fid}_{os.path.basename(qpath)}")
        try:
            with open(local, "wb") as fh:
                dl = MediaIoBaseDownload(fh, svc.files().get_media(fileId=fid))
                done = False
                while not done:
                    _, done = dl.next_chunk()
            alist.put(local, qpath, size)
            return ("ok", rel, size)
        finally:
            if os.path.isfile(local):
                os.remove(local)

    pending = list(items)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        while pending:
            if budget_left() < 300:
                log(f"budget reached; {len(pending)} items left for next run")
                break
            batch, pending = pending[:200], pending[200:]
            futs = {ex.submit(one, it): it for it in batch}
            for fut in as_completed(futs):
                try:
                    kind, rel, size = fut.result()
                    if kind == "ok":
                        n_ok += 1
                        done_bytes += size
                    elif kind == "skip":
                        n_skip += 1
                except Exception as e:
                    n_fail += 1
                    log("FAIL", futs[fut][0], str(e)[:160])
            log(f"progress: ok={n_ok} skip={n_skip} fail={n_fail} "
                f"bytes={done_bytes/1e9:.2f}GB budget_left={budget_left()/60:.0f}m")
    log(f"DONE ok={n_ok} skip={n_skip} fail={n_fail} bytes={done_bytes/1e9:.2f}GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
