#!/usr/bin/env python3
"""Google Drive -> Quark(夸克网盘) 同步，GitHub Actions runner 内运行（v4 流式）。

边扫边传：目录遍历是生产者，发现一个文件就入队，传输工人立刻开传；
进度 Issue 实时显示正在传的文件名。幂等（同名同大小跳过），可无限重跑续传。
读端：Google 服务账号（只读）。写端：本机 Alist 夸克驱动（Cookie 来自 Secret）。
"""
import json, os, queue, secrets, ssl, subprocess, sys, threading, time
import http.client
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

QUARK_COOKIE = os.environ["QUARK_COOKIE"]
SA_JSON = os.environ["GOOGLE_SA_JSON"]
DRIVE_ROOT = os.environ.get("DRIVE_ROOT_ID", "1ZtIeCOD69NZ8F_8XECr_dnz6c1z8NjUk")
BUDGET_MIN = int(os.environ.get("BUDGET_MINUTES", "300"))
INCLUDE_FILM = os.environ.get("INCLUDE_FILM", "false") == "true"
GH_REPO = os.environ.get("GITHUB_REPOSITORY", "")
GH_TOKEN = os.environ.get("GH_TOKEN", "")
QROOT = "/HellGrind"
STAGE = "/tmp/stage"
TRANSFER_WORKERS = 4
WALK_WORKERS = 6
T0 = time.time()

STATE = {"phase": "启动", "scanned": 0, "scan_done": False, "total": 0,
         "total_bytes": 0, "ok": 0, "skip": 0, "fail": 0, "bytes": 0,
         "done_bytes_all": 0, "started": T0, "current": {}}
_LOCK = threading.Lock()


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, flush=True)


def budget_left():
    return BUDGET_MIN * 60 - (time.time() - T0)


# ---------------- 进度上报（Issue 实时页） ----------------
def _bar(done, total, width=24):
    if total <= 0:
        return "░" * width
    fill = int(width * done / total)
    return "█" * fill + "░" * (width - fill)


def progress_body():
    import datetime
    s = STATE
    el = time.time() - s["started"]
    speed = s["done_bytes_all"] / el if el > 0 else 0
    done_n = s["ok"] + s["skip"]
    cur = sorted(s["current"].values())
    cur_txt = "\n".join(f"- `{os.path.basename(c)}`" for c in cur) or "- （无）"
    head = (f"## 📊 Drive → 夸克同步进度（自动更新）\n\n"
            f"| 指标 | 值 |\n| --- | --- |\n"
            f"| 阶段 | {s['phase']} |\n"
            f"| 已发现文件 | {s['scanned']:,} |\n"
            f"| 已处理 | {done_n:,}（新传 {s['ok']:,}、已存在跳过 {s['skip']:,}、失败 {s['fail']}） |\n"
            f"| 已传数据 | {s['done_bytes_all']/1e9:.2f} GB（其中新传 {s['bytes']/1e9:.2f} GB） |\n"
            f"| 平均速度 | {speed/1e6:.2f} MB/s |\n"
            f"| 已运行 | {el/60:.0f} 分钟 |\n")
    if s["scan_done"] and s["total"]:
        pctb = 100.0 * s["done_bytes_all"] / s["total_bytes"] if s["total_bytes"] else 0
        eta = (s["total_bytes"] - s["done_bytes_all"]) / speed if speed > 0 else 0
        head += (f"| 总进度 | {_bar(done_n, s['total'])} {100.0*done_n/s['total']:.1f}%"
                 f"（数据量 {pctb:.1f}%） |\n"
                 f"| 预计剩余 | {eta/60:.0f} 分钟 |\n")
    return (head + f"\n**正在传：**\n{cur_txt}\n\n"
            f"_最后更新：{datetime.datetime.now().strftime('%H:%M:%S')}_")


class ProgressReporter:
    def __init__(self):
        self.issue = None
        self.stop = False
        if not (GH_REPO and GH_TOKEN):
            return
        self.h = {"Authorization": f"Bearer {GH_TOKEN}",
                  "Accept": "application/vnd.github+json"}
        try:
            r = requests.get(f"https://api.github.com/repos/{GH_REPO}/issues",
                             headers=self.h, params={"state": "open"}, timeout=30)
            for it in r.json():
                if it["title"].startswith("📊 同步进度"):
                    self.issue = it["number"]
                    break
            if not self.issue:
                r = requests.post(
                    f"https://api.github.com/repos/{GH_REPO}/issues",
                    headers=self.h, timeout=30,
                    json={"title": "📊 同步进度（自动更新）", "body": progress_body()})
                self.issue = r.json()["number"]
            log("progress issue:", self.issue)
        except Exception as e:
            log("progress issue setup failed:", str(e)[:120])

    def run(self):
        if not self.issue:
            return
        while not self.stop:
            try:
                requests.patch(
                    f"https://api.github.com/repos/{GH_REPO}/issues/{self.issue}",
                    headers=self.h, json={"body": progress_body()}, timeout=30)
            except Exception:
                pass
            for _ in range(6):
                if self.stop:
                    break
                time.sleep(10)
        try:
            requests.patch(
                f"https://api.github.com/repos/{GH_REPO}/issues/{self.issue}",
                headers=self.h, json={"body": progress_body()}, timeout=30)
        except Exception:
            pass


# ---------------- Alist ----------------
class Alist:
    def __init__(self):
        self.pw = "hg-" + secrets.token_hex(5)
        self.token = None
        self._qcache = {}
        self._qlock = threading.Lock()

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

    def quark_map(self, qdir):
        with self._qlock:
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
        with self._qlock:
            self._qcache[qdir] = out
        return out

    _dirs_done = set()

    def ensure_dir(self, qdir):
        """逐级建目录（已建过的跳过）；mkdir 失败不抛，交给 put 的结果说话。"""
        parts = [p for p in qdir.split("/") if p]
        cur = ""
        for p in parts:
            cur += "/" + p
            with self._qlock:
                if cur in self._dirs_done:
                    continue
                self._dirs_done.add(cur)
            try:
                self.api("/api/fs/mkdir", {"path": cur})
            except Exception:
                pass

    def put(self, local, qpath, size):
        import urllib.parse
        with open(local, "rb") as fh:
            resp = self.api("/api/fs/put", raw=fh,
                            headers_extra={
                                "File-Path": urllib.parse.quote(qpath),
                                "Content-Length": str(size)},
                            timeout=900)
        if resp.get("code") != 200:
            raise RuntimeError(f"put failed {qpath}: {resp}")


# ---------------- Drive ----------------
_SA_INFO = json.loads(SA_JSON)
_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
_TLS = threading.local()
_QUOTA_UNTIL = [0.0]
_TRANSPORT_ERRORS = (ssl.SSLError, ConnectionError, TimeoutError, OSError,
                     http.client.HTTPException)


def _make_service():
    creds = service_account.Credentials.from_service_account_info(
        _SA_INFO, scopes=_SCOPES)
    return build("drive", "v3", credentials=creds)


def drive_service():
    if getattr(_TLS, "svc", None) is None:
        _TLS.svc = _make_service()
    return _TLS.svc


def _is_quota_error(e):
    body = str(getattr(e, "content", b"") or "") + str(e)
    return (e.resp.status in (403, 429) and
            ("rateLimitExceeded" in body or "userRateLimitExceeded" in body
             or "Quota exceeded" in body or "quotaExceeded" in body))


def execute_retry(req, tries=10):
    """HTTP 429/5xx、配额 403、以及传输层断裂（SSL EOF 等）统一退避重试。"""
    import random
    last = None
    for i in range(tries):
        wait = _QUOTA_UNTIL[0] - time.time()
        if wait > 0:
            time.sleep(min(wait, 130))
        try:
            return req.execute()
        except HttpError as e:
            last = e
            if _is_quota_error(e):
                _QUOTA_UNTIL[0] = time.time() + 65
                time.sleep(min(120, 5 * (2 ** i)) + random.random() * 3)
                continue
            if e.resp.status not in (429, 500, 502, 503):
                raise
            time.sleep(min(60, 2 ** i) + random.random())
        except _TRANSPORT_ERRORS as e:
            last = e
            time.sleep(min(60, 2 ** i) + random.random())
    raise last


def list_folder(fid):
    svc = drive_service()
    out, page = [], None
    while True:
        r = execute_retry(svc.files().list(
            q=f"'{fid}' in parents and trashed=false",
            fields="files(id,name,mimeType,size),nextPageToken",
            pageSize=1000, pageToken=page))
        out.extend(r.get("files", []))
        page = r.get("nextPageToken")
        if not page:
            return out


def walk_producer(root_fid, prefix, item_q, extra_items):
    """BFS 遍历网盘，发现文件立刻入队；失败文件夹最后补扫一遍。"""
    failed = []
    q = deque([(root_fid, prefix)])

    def emit(rel, fid, size):
        item_q.put((rel, fid, size))
        with _LOCK:
            STATE["scanned"] += 1
            STATE["total_bytes"] += size

    with ThreadPoolExecutor(max_workers=WALK_WORKERS) as ex:
        while q:
            batch = []
            while q and len(batch) < 48:
                batch.append(q.popleft())
            futs = {ex.submit(list_folder, fid): (fid, pre) for fid, pre in batch}
            for fut in as_completed(futs):
                fid, pre = futs[fut]
                try:
                    files = fut.result()
                except Exception as e:
                    log("folder list failed, rescan later:", pre or "/", str(e)[:100])
                    failed.append((fid, pre))
                    continue
                for f in files:
                    rel = f"{pre}/{f['name']}" if pre else f["name"]
                    if f["mimeType"] == "application/vnd.google-apps.folder":
                        q.append((f["id"], rel))
                    else:
                        emit(rel, f["id"], int(f.get("size", 0)))
    for fid, pre in failed:
        q.append((fid, pre))
    missed = 0
    while q:
        fid, pre = q.popleft()
        try:
            files = list_folder(fid)
        except Exception as e:
            log("rescan failed:", pre or "/", str(e)[:100])
            missed += 1
            continue
        for f in files:
            rel = f"{pre}/{f['name']}" if pre else f["name"]
            if f["mimeType"] == "application/vnd.google-apps.folder":
                q.append((f["id"], rel))
            else:
                emit(rel, f["id"], int(f.get("size", 0)))
    for it in extra_items:
        emit(*it)
    with _LOCK:
        STATE["scan_done"] = True
        STATE["total"] = STATE["scanned"]
        if missed:
            STATE["phase"] = f"传输中（扫描缺 {missed} 个文件夹，下轮补）"
    log(f"walk done: {STATE['scanned']} items, missed folders: {missed}")
    for _ in range(TRANSFER_WORKERS):
        item_q.put(None)


# ---------------- 传输 ----------------
def transfer_one(alist, item):
    rel, fid, size = item
    qpath = QROOT + "/" + rel
    have = alist.quark_map(os.path.dirname(qpath))
    if have.get(os.path.basename(qpath)) == size:
        with _LOCK:
            STATE["skip"] += 1
            STATE["done_bytes_all"] += size
        return
    local = os.path.join(STAGE, f"{fid}_{os.path.basename(qpath)}")
    try:
        with open(local, "wb") as fh:
            dl = MediaIoBaseDownload(
                fh, drive_service().files().get_media(fileId=fid))
            done = False
            while not done:
                _, done = dl.next_chunk(num_retries=5)
        alist.ensure_dir(os.path.dirname(qpath))
        alist.put(local, qpath, size)
        with _LOCK:
            STATE["ok"] += 1
            STATE["bytes"] += size
            STATE["done_bytes_all"] += size
    finally:
        if os.path.isfile(local):
            os.remove(local)


def transfer_worker(wid, alist, item_q):
    while True:
        if budget_left() < 120:
            log(f"worker {wid}: budget reached, exit")
            return
        try:
            item = item_q.get(timeout=30)
        except queue.Empty:
            if STATE["scan_done"]:
                return
            continue
        if item is None:
            return
        with _LOCK:
            STATE["current"][wid] = item[0]
        try:
            transfer_one(alist, item)
        except Exception as e:
            with _LOCK:
                STATE["fail"] += 1
            log("FAIL", item[0], str(e)[:300])
        finally:
            with _LOCK:
                STATE["current"].pop(wid, None)
        if (STATE["ok"] + STATE["skip"]) % 100 == 0 and STATE["ok"] + STATE["skip"] > 0:
            log(f"progress: ok={STATE['ok']} skip={STATE['skip']} "
                f"fail={STATE['fail']} bytes={STATE['done_bytes_all']/1e9:.2f}GB "
                f"budget_left={budget_left()/60:.0f}m")


def main():
    os.makedirs(STAGE, exist_ok=True)
    rep = ProgressReporter()
    th = threading.Thread(target=rep.run, daemon=True)
    th.start()
    alist = Alist()
    alist.start()
    alist.mount_quark()
    svc = _make_service()

    kids = execute_retry(svc.files().list(
        q=f"'{DRIVE_ROOT}' in parents and name='media' and trashed=false",
        fields="files(id)")).get("files", [])
    if not kids:
        raise RuntimeError("media/ not found under DRIVE_ROOT (check SA sharing)")
    extra = []
    meta = execute_retry(svc.files().list(
        q=f"'{DRIVE_ROOT}' in parents and name='hell_grind_metadata.zip' and trashed=false",
        fields="files(id,name,size)")).get("files", [])
    for f in meta:
        extra.append((f["name"], f["id"], int(f.get("size", 0))))
    if INCLUDE_FILM:
        films = execute_retry(svc.files().list(
            q=f"'{DRIVE_ROOT}' in parents and name contains '.mov' and trashed=false",
            fields="files(id,name,size)")).get("files", [])
        for f in films:
            extra.append((f["name"], f["id"], int(f.get("size", 0))))

    STATE["phase"] = "传输中（边扫边传）"
    item_q = queue.Queue(maxsize=1000)
    prod = threading.Thread(target=walk_producer,
                            args=(kids[0]["id"], "media", item_q, extra),
                            daemon=True)
    prod.start()
    with ThreadPoolExecutor(max_workers=TRANSFER_WORKERS) as ex:
        futs = [ex.submit(transfer_worker, w, alist, item_q)
                for w in range(TRANSFER_WORKERS)]
        for f in futs:
            f.result()
    with _LOCK:
        STATE["phase"] = "本轮结束（重跑 workflow 续传剩余部分）"
    log(f"DONE ok={STATE['ok']} skip={STATE['skip']} fail={STATE['fail']} "
        f"bytes={STATE['done_bytes_all']/1e9:.2f}GB scanned={STATE['scanned']}")
    rep.stop = True
    th.join(timeout=40)
    return 0


if __name__ == "__main__":
    sys.exit(main())
