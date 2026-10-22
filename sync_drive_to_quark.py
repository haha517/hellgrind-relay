#!/usr/bin/env python3
"""Google Drive -> Quark(夸克网盘) 增量同步，在 GitHub Actions runner 内运行。

读端：Google 服务账号（只读被共享的网盘文件夹）。
写端：本机 Alist 的夸克驱动（Cookie 来自仓库 Secret，绝不打印）。
幂等：夸克侧同名同大小跳过；随时重跑续传。预算到点干净退出（exit 0）。
"""
import io, json, os, secrets, subprocess, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import random

import requests
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

QUARK_COOKIE = os.environ["QUARK_COOKIE"]
SA_JSON = os.environ["GOOGLE_SA_JSON"]
DRIVE_ROOT = os.environ.get("DRIVE_ROOT_ID", "1ZtIeCOD69NZ8F_8XECr_dnz6c1z8NjUk")
BUDGET_MIN = int(os.environ.get("BUDGET_MINUTES", "300"))
INCLUDE_FILM = os.environ.get("INCLUDE_FILM", "false") == "true"
QROOT = "/HellGrind"
GH_REPO = os.environ.get("GITHUB_REPOSITORY", "")
GH_TOKEN = os.environ.get("GH_TOKEN", "")
STATE = {"phase": "启动", "scanned": 0, "total": 0, "total_bytes": 0,
         "ok": 0, "skip": 0, "fail": 0, "bytes": 0, "started": time.time()}


def _bar(done, total, width=24):
    if total <= 0:
        return "░" * width
    fill = int(width * done / total)
    return "█" * fill + "░" * (width - fill)


def progress_body():
    s = STATE
    el = time.time() - s["started"]
    speed = s["bytes"] / el if el > 0 else 0
    if s["total"]:
        done_n = s["ok"] + s["skip"]
        pct = 100.0 * done_n / s["total"]
        pctb = 100.0 * s["bytes"] / s["total_bytes"] if s["total_bytes"] else 0
        eta = (s["total_bytes"] - s["bytes"]) / speed if speed > 0 else 0
        core = (f"### {_bar(done_n, s['total'])} {pct:.1f}%（按文件数）\n\n"
                f"| 指标 | 值 |\n| --- | --- |\n"
                f"| 阶段 | {s['phase']} |\n"
                f"| 文件 | {done_n:,} / {s['total']:,}（新传 {s['ok']:,}、已存在跳过 {s['skip']:,}、失败 {s['fail']}） |\n"
                f"| 数据量 | {s['bytes']/1e9:.2f} / {s['total_bytes']/1e9:.2f} GB（{pctb:.1f}%） |\n"
                f"| 平均速度 | {speed/1e6:.2f} MB/s |\n"
                f"| 预计剩余 | {eta/60:.0f} 分钟 |\n"
                f"| 已运行 | {el/60:.0f} 分钟 |\n")
    else:
        core = (f"### 扫描中… 已发现 {s['scanned']:,} 个文件\n\n"
                f"阶段：{s['phase']}（先把网盘文件树完整清点一遍才开始传，"
                f"这是为了算得出总进度）\n\n已运行 {el/60:.0f} 分钟")
    import datetime
    return ("## 📊 Drive → 夸克同步进度（自动更新）\n\n" + core +
            f"\n\n_最后更新：{datetime.datetime.now().strftime('%H:%M:%S')}_")


class ProgressReporter:
    def __init__(self):
        self.issue = None
        self.stop = False
        if not (GH_REPO and GH_TOKEN):
            return
        h = {"Authorization": f"Bearer {GH_TOKEN}",
             "Accept": "application/vnd.github+json"}
        self.h = h
        try:
            r = requests.get(f"https://api.github.com/repos/{GH_REPO}/issues",
                             headers=h, params={"state": "open"}, timeout=30)
            for it in r.json():
                if it["title"].startswith("📊 同步进度"):
                    self.issue = it["number"]
                    break
            if not self.issue:
                r = requests.post(f"https://api.github.com/repos/{GH_REPO}/issues",
                                  headers=h, timeout=30,
                                  json={"title": "📊 同步进度（自动更新）",
                                        "body": progress_body()})
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
STAGE = "/tmp/stage"
WORKERS = 4
T0 = time.time()


def log(*a):
    print(f"[{time.time() - T0:7.1f}s]", *a, flush=True)


_QUOTA_UNTIL = [0.0]


def _is_quota_error(e):
    body = str(getattr(e, "content", b"") or "") + str(e)
    return (e.resp.status in (403, 429) and
            ("rateLimitExceeded" in body or "userRateLimitExceeded" in body
             or "Quota exceeded" in body or "quotaExceeded" in body))


def execute_retry(req, tries=10):
    """Drive API 偶发 429/5xx 与配额 403 时退避重试；配额错误触发全员冷却。"""
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
    raise last


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
                            timeout=900)
        if resp.get("code") != 200:
            raise RuntimeError(f"put failed {qpath}: {resp}")


# ---------------- Drive ----------------
import threading as _th
_TLS = _th.local()


_SA_INFO = json.loads(SA_JSON)
_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def _make_service():
    creds = service_account.Credentials.from_service_account_info(
        _SA_INFO, scopes=_SCOPES)
    return build("drive", "v3", credentials=creds)


def drive_service():
    if getattr(_TLS, "svc", None) is None:
        _TLS.svc = _make_service()
    return _TLS.svc


def _build_service():
    return _make_service()


def drive_walk(svc, folder_id, prefix=""):
    """并行 BFS 遍历：16 线程并发列文件夹（每线程独立 Drive service）。"""
    from collections import deque
    results = []
    lock = _th.Lock()

    def list_folder(fid):
        svc_l = drive_service()
        out = []
        page = None
        while True:
            r = execute_retry(svc_l.files().list(
                q=f"'{fid}' in parents and trashed=false",
                fields="files(id,name,mimeType,size),nextPageToken",
                pageSize=1000, pageToken=page))
            out.extend(r.get("files", []))
            page = r.get("nextPageToken")
            if not page:
                return out

    queue = deque([(folder_id, prefix)])
    failed = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        while queue:
            futs = {}
            batch = []
            while queue and len(batch) < 64:
                batch.append(queue.popleft())
            for fid, pre in batch:
                futs[ex.submit(list_folder, fid)] = (fid, pre)
            for fut in as_completed(futs):
                fid, pre = futs[fut]
                try:
                    files = fut.result()
                except Exception as e:
                    log("folder list failed, queued for rescan:", pre or "/",
                        str(e)[:100])
                    failed.append((fid, pre))
                    continue
                for f in files:
                    rel = f"{pre}/{f['name']}" if pre else f["name"]
                    if f["mimeType"] == "application/vnd.google-apps.folder":
                        queue.append((f["id"], rel))
                    else:
                        with lock:
                            results.append((rel, f["id"],
                                            int(f.get("size", 0))))
                            STATE["scanned"] = len(results)
    for fid, pre in failed:
        queue.append((fid, pre))
    retry_failed = []
    while queue:
        fid, pre = queue.popleft()
        try:
            files = list_folder(fid)
        except Exception as e:
            log("rescan failed:", pre or "/", str(e)[:100])
            retry_failed.append(pre or "/")
            continue
        for f in files:
            rel = f"{pre}/{f['name']}" if pre else f["name"]
            if f["mimeType"] == "application/vnd.google-apps.folder":
                queue.append((f["id"], rel))
            else:
                results.append((rel, f["id"], int(f.get("size", 0))))
                STATE["scanned"] = len(results)
    if retry_failed:
        log("SCAN INCOMPLETE, folders missed:", len(retry_failed))
        STATE["phase"] = "传输中（扫描缺 " + str(len(retry_failed)) + " 个文件夹，下轮补）"
    return results


def main():
    os.makedirs(STAGE, exist_ok=True)
    import threading
    rep = ProgressReporter()
    th = threading.Thread(target=rep.run, daemon=True)
    th.start()
    alist = Alist()
    alist.start()
    alist.mount_quark()
    svc = _build_service()

    kids = execute_retry(svc.files().list(
        q=f"'{DRIVE_ROOT}' in parents and name='media' and trashed=false",
        fields="files(id)")).get("files", [])
    if not kids:
        raise RuntimeError("media/ not found under DRIVE_ROOT (check SA sharing)")
    STATE["phase"] = "扫描网盘文件树"
    items = drive_walk(svc, kids[0]["id"], "media")
    if not STATE["phase"].startswith("传输中"):
        STATE["phase"] = "传输中"
    STATE["total"] = len(items)
    STATE["total_bytes"] = sum(x[2] for x in items)
    if INCLUDE_FILM:
        films = execute_retry(svc.files().list(
            q=f"'{DRIVE_ROOT}' in parents and name contains '.mov' and trashed=false",
            fields="files(id,name,size)")).get("files", [])
        for f in films:
            items.append((f["name"], f["id"], int(f.get("size", 0))))
    meta = execute_retry(svc.files().list(
        q=f"'{DRIVE_ROOT}' in parents and name='hell_grind_metadata.zip' and trashed=false",
        fields="files(id,name,size)")).get("files", [])
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
                dl = MediaIoBaseDownload(
                    fh, drive_service().files().get_media(fileId=fid))
                done = False
                while not done:
                    _, done = dl.next_chunk(num_retries=5)
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
            batch, pending = pending[:20], pending[20:]
            futs = {ex.submit(one, it): it for it in batch}
            for fut in as_completed(futs):
                try:
                    kind, rel, size = fut.result()
                    if kind == "ok":
                        n_ok += 1
                        done_bytes += size
                    elif kind == "skip":
                        n_skip += 1
                        done_bytes += futs[fut][2]
                except Exception as e:
                    n_fail += 1
                    log("FAIL", futs[fut][0], str(e)[:160])
                STATE.update(ok=n_ok, skip=n_skip, fail=n_fail, bytes=done_bytes)
            log(f"progress: ok={n_ok} skip={n_skip} fail={n_fail} "
                f"bytes={done_bytes/1e9:.2f}GB budget_left={budget_left()/60:.0f}m")
    STATE["phase"] = "本轮结束（重跑 workflow 续传剩余部分）"
    STATE.update(ok=n_ok, skip=n_skip, fail=n_fail, bytes=done_bytes)
    log(f"DONE ok={n_ok} skip={n_skip} fail={n_fail} bytes={done_bytes/1e9:.2f}GB")
    rep.stop = True
    th.join(timeout=40)
    return 0


if __name__ == "__main__":
    sys.exit(main())
