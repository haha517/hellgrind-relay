# Hell Grind：Google Drive → 夸克网盘中转（GitHub Actions）

把已存入 Google Drive 的 Hell Grind 媒体，经 GitHub Actions 的海外带宽增量同步一份到夸克网盘。
公开仓库 Actions 不限时；每次运行最多约 5 小时（预算可调），到点干净退出，**重跑即续传**（夸克侧同名同大小自动跳过）。

代码里没有任何密钥。两个 Secret 只存在于仓库设置里，不会进日志（脚本从不打印它们）。

## 一次性设置（约 10 分钟）

### 1. 建 Google 服务账号（让 Actions 能只读你的网盘文件夹）

1. 打开 https://console.cloud.google.com/ ，新建一个项目（名字随意，如 `hellgrind-relay`）。
2. 在该项目里启用 **Google Drive API**（"API 和服务" → "启用 API" → 搜 Drive）。
3. "IAM 和管理" → "服务账号" → 创建服务账号（名字随意）→ 完成后点开它 → "密钥" → "添加密钥" → JSON → 下载得到一个 `.json` 文件。
4. 记下服务账号的邮箱（形如 `xxx@hellgrind-relay.iam.gserviceaccount.com`）。
5. 打开谷歌网盘里的文件夹 **Hell Grind 项目数据（Higgsfield 全量抓取）** → 共享 → 把上面的邮箱加为**查看者**。

> 服务账号在这里只有读取权限；它写不了你的网盘，也看不到文件夹以外的东西。

### 2. 取夸克 Cookie

1. 电脑浏览器登录 https://pan.quark.cn/
2. F12 → "网络"标签 → 筛选选"全部"、勾"保留日志" → 刷新页面 → 点任意文件夹
3. 在请求列表里找一个 `drive-pc.quark.cn` 的请求 → "请求标头" → 复制 `cookie:` 后面的整串

> Cookie 有效期数天到数周；失效后同步会在开头报错，换新 Cookie 更新 Secret 即可。

### 3. 把两个 Secret 填进本仓库

仓库页 → Settings → Secrets and variables → Actions → New repository secret，建两个：

| 名称 | 内容 |
| --- | --- |
| `QUARK_COOKIE` | 第 2 步的整串 cookie |
| `GOOGLE_SA_JSON` | 第 1 步下载的 JSON 文件的**全部内容**（原样粘贴） |

## 日常运行

Actions 标签 → **Drive to Quark sync** → Run workflow → 直接 Run。

- `budget_minutes`：默认 300（5 小时）；想短跑验证可填 15。
- `include_film`：默认 false。正片 .mov 是 13.75GB 单文件，夸克非会员上限可能拒绝，需要时再开。
- 进度看运行日志：每批打印 `ok/skip/fail` 和已传 GB；跨网速度（Azure → 夸克）在日志的 GB/分钟里能算出来。
- 跑完一轮后再 Run 一次就接着传剩下的，直到日志显示剩余为 0。

## 安全说明

- 本仓库是公开的：**不要把任何密钥写进代码或提交文件**，只走 Secrets。
- 服务账号 JSON 泄露的风险面 = 别人能只读这一个共享文件夹；真泄露了，去 Google Cloud 删掉那把密钥即可。
- 夸克 Cookie 泄露 = 别人能操作你的夸克网盘；只存在 Secret 里、不要外传。
