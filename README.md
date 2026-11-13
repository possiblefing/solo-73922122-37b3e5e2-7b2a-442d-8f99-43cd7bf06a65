# 机构库 → 馆藏目录采集

把机构库 OAI-PMH（`oai_dc`）的新增、修改、撤回同步为一份可导入离线发现系统的
馆藏目录 XML。FastAPI + httpx + SQLite，附本地演示 OAI 源，Docker Compose 一条命令启动。

## 快速启动

```bash
docker compose up --build -d
```

* 采集页：<http://localhost:8000/>（采集、下载、进度、错误、续采）
* 本地演示 OAI-PMH 源：<http://localhost:8001/>（修改/撤回/凭据失效操作面板）
* 源端点：<http://localhost:8001/oai>（`ListRecords&metadataPrefix=oai_dc`）

```bash
docker compose logs -f catalog-web    # 观察采集中/发布失败等日志
docker compose down                   # 停止（数据保留在命名卷）
docker compose down -v                # 连数据一起清空
```

页面操作顺序（也可直接看页面底部"演示脚本"）：

1. **首次全量**：选本轮截止日（默认今天）→ 开始采集。无水位时 `ListRecords`
   不带 `from`，收齐后生成目录并发布，水位 = 截止日。
2. **同日更新 / 撤回**：在 8001 演示源修改标题或撤回一条记录（源端 datestamp
   刷新为今天）→ 再次采集。窗口为 `from=上次成功截止日`（含当天），按
   `identifier` 更新或删除。未出现的记录不动，**不以缺席推断删除**。
3. **分页凭据失效**：演示源上点“下一次凭据请求失效”→ 采集端起续页时收到
   `badResumptionToken`，立即回到原窗口第一页重采，水位不推进、已入库记录不丢。
   页面的“凭据重置 N 次”和橙色提示会记录此事件。
4. **发布前后退出**：采集页“发布故障演练”选 `crash_after_publish` 或
   `crash_before_commit`，再跑一轮 → 进程退出。执行
   `docker compose restart catalog-web` 后，页面水位与下载文件仍是上一版完整结果；
   `crash_after_publish` 的轮次显示“发布失败·可重试”，点重试即可发布新版本，
   无需重采。
5. **写锁演练（SQLite 被其他连接占用）**：采集页“写锁演练”输入秒数（≥45）
   → 开始持锁，另一条连接对同一 SQLite 持有写锁。期间：
   * 首页、`/healthz`、旧目录下载照常，1 秒内返回已提交内容；
   * “开始采集 / 继续采集”的写库忙等 30 秒后超时，该页整体回滚——分页进度
     与发布水位都不推进，轮次转为“已暂停·可续采”（`database_locked`）；
   * 锁释放（到期或点“立即释放”）后点“继续采集”即可从原窗口续采，
     重读不会产生重复记录。
   提示：以 `MAX_PAGES_PER_CALL=1` 或 `PAGE_DELAY_MS=2000` 启动，更容易在
   采集途中施加写锁。
6. **写库等待中停机**：持锁期间执行 `docker compose stop catalog-web`
   （宽限 50 秒）：服务会等写库提交/回滚结束、关闭连接后才退出；
   `docker compose start catalog-web` 后该轮变为“已暂停·可续采”，
   点“继续采集”即可。

## 行为设计

| 需求 | 实现 |
| --- | --- |
| 固定地址读取 oai_dc | `OAI_ENDPOINT` 固定，`ListRecords` + `resumptionToken` |
| 源按日标记变更、删除标记永久保留 | `records.identifier` 唯一，`deleted=1` 只置位不删除；重采/再版才改 |
| 首次全量，此后从上次成功截止日含当天重采 | `runs.mode=full/incremental`；`window_from=watermark`，`from/until` 均含当天 |
| 网页选择本轮截止日 | 首页日期表单 → `runs.window_until` |
| 按 identifier 更新或删除 | 每页记录 UPSERT；删除头写 tombstone |
| 不以缺席推断删除 | 本轮只处理返回记录，从不扫描本地记录做删除 |
| 同期只采一轮 | `runs` 上的部分唯一索引 + 应用层检查；未解决的轮次挡住新轮次 |
| 保存分页进度、断流/重启可续采 | 每页提交记录与 `resumption_token`（SQLite 事务）；启动把 `running` 转 `paused` |
| badResumptionToken 重采原窗口、不推进水位 | 收到该错误即重置页码/token 从原窗口第一页重采；水位只在发布时写 |
| 收齐后生成含全部有效记录的 XML | `records where deleted=0` 全量入目录，`<catalog watermark recordCount>` |
| 文件与水位一起发布 | 同一 `publications` 行保存文件路径+水位+sha256；页面/下载都只读该行 |
| 生成或发布失败保留旧目录 | 先构造 XML、写临时文件、原子 `os.replace`，最后落库；任一步失败旧行不动 |
| 发布中退出后重启，内容与水位对应同一完整结果 | 唯一文件名+原子改名；未落库的孤儿文件启动时清理；`publishing` 按有无发布记录补完成/转失败 |
| 采集中旧目录照常下载 | 下载只读最新 `publications` 行，与采集事务隔离 |
| 页面显示进度、错误、可续采 | 后台任务 + 2 秒自动刷新；进度条、错误码、继续采集/重试发布/放弃本轮 |
| 外部连接持写锁时读不卡 | 读走独立只读连接（WAL 快照，1 秒超时）；写连接单独串行，读写互不排队 |
| 写库保留 30 秒忙等待 | 写连接 `busy_timeout=30000`；写操作放工作线程，不堵事件循环 |
| 写库超时不推进进度/水位 | 一页记录+分页进度同一事务，超时整页回滚；水位只随发布记录提交 |
| 超时后显示错误、可续采、不重复 | 轮次转 `paused(database_locked)`；解锁后网页续采原窗口，UPSERT 幂等 |
| 写库结束后才接纳续采 | 单轮 asyncio 互斥锁 + 每轮单任务；新请求等当前写库提交/回滚后才执行 |
| 停机不斩断写库 | 停机先通知采集循环收尾，等任务写库结束；`db.close()` 拿走写锁才关连接 |
| 重启仍可续采 | 启动恢复把遗留 `running` 转 `paused`（保留 token），`publishing` 按发布记录补记 |

## 测试

```bash
python3 -m pytest tests -q        # 或 docker compose exec catalog-web python -m pytest tests -q
```

覆盖：全量→同日增量、按 identifier 更新、撤回永久保留、缺席不删除、分页进度重启续采、
`badResumptionToken` 重采且水位不动、生成/落库失败保留旧目录、发布前后退出后的
“水位 ↔ 文件”一致性、目录哈希与记录数；以及写锁场景：持锁期间读 1 秒内返回、
写库超时整页回滚、`database_locked` 暂停后可续采且记录不重复、停机等待写库结束、
重启恢复续采。

## 文件结构

```
app/         采集网页（main.py 路由、harvester.py 编排、oai.py 解析、catalog.py 发布、db.py SQLite）
source/      本地演示 OAI-PMH 源（同样 FastAPI + SQLite）
tests/       端到端行为测试（真实 ASGI 源，无外部依赖）
scripts/     demo_flow.py：无浏览器跑通演示（容器内 python -m scripts.demo_flow）
Dockerfile / docker-compose.yml
```

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `OAI_ENDPOINT` | `http://oai-source:8001/oai` | 固定 OAI-PMH 地址 |
| `CATALOG_DATA_DIR` | `/srv/data`（compose） | SQLite 与已发布目录 |
| `MAX_PAGES_PER_CALL` | 空（不限） | 单次后台采集页数上限，便于演示多次续采 |
| `PAGE_DELAY_MS` | 空（0） | 每页之间的人为间隔（毫秒），便于在采集途中施加演示写锁 |
| `DB_BUSY_TIMEOUT_MS` | `30000` | 写库忙等待毫秒数（保留的 30 秒忙等待；测试可用小值） |
| `PUBLISH_FAILURE_MODE` | `none` | 首次启动时预置一次性发布故障：`generate_error` / `commit_error` / `crash_before_commit` / `crash_after_publish` |
| `SOURCE_DB_PATH` | `/srv/data/source.db` | 演示源 SQLite（`oai-source` 服务使用） |

演示源数据在 `source-data` 卷；采集端数据在 `catalog-data` 卷。
`catalog-web` 配置了 `stop_grace_period: 50s`：覆盖“写库忙等 30 秒 + 状态落库 +
关闭连接”，保证停机不斩断写库。
