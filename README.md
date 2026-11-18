# 边缘遥测 LSM 合并工坊

北岭边缘计算小组的有序键值写入与合并演练：记录写入批次、配置层级策略、后台执行刷盘与合并，用最后写入字典 Oracle 核查读结果，防止墓碑误删导致旧值复活。

**这是 Python 确定性教学 LSM 模型。MySQL 8.0 仅保存业务元数据，不是对 MySQL/InnoDB 存储内核的实现或性能测试。** 页面与导出结果均保留此边界。

## 场景与模块

北岭边缘计算小组有 7 人，负责低内存遥测站点的存储方案验证。过去在电子表格记录写入序列、手工画 SSTable、用本地脚本核对合并结果，三个痛点是配置与操作序列混在一起、删除键的历史难复查、读写放大缺少一致口径。上游为站点模拟写入序列；本站替代输入归档、配置组合与可重现演练；下游是边缘存储评审人与值班交接。

- 写入批次：有序 put/delete，支持重复键、墓碑、完整操作预览、输入校验。
- 层级配置：memtable 不同键容量、SSTable 合并触发数、层级上限、最终完整合并开关。
- 合并队列：创建试验时持久化输入及配置快照，Redis 分发，独立 worker 执行并记录状态。
- 读放大证据：显示合并前后层级、读写放大、真实存活键、Oracle 摘要、刷盘/合并轨迹、JSON 导出。
- 管理员审计：记录写操作账号、路由与 HTTP 结果，包含权限拒绝。
- 持久化演练：从批次建立键值库，按组原子多键覆盖/删键，生成检查点、模拟掉电并从磁盘恢复；验证成功组保留、未完成组整组不生效、重试幂等与损坏停止。

| 角色 | 页面与权限 | 典型动作 |
|---|---|---|
| admin 管理员 | 全业务页面与审计 | 登记批次/配置、执行、导出、查看审计 |
| operator 操作员 | 五业务页面，不可看管理员审计 | 新建输入与配置、提交 worker、建立演练并执行恢复、检查与下载结果 |
| viewer 观察员 | 五业务页面只读与导出 | 阅读层级、Oracle 证据与演练恢复报告，写请求在 API 层403 |

## 启动与账号

```bash
cp .env.example .env
docker compose config --quiet
docker compose up -d --build
# 默认启动自动执行；手动重跑不覆盖已存在的输入、配置、账号
docker compose exec -T backend python -m scripts.seed_demo
```

[前端入口](http://localhost:18245) · [后端接口文档](http://localhost:19245/docs)，数据库映射端口17745。

三个本地演示账号是 `admin@beiling.test` / `operator@beiling.test` / `viewer@beiling.test`，统一密码 `EdgeStore!17`。`.env.example` 提供固定本地数据库与JWT演示凭据，上线前替换。

```bash
# 只清理本项目与其演示卷
docker compose down -v --remove-orphans
```

## 09:00—10:00 的工作剧本

1. 09:00 operator 登录，打开“北岭站点-早班写入”：12条操作，8个不同键。
2. 09:05 查看 sensor-01 的18.2→18.8→19.0覆盖记录，sensor-02和sensor-03有删除墓碑。
3. 09:10 新建 `shift-20261117` 输入，或复用“旧值复活回归集”。
4. 09:15 选择“小内存-完整合并”：memtable键容量2，表触发数2，3层，最后完整合并。
5. 09:20 创建试验并提交worker，在队列刷新至“Oracle一致”。
6. 09:25 打开读放大证据，确认早班输入最终6个存活键，sensor-02/03未复活。
7. 09:30 下载 `lsm-trial-<id>.json`，包含操作/配置快照、层级、轨迹与校验和。
8. 09:40 viewer复核，创建试验被403拒绝；10:00 admin检查操作审计。
9. 09:50 打开“05 持久化演练”，新建演练并恢复至可写入，依次写入 g1–g4；生成检查点后模拟掉电，恢复核对四组键值，再以同组号不同内容重试确认 409 拒绝。

Seed另含“旧值复活回归集”“全部删除边界集”和带 group 字段的“掉电恢复演练批次”；配置另含“保留层级观测”和“宽缓冲-完整合并”。账户、批次、配置以自然键查缺插入，重复seed不改变已存在字段。

## 核心算法与指标口径

输入为有序操作数组和参数配置：

- 操作数1–2000；键长1–64；put的value必须是1个不超过200字符的字符串（允许空字符串）；delete的value忽略。
- memtable_limit范围2–64，按不同键数量计容量；l0_trigger范围2–8；max_levels范围2–6。
- 每个用户操作分配单调序号。memtable同键覆盖；达到容量后按键排序刷成L0 SSTable。末尾不足容量也刷盘。
- 某层表数达到触发值时，合并该层所有表到下一层；相同键按最高序号保留记录。最深层在原层合并。**层级合并始终保留墓碑**，不会提早清理。
- 若打开最终完整合并，收集全部层级所有表，按序号合并完整历史后才移除墓碑，存放在最深层。不开启时保留中间层级与墓碑。
- 点查遍历范围覆盖键的SSTable，在候选记录中选择最高序号；带墓碑则不返回。固定探测集合为全部输入键加一个不存在的探针键。没有Bloom过滤器模型。
- 读放大 = 固定点查集合的范围命中SSTable读取总数 / 探测键数；合并前后使用相同集合。写放大 = flush/各次compaction真实输出条目总数 / 用户操作数，**不是物理字节或磁盘I/O**。
- Python另从原始操作顺序构造最后写入字典Oracle；合并前、合并后均必须与Oracle一致，否则试验failed。结果与Oracle各自对有序JSON求SHA-256；同输入/配置结果确定，无随机时钟项。

输出包括真实计算的层级表数/键范围/墓碑数、每步刷盘合并轨迹、输入键数、存活键、写出条目、读写放大、Oracle及实际键值、摘要。空输入和超限在提交时422，引用不存在404，重名409，排队/运行总数超过20时429；worker失败保存error和failed状态。

## 持久化恢复演练

独立的文件级演练目录 `/artifacts/drills/<id>/`，MySQL 只存状态、计数与最近报告。页面在“05 持久化演练”，新演练为“待恢复”，执行恢复后才进入“可写入”。

- 原子组：一条组记录 = 组号 + LSN + 组内容摘要 + 有序 put/delete。整组先编码为 `magic + 长度 + crc32 + 载荷` 再 `fsync`，成功应答即整组落盘。组内覆盖与删键按顺序一次生效。
- 幂等：同组号相同内容的重试返回“未重复写入”（同一 LSN，不再追加）；同组号不同内容整组 409 拒绝，不改动任何已存键值。
- 掉电注入：`before_fsync` 只写半条且不应答，恢复时整组丢弃；`after_fsync` 记录已落盘，恢复时整组保留；另可单独“模拟掉电”，未进入检查点的正常写入也能重放。
- 检查点：先写 `checkpoint.json.tmp` 并 `fsync`，再原子 `rename` 为 `checkpoint.json`，最后才回收日志。快照在状态锁内复制，文件写入不持锁，因此检查点期间并发写入照常应答；回收只删除“整段所有记录 LSN ≤ 检查点 LSN”的旧段，新写入所在段不会被误删。中断注入只留下半个 tmp，旧检查点与全部日志都保留。
- 恢复规则：只接受完整且校验和正确的检查点；`segment.seal` 记录最高日志段与该段最后写入的 LSN，整段丢失且其中有已应答写入时同样报缺口；段内记录按 LSN 从检查点+1 严格递增。最后一段末尾的半条记录可丢弃后继续；标记/CRC/长度损坏、缺段、尾部整段丢失、中段截断或序号缺口都会停止恢复，不截断、不删除任何文件，并回报 `error_code`、偏移与现场文件清单。
- 并发：`DurableStore` 每组追加与状态更新在同一把锁内串行；检查点快照与回收分别短暂持锁，不与追加长时间互斥。测试覆盖 4–6 个并发写线程与检查点交叉执行后重启一致性。
- 状态：`pending_recovery → writable`，恢复失败进入 `stopped`（保留现场，不接受写入）；再执行恢复成功可回到 `writable`。GET 只读磁盘现场，不截断、不发布任何文件。

`backend/tests/` 使用 SQLite 与临时目录覆盖落盘前后掉电、检查点发布中断、中段损坏、检查点与并发写入交叉、页面对应 API 流程；原 LSM 试验页面与接口保持不变。运行方式：在 `backend/` 下执行 `python -m unittest discover -s tests`（仅需 FastAPI/SQLAlchemy/httpx，不连接 MySQL 或 Redis）。

## 技术栈与代码组织

| 服务 | 实现与作用 |
|---|---|
| frontend | 原生HTML/CSS/JS ES模块，Nginx1.27，土橙材料样本册视觉 |
| backend | Python3.11、FastAPI、SQLAlchemy、PyMySQL、JWT与PBKDF2密码，持久化演练的文件存储与恢复 |
| worker | 独立Python进程，Redis BLPOP，LSM演算，数据库恢复队列，心跳健康检查 |
| db | MySQL8.0，持久化3个业务实体、演练状态、用户与审计 |
| redis | Redis7，异步队列与持久卷 |

`backend/src/`分为config/models/schemas/services/controllers/routes/middlewares/utils/constants；`backend/scripts/seed_demo.py`种子与初始化；`backend/migrations/README.md`为schema v2说明。3实体dataset/config/trial各自贯穿model/schema/service/route、前端api/store/page；持久化演练drill同样贯穿model/schema/service/route与前端api/store/page，文件模型在`services/durable_store.py`。试验结果保存在Trial JSON字段中，详情页为独立业务页面。

`frontend/src/`包含api/stores/types/components/common/hooks/pages/router/utils/constants，表格/状态/页头跨页共享，HTML转义与反馈工具复用。角色/任务状态/操作类型枚举在`backend/src/constants/enums.py`和`frontend/src/constants/enums.js`。模型默认任务状态使用后端枚举，前端状态组件使用前端枚举。

Docker恰好5服务，全部healthcheck；backend依赖db/redis healthy，worker还等待backend完成seed；frontend等待backend healthy。命名卷保存MySQL、Redis及worker证据。Nginx `/api/`剥前缀转发后端。

环境变量：COMPOSE_PROJECT_NAME、FRONTEND_PORT、BACKEND_PORT、DB_PORT、MYSQL_PASSWORD、MYSQL_ROOT_PASSWORD、JWT_SECRET、SEED_DEMO；容器连接还配置DATABASE_URL/REDIS_URL，演练目录由`DRILL_ROOT`（默认`/artifacts/drills`）指定。端口可在.env整体调整避免冲突。

本地开发推荐修改代码后`docker compose up -d --build`。也可用虚拟环境安装backend/requirements.txt，启动Compose db/redis，设置可达DATABASE_URL/REDIS_URL/JWT_SECRET，进入backend执行`uvicorn src.main:app --reload`；worker须有可写`/artifacts`并执行`python -m src.worker`。

## 已知边界

- LSM 合并演算为内存教学模型，忽略磁盘块、压缩、Bloom过滤器、并行compaction与真实故障恢复；MySQL负责保存该演练的业务输入和输出。独立的持久化演练（WAL+检查点）把日志与检查点写入 `/artifacts/drills/<id>/`，MySQL 只保存演练状态与最近报告。
- 前端创建独立批次/配置，不提供原记录编辑删除；新版本新建记录，试验总是固定输入快照。
- 最终全历史合并允许去掉墓碑；没有模拟跨副本历史/快照读，因此不能将该规则直接用于生产数据库。
- 单worker与按钮刷新；不提供多worker分布式锁、SSE。持久化演练仅支持单 backend 进程访问同一 artifacts 目录：文件级锁在进程内生效，不模拟多节点共享盘竞争。
- 持久化演练是教学用 WAL/检查点模型：每组一条长度+CRC 记录，fsync 后才应答；记录级 CRC 不覆盖检查点索引之外的外部排序，不模拟磁盘扇区、写缓存、文件系统 journal 与真实掉电时序。
- Web双字体来自Google Fonts，离线回退到serif/monospace。

MIT License，见LICENSE。实际测试与截图位于批次的`test-artifacts/gb-4106/`，源码内不保存测试证据。
