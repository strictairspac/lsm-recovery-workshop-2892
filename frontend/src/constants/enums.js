export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};export const STATUSES={queued:'排队中',running:'合并中',passed:'Oracle 一致',failed:'执行失败'};export const OPERATIONS={put:'写入',delete:'墓碑删除'};
export const DRILL_STATUSES={pending_recovery:'待恢复',writable:'可写入',stopped:'已停止 · 保留现场'};
export const OUTCOMES={committed:'整组已落盘',duplicate:'同内容重试 · 未重复写入',lost_before_fsync:'fsync 前掉电 · 整组失效',durable_after_fsync:'fsync 后掉电 · 整组保留',published:'检查点已发布',checkpoint_interrupted:'检查点发布中断'};
