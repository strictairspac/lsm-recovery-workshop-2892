import{datasets}from'../stores/dataset.js';import{drills}from'../stores/drill.js';import{create,detail,writeGroup,checkpoint,crash,recover,download}from'../api/drill.js';import{header}from'../components/common/header.js';import{table}from'../components/common/table.js';import{esc,json}from'../utils/html.js';import{submit,notify}from'../hooks/feedback.js';import{session}from'../stores/session.js';import{DRILL_STATUSES,OUTCOMES}from'../constants/enums.js';

const status=s=>'<span class="badge '+esc(s)+'">'+esc(DRILL_STATUSES[s]||s)+'</span>';
const kvTable=kv=>table(['键','值'],Object.entries(kv).map(([k,v])=>['<span class="mono">'+esc(k)+'</span>','<span class="mono">'+esc(v)+'</span>']));
const groupRows=rows=>rows.map(g=>['<span class="mono">'+esc(g.group)+'</span>',String(g.ops.length),'<span class="mono">'+esc(g.ops.map(o=>o.op+':'+o.key+(o.op==='put'?'='+o.value:'')).join(' · '))+'</span>']);

function reasons(report,action){
  const rows=[];
  (report.rejections||[]).forEach(x=>rows.push([esc(x.group_id),'L'+x.lsn,esc(x.reason),esc(x.message)]));
  if(report.error_code)rows.push(['—','—',esc(report.error_code),esc(report.error)]);
  if(action&&action.outcome==='rejected')rows.push([esc(action.group_id||'—'),action.lsn?'L'+action.lsn:'—',esc(action.reason||'REJECTED'),esc(action.message)]);
  return rows;
}

function tailNote(tail){
  if(!tail)return '';
  const where=tail.segment?`${tail.segment} 偏移 ${tail.offset}（${tail.bytes} 字节）`:'尾部';
  return `<p class="helper">尾部半条记录：${esc(where)} · ${esc(tail.action||'丢弃后继续恢复')}</p>`;
}

function recoveryPanel(report,action){
  if(!report)return '<div class="empty">尚未执行恢复。</div>';
  const rows=reasons(report,action);
  return `<div class="metrics"><div><small>检查点 LSN</small><strong>${report.checkpoint_lsn}</strong></div><div><small>最后有效 LSN</small><strong>${report.last_lsn}</strong></div><div><small>重放生效组</small><strong>${(report.applied_groups||[]).length}</strong></div><div><small>同内容跳过 / 拒绝</small><strong>${(report.duplicates_skipped||[]).length} / ${(report.rejections||[]).length}</strong></div></div>
  ${report.message?'<p class="helper">'+esc(report.message)+'</p>':''}
  ${report.preserved?'<p class="error">恢复已停止并保留现场：未删除或截断任何日志与检查点文件。</p>':''}
  ${report.ignored_incomplete_checkpoint?'<p class="helper">已忽略中断发布留下的临时检查点，仅使用上一个完整检查点。</p>':''}
  ${tailNote(report.discarded_tail)}
  ${rows.length?'<h2 style="margin-top:22px">拒绝原因</h2>'+table(['组号','LSN','原因码','说明'],rows):''}
  ${report.kv?`<h2 style="margin-top:22px">恢复后的键值</h2>${kvTable(report.kv)}`:report.partial_kv?`<h2 style="margin-top:22px">中断时的部分键值（未生效）</h2>${kvTable(report.partial_kv)}`:''}`;
}

function actionPanel(x){
  const a=x.last_action||{};
  const label=OUTCOMES[a.outcome]||a.outcome||'—';
  return `<div class="helper">上次动作：${esc(a.action||'—')} · ${esc(label)}<br>${esc(a.message||'')}<br>检查点 LSN ${x.checkpoint_lsn} · 最后 LSN ${x.last_lsn} · ${x.group_count} 组在检查点索引中。</div>`;
}

export async function drillsPage(el){
  const ds=await datasets.load();const list=await drills.load();const selected=Number(location.hash.split('/')[1])||null;const writable=session.get().role!=='viewer';
  if(selected){
    const d=await detail(selected);const report=d.last_report||{};const action=d.last_action||{};const canWrite=writable&&d.status==='writable';
    el.innerHTML=header('05','掉电之后，哪些写入还在','每组多键操作先写长度+CRC记录并 fsync 才应答；检查点在临时文件写完后原子替换发布，只回收整段都已进入检查点的日志。恢复只接受完整检查点与连续有效日志：尾部半条可丢弃，中段损坏立即停止并保留现场。')+`<div class="split"><section><section class="panel"><div class="actions" style="margin-top:0"><h2 style="margin:0">${esc(d.name)}</h2>${status(d.status)}<button id="export">导出演练 JSON ↓</button></div>${actionPanel(d)}<div class="actions"><button id="checkpoint" ${canWrite?'':'disabled'}>生成检查点</button><button id="interrupt" ${canWrite?'':'disabled'}>检查点发布中断</button><button id="poweroff" ${canWrite?'':'disabled'}>模拟掉电</button><button class="primary" id="recover" ${writable?'':'disabled'}>执行恢复</button></div><p class="helper">新建演练为“待恢复”，执行一次恢复后才接受写入。掉电或恢复停止后，写入按钮不可用，必须先恢复。</p></section><section class="panel"><h2>恢复报告</h2>${recoveryPanel(report,action)}</section><section class="panel"><h2>演练脚本（按组原子执行）</h2>${table(['组号','操作数','操作'],groupRows(d.script||[]))}<p class="helper">脚本来自关联的写入批次；组内 put/delete 按顺序一次写入，同组重试不会重复生效。</p></section></section><aside><section class="panel"><h2>写入一组</h2><form id="group-form"><label>组号</label><input name="group_id" value="${esc(d.script?.[0]?.group||'g1')}" required maxlength="64"><label>多键操作 · JSON 数组</label><textarea name="operations" rows="8">${json(d.script?.[0]?.ops||[])}</textarea><label>掉电时机</label><select name="crash"><option value="none">应答前不注入</option><option value="before_fsync">fsync 前掉电（整组失效）</option><option value="after_fsync">fsync 后掉电（整组保留）</option></select><div class="actions"><button class="primary" type="submit" ${canWrite?'':'disabled'}>原子写入整组 →</button></div></form><p class="helper">同组号相同内容返回“未重复写入”；内容不同直接 409 整组拒绝。正常应答即表示整组已 fsync。</p></section><section class="panel"><h2>磁盘现场</h2>${table(['文件','字节'],(d.on_disk||[]).map(f=>['<span class="mono">'+esc(f.file)+'</span>',f.bytes]))}<p class="helper">日志段按组数轮转；检查点只回收整段都已被覆盖的日志，并发写入的新日志不受影响。</p></section><p><a href="#drills">← 返回演练列表</a></p></aside></div>`;
    el.querySelector('#export').onclick=()=>download(d.id).catch(e=>notify(e.message));
    const on=(id,fn)=>{const b=el.querySelector('#'+id);if(b)b.onclick=()=>submit(b.closest('.panel'),fn)};
    on('checkpoint',async()=>{await checkpoint(d.id,false);notify('检查点已发布');await drillsPage(el)});
    on('interrupt',async()=>{await checkpoint(d.id,true);notify('检查点发布中断，旧检查点与日志保留');await drillsPage(el)});
    on('poweroff',async()=>{await crash(d.id);notify('已模拟掉电，等待恢复');await drillsPage(el)});
    on('recover',async()=>{try{await recover(d.id);notify('恢复完成，状态可写入')}catch(e){notify(e.message)}await drillsPage(el)});
    el.querySelector('#group-form').onsubmit=e=>{e.preventDefault();submit(e.target,async()=>{const data=Object.fromEntries(new FormData(e.target));try{data.operations=JSON.parse(data.operations)}catch{throw Error('操作JSON格式错误，请检查引号与逗号')}const result=await writeGroup(d.id,data);notify(OUTCOMES[result.outcome]||result.message);await drillsPage(el)})};
    return;
  }
  el.innerHTML=header('05','掉电之后，哪些写入还在','从批次建立演练目录，按组原子执行多键覆盖与删键；掉电后可恢复并核对哪些组整组保留、哪些整组不生效、哪些被拒绝。')+`<div class="split"><section class="panel"><h2>持久化演练</h2>${table(['演练','输入批次','状态','检查点/最后 LSN','组',''],list.map(x=>['<a href="#drills/'+x.id+'">'+esc(x.name)+'</a><br><small>#'+x.id+' / '+esc(x.created_at)+'</small>','#'+x.dataset_id,status(x.status),x.checkpoint_lsn+' / '+x.last_lsn,x.group_count,'<a href="#drills/'+x.id+'">打开演练 →</a>']))}</section><aside class="panel"><h2>新建演练</h2><form id="drill-form"><label>演练名称</label><input name="name" value="durability-${Date.now().toString().slice(-6)}" required maxlength="120"><label>写入批次</label><select name="dataset_id">${ds.map(x=>`<option value="${x.id}">${esc(x.name)} / ${x.operation_count}条</option>`).join('')}</select><label>每段日志组数</label><input name="segment_limit" type="number" min="1" max="128" value="2"><div class="actions"><button class="primary" type="submit" ${writable?'':'disabled'}>建立演练目录</button></div></form><p class="helper">新演练状态为“待恢复”，先执行一次恢复再写入。选择带 group 字段的批次可作为分组脚本。</p></aside></div>`;
  if(writable)el.querySelector('#drill-form').onsubmit=e=>{e.preventDefault();submit(e.target,async()=>{const data=Object.fromEntries(new FormData(e.target));data.dataset_id=Number(data.dataset_id);data.segment_limit=Number(data.segment_limit);await create(data);notify('演练已建立');await drillsPage(el)})};
}
