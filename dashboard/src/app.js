/* =============================================================================
   app.js —— 取数、渲染、轮询
   -----------------------------------------------------------------------------
   一轮的流程：GET /api/dashboard → 一次性拿到 8 个面板的数据 → 各画各的。
   不做增量更新：整块重绘 8 个面板实测 < 8ms，比自己维护 diff 更不容易出错，
   也避免了「某个字段没更新到」这类只有在演示时才发现的 bug。
   ============================================================================= */

const REFRESH_MS = 1500;
// ?once=1 只取一轮，不排下一次。无头浏览器截图时用得上 ——
// 它会快进定时器，轮询会和首屏请求抢管道，结果截到一片「加载中…」。
const ONCE = location.search.includes('once=1');
const screenEl = document.getElementById('screen');

/* ------------------------------------------------------------------ 缩放 */
// 画布固定 1920×1080，按视口等比缩放。用 transform 而不是改 rem：
// 设计稿是按像素画的，整块缩放不会让 1px 描边在某些分辨率下消失。
function fit() {
  const s = Math.min(window.innerWidth / 1920, window.innerHeight / 1080);
  screenEl.style.transform = `translate(-50%, -50%) scale(${s})`;
}
window.addEventListener('resize', fit);
fit();

/* --------------------------------------------------------------- KPI 卡 */
const KPI_DEFS = [
  {
    label: '当日 GMV', accent: true,
    value: (d) => '¥' + fmtMoney(d.kpi.gmv, 2),
    foot: (d) => deltaFoot(d.kpi.gmv, d.kpi.prev_gmv, '较前一日'),
  },
  {
    label: '支付订单数',
    value: (d) => fmtMoney(d.kpi.orders, 0),
    foot: (d) => deltaFoot(d.kpi.orders, d.kpi.prev_orders, '较前一日'),
  },
  {
    label: '成交买家数',
    value: (d) => fmtMoney(d.kpi.buyers, 0),
    foot: (d) => `<span>人均 ${(d.kpi.orders / Math.max(1, d.kpi.buyers)).toFixed(2)} 单</span>`,
  },
  {
    label: '客单价 AOV',
    value: (d) => '¥' + fmtMoney(d.kpi.aov, 2),
    foot: (d) => `<span>件单价 ¥${fmtMoney(d.kpi.gmv / Math.max(1, d.kpi.items), 2)}</span>`,
  },
  {
    label: '无效交易占比',
    value: (d) => (d.kpi.canceled_ratio == null ? '—' : d.kpi.canceled_ratio.toFixed(2) + '%'),
    foot: (d) => `<span>全期已取消 ${fmtMoney(d.healthMap?.canceled_gmv, 2)} 元</span>`,
  },
  {
    label: '端到端延迟',
    value: (d) => (d.kpi.p99_ms == null ? '—' : fmtMoney(d.kpi.p99_ms, 0)),
    unit: 'ms', accent: true,
    foot: (d) => {
      const ck = d.healthMap?.checkpoint_p95_ms;
      return `<span>Checkpoint P95 ${ck == null ? '—' : fmtMoney(ck, 0)} ms</span>`;
    },
  },
];

function deltaFoot(cur, prev, label) {
  if (cur == null || !prev) return `<span>${label} —</span>`;
  const r = (cur - prev) / prev;
  const cls = r > 0.0005 ? 'up' : r < -0.0005 ? 'down' : 'flat';
  const arrow = r > 0.0005 ? '↑' : r < -0.0005 ? '↓' : '·';
  return `<span class="delta ${cls}">${arrow} ${Math.abs(r * 100).toFixed(1)}%</span>
          <span>${label}</span>`;
}

function renderKpi(d) {
  const row = document.getElementById('kpiRow');
  // 只在结构变化时重建 DOM；数值更新走 textContent，避免每 2 秒闪一次重排
  if (row.children.length !== KPI_DEFS.length) {
    row.innerHTML = KPI_DEFS.map((k) => `
<div class="kpi">
  <div class="kpi-label">${k.label}${k.accent ? '<i style="width:5px;height:5px;border-radius:50%;background:#35D6E8;box-shadow:0 0 6px #35D6E8"></i>' : ''}</div>
  <div class="kpi-value"><span class="v">—</span>${k.unit ? `<span class="unit">${k.unit}</span>` : ''}</div>
  <div class="kpi-foot"></div>
</div>`).join('');
  }

  [...row.children].forEach((card, i) => {
    const def = KPI_DEFS[i];
    const vEl = card.querySelector('.v');
    const next = def.value(d);
    if (vEl.textContent !== next) {
      vEl.textContent = next;
      vEl.classList.remove('flash');
      void vEl.offsetWidth;          // 强制重排，让动画能重复触发
      vEl.classList.add('flash');
    }
    const fEl = card.querySelector('.kpi-foot');
    const foot = def.foot(d);
    if (fEl.dataset.sig !== foot) { fEl.innerHTML = foot; fEl.dataset.sig = foot; }
  });
}

/* ----------------------------------------------------------------- 渲染 */
function render(d) {
  const meta = d.meta || {};
  d.healthMap = Object.fromEntries((d.health || []).map((h) => [h.key, h.value]));

  // 顶栏
  document.getElementById('datePill').textContent =
    `数据日 ${meta.data_date || '—'}`;
  document.getElementById('funnelNote').textContent =
    `埋点 ${meta.funnel_date || '—'}`;
  document.getElementById('catNote').textContent = meta.data_date || '';
  document.getElementById('mapNote').textContent =
    `${(d.map || []).length} / 27 州有成交`;
  document.getElementById('clock').textContent = (meta.server_ts || '').slice(11) || '--:--:--';

  // 今日实时：唯一不按展示日取的口径。改一行 MySQL，变的是这里。
  const live = d.today || {};
  const gmvEl = document.getElementById('todayGmv');
  const ordEl = document.getElementById('todayOrders');
  const gmvTxt = live.gmv == null ? '—' : '¥' + fmtMoney(live.gmv, 2);
  if (gmvEl.textContent !== gmvTxt) {
    gmvEl.textContent = gmvTxt;
    gmvEl.classList.remove('flash');
    void gmvEl.offsetWidth;
    gmvEl.classList.add('flash');
  }
  ordEl.textContent = live.orders == null ? '—' : fmtMoney(live.orders, 0);

  renderKpi(d);
  funnelChart(document.getElementById('pFunnel'), d.funnel);
  areaChart(document.getElementById('pTrend'), d.trend);
  hbarList(document.getElementById('pCategory'), d.category);
  bubbleMap(document.getElementById('pMap'), d.map);
  donut(document.getElementById('pPayment'), d.payment, '支付笔数');
  orderTable(document.getElementById('pOrders'), d.orders);
  healthPanel(document.getElementById('pHealth'), d.health);
}

/* ------------------------------------------------------------- 链路状态 */
function setLink(state, text) {
  const pill = document.getElementById('linkPill');
  pill.className = 'pill' + (state === 'ok' ? '' : state === 'warn' ? ' warn' : ' bad');
  document.getElementById('linkText').textContent = text;
}

/* --------------------------------------------------------------- 轮询 */
let seq = 0, failStreak = 0, tickTimer = null;

function startTick() {
  const bar = document.getElementById('tickBar');
  clearInterval(tickTimer);
  const t0 = performance.now();
  tickTimer = setInterval(() => {
    const p = Math.max(0, 1 - (performance.now() - t0) / REFRESH_MS);
    bar.style.transform = `scaleX(${p.toFixed(3)})`;
    if (p <= 0) clearInterval(tickTimer);
  }, 40);
}

async function pull() {
  const my = ++seq;
  startTick();
  // 8 秒硬超时。后端已给 Doris 设 query_timeout=5s，正常一轮约 0.3s；
  // 8 秒还拿不到说明管道假死（Doris 进程僵住 / 容器正在重启）。
  // 不 abort 的话 fetch 会永远 pending，下面的 finally 也就永远不执行 ——
  // 轮询彻底停摆，页面冻结在最后一个数字上，看起来像"还活着"。
  const ctrl = new AbortController();
  const killer = setTimeout(() => ctrl.abort(), 8000);
  try {
    const res = await fetch('/api/dashboard', { cache: 'no-store', signal: ctrl.signal });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    if (my !== seq) return;                 // 有更新的请求在跑，丢弃这次结果
    failStreak = 0;
    setLink('ok', '链路正常');
    render(data);
  } catch (err) {
    // 失败不清屏：保留上一轮数据比白屏有用，只在顶栏标红
    failStreak += 1;
    setLink(failStreak >= 3 ? 'bad' : 'warn',
      failStreak >= 3 ? '连接中断，重试中' : '数据延迟');
    if (failStreak === 1) {
      console.warn('[dashboard]', err.name === 'AbortError' ? '请求超过 8s 未返回' : err.message);
    }
  } finally {
    clearTimeout(killer);
    if (!ONCE) setTimeout(pull, REFRESH_MS);
  }
}

/* --------------------------------------------------------------- 启动 */
// 快照模式：single-file HTML 里内联了 window.__DATA__（由 server/snapshot.py 生成），
// 不再发请求。这样一份文件就能离线打开，也方便无头浏览器直接截图。
if (window.__DATA__) {
  try {
    render(window.__DATA__);
    setLink('ok', '离线快照');
    document.getElementById('tickBar').style.transform = 'scaleX(0)';
  } catch (err) {
    console.error(err);
    setLink('bad', '快照渲染失败');
  }
} else {
  pull();
}
