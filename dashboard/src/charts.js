/* =============================================================================
   charts.js —— 零依赖 SVG 图表
   -----------------------------------------------------------------------------
   全部手写，不引 ECharts。原因很实际：离线环境装不了 npm 包，CDN 也拉不到。
   手写反而带来两个好处 —— 体积只有几十 KB，且每个像素都在自己控制下，
   能精确还原设计稿的深色描边与发光效果。

   所有函数都是「拿到容器 + 数据 → 直接写 innerHTML」，不做增量 diff。
   一屏 8 个面板、2 秒一轮，实测整轮渲染 < 8ms，没必要上虚拟 DOM。
   ============================================================================= */

const NS = 'http://www.w3.org/2000/svg';

/* ------------------------------------------------------------------ 工具 */
const fmtMoney = (v, digits = 0) =>
  v == null ? '—' : v.toLocaleString('zh-CN', { minimumFractionDigits: digits, maximumFractionDigits: digits });

const fmtCompact = (v) => {
  if (v == null) return '—';
  if (Math.abs(v) >= 1e8) return (v / 1e8).toFixed(2) + ' 亿';
  if (Math.abs(v) >= 1e4) return (v / 1e4).toFixed(1) + ' 万';
  return fmtMoney(v, 0);
};

const pct = (v, digits = 1) => (v == null ? '—' : (v * 100).toFixed(digits) + '%');

/** 'HH:MM' → 当天第几分钟，用于把不等距的时间点摆到正确的横坐标上 */
const minuteOf = (s) => {
  const [h, m] = String(s || '0:0').split(':');
  return (parseInt(h, 10) || 0) * 60 + (parseInt(m, 10) || 0);
};

const esc = (s) =>
  String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

const sizeOf = (el) => ({
  w: Math.max(80, el.clientWidth || 300),
  h: Math.max(60, el.clientHeight || 160),
});

/* ==================================================================
   1. 趋势面积图 —— 当日累计 GMV + 昨日同期虚线
   ================================================================== */
function areaChart(el, data) {
  const today = data?.today || [];
  const prev = data?.prev || [];
  if (!today.length) { el.innerHTML = '<div class="empty">暂无成交数据</div>'; return; }

  const { w, h } = sizeOf(el);
  const pad = { l: 52, r: 14, t: 12, b: 26 };
  const iw = w - pad.l - pad.r;
  const ih = h - pad.t - pad.b;

  // 横轴固定铺满 0~24h。若按数据首尾缩放，前一日只有 237 个点会显得「更长」，
  // 两条线的时间轴就对不上了 —— 这是最容易看错的地方。
  const X = (minute) => pad.l + (minute / 1440) * iw;

  const maxY = Math.max(
    today[today.length - 1]?.[1] || 0,
    prev[prev.length - 1]?.[1] || 0,
    1
  );
  // 顶部留 12% 余量，避免最高点贴边
  const Y = (v) => pad.t + ih - (v / (maxY * 1.12)) * ih;

  const toPath = (pts) => {
    if (!pts.length) return '';
    return pts.map(([m, v], i) => `${i ? 'L' : 'M'}${X(minuteOf(m)).toFixed(1)},${Y(v).toFixed(1)}`).join('');
  };
  const toArea = (pts) => {
    if (!pts.length) return '';
    const line = pts.map(([m, v]) => `${X(minuteOf(m)).toFixed(1)},${Y(v).toFixed(1)}`).join('L');
    const x0 = X(minuteOf(pts[0][0])).toFixed(1);
    const x1 = X(minuteOf(pts[pts.length - 1][0])).toFixed(1);
    return `M${x0},${(pad.t + ih).toFixed(1)}L${line}L${x1},${(pad.t + ih).toFixed(1)}Z`;
  };

  // 横轴刻度：0/4/8/12/16/20/24 点
  const xTicks = [0, 4, 8, 12, 16, 20, 24].map((hh) => {
    const x = X(hh * 60);
    const label = String(hh).padStart(2, '0') + ':00';
    return `<line x1="${x}" y1="${pad.t}" x2="${x}" y2="${pad.t + ih}" stroke="rgba(255,255,255,.03)"/>
            <text x="${x}" y="${h - 8}" fill="#4C5B69" font-size="10" text-anchor="middle">${label}</text>`;
  }).join('');

  // 纵轴刻度：4 条
  const yTicks = [0.25, 0.5, 0.75, 1].map((r) => {
    const y = pad.t + ih - r * ih;
    return `<line x1="${pad.l}" y1="${y.toFixed(1)}" x2="${w - pad.r}" y2="${y.toFixed(1)}"
                  stroke="rgba(255,255,255,.04)" stroke-dasharray="3 5"/>
            <text x="${pad.l - 8}" y="${(y + 3.5).toFixed(1)}" fill="#4C5B69" font-size="10"
                  text-anchor="end">${fmtCompact(maxY * 1.12 * r)}</text>`;
  }).join('');

  const last = today[today.length - 1];

  el.innerHTML = `
<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
  <defs>
    <linearGradient id="gArea" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%"   stop-color="#35D6E8" stop-opacity=".34"/>
      <stop offset="55%"  stop-color="#35D6E8" stop-opacity=".10"/>
      <stop offset="100%" stop-color="#35D6E8" stop-opacity="0"/>
    </linearGradient>
    <linearGradient id="gLine" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%"   stop-color="#1FA8BC"/>
      <stop offset="60%"  stop-color="#35D6E8"/>
      <stop offset="100%" stop-color="#7BEAF7"/>
    </linearGradient>
  </defs>
  ${yTicks}${xTicks}
  <path d="${toArea(today)}" fill="url(#gArea)"/>
  <path d="${toPath(prev)}" fill="none" stroke="#5A6B7A" stroke-width="1.4"
        stroke-dasharray="4 4" stroke-linecap="round" opacity=".8"/>
  <path d="${toPath(today)}" fill="none" stroke="url(#gLine)" stroke-width="2"
        stroke-linejoin="round" stroke-linecap="round"/>
  <circle cx="${X(minuteOf(last[0]))}" cy="${Y(last[1])}" r="4" fill="#7BEAF7"/>
  <circle cx="${X(minuteOf(last[0]))}" cy="${Y(last[1])}" r="9" fill="#35D6E8" opacity=".18"/>
</svg>`;
}

/* ==================================================================
   2. 品类排行 —— 横向条形
   ================================================================== */
function hbarList(el, items) {
  if (!items?.length) { el.innerHTML = '<div class="empty">暂无类目数据</div>'; return; }
  const max = Math.max(...items.map((i) => i.gmv || 0), 1);

  el.innerHTML = items.map((it) => {
    const w = Math.max(2, ((it.gmv || 0) / max) * 100);
    const hot = it.rank <= 3;
    return `
<div style="display:grid;grid-template-columns:96px 1fr 74px;align-items:center;gap:10px;
            height:${(100 / items.length).toFixed(1)}%;min-height:26px">
  <span style="font-size:12.5px;color:${hot ? '#E8F0F5' : '#9AAAB8'};white-space:nowrap;
               overflow:hidden;text-overflow:ellipsis" title="${esc(it.raw || it.name)}">${esc(it.name)}</span>
  <span style="height:9px;border-radius:5px;background:rgba(255,255,255,.045);overflow:hidden">
    <i style="display:block;height:100%;width:${w.toFixed(1)}%;border-radius:5px;
              background:linear-gradient(90deg,${hot ? '#1FA8BC,#35D6E8' : '#2C5D68,#3E8E9C'});
              box-shadow:${hot ? '0 0 10px rgba(53,214,232,.45)' : 'none'};
              transition:width .5s ease"></i>
  </span>
  <span style="font-family:var(--mono);font-size:12px;text-align:right;
               color:${hot ? '#7BEAF7' : '#7C8D9B'};font-variant-numeric:tabular-nums">¥${fmtCompact(it.gmv)}</span>
</div>`;
  }).join('');
}

/* ==================================================================
   3. 支付方式 —— 环形图
   ================================================================== */
const PAY_COLORS = ['#35D6E8', '#5B8FF9', '#FFB020', '#FF6A55', '#8B7CF6'];

function donut(el, items, centerTitle) {
  if (!items?.length) { el.innerHTML = '<div class="empty">暂无支付数据</div>'; return; }

  const { w, h } = sizeOf(el);
  const legendH = 96;
  const box = Math.min(w * 0.72, h - legendH - 10);
  const cx = w / 2;
  const cy = 8 + box / 2;
  const r = box / 2 - 12;
  const stroke = Math.max(11, box * 0.115);

  const total = items.reduce((s, i) => s + (i.cnt || 0), 0) || 1;
  const circ = 2 * Math.PI * r;

  let offset = 0;
  const arcs = items.map((it, i) => {
    const frac = (it.cnt || 0) / total;
    // 段与段之间留 2px 空隙，视觉上更清晰（占比 0 的段不画）
    const len = Math.max(0, frac * circ - 2);
    const seg = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none"
        stroke="${PAY_COLORS[i % PAY_COLORS.length]}" stroke-width="${stroke}"
        stroke-dasharray="${len.toFixed(2)} ${(circ - len).toFixed(2)}"
        stroke-dashoffset="${(-offset).toFixed(2)}"
        stroke-linecap="butt" transform="rotate(-90 ${cx} ${cy})"/>`;
    offset += frac * circ;
    return seg;
  }).join('');

  const legend = items.map((it, i) => `
<span style="display:flex;align-items:center;gap:8px;font-size:11.5px;color:#9AAAB8">
  <i style="width:7px;height:7px;border-radius:2px;flex:0 0 auto;background:${PAY_COLORS[i % PAY_COLORS.length]}"></i>
  <span style="min-width:74px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(it.type)}</span>
  <b style="color:#E8F0F5;font-weight:500;font-family:var(--mono)">${pct(it.cnt_ratio)}</b>
</span>`).join('');

  el.innerHTML = `
<svg width="${w}" height="${cy + box / 2 + 6}" viewBox="0 0 ${w} ${cy + box / 2 + 6}">
  <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="rgba(255,255,255,.04)" stroke-width="${stroke}"/>
  ${arcs}
  <text x="${cx}" y="${cy - 4}" fill="#E8F0F5" font-size="${Math.round(box * 0.155)}"
        font-weight="600" text-anchor="middle" font-family="var(--mono)">${fmtMoney(total, 0)}</text>
  <text x="${cx}" y="${cy + 15}" fill="#7C8D9B" font-size="10.5" text-anchor="middle">${esc(centerTitle)}</text>
</svg>
<div style="display:flex;flex-direction:column;gap:6px;margin-top:4px">${legend}</div>`;
}

/* ==================================================================
   4. 转化漏斗 —— 居中梯形条
   ================================================================== */
function funnelChart(el, steps) {
  if (!steps?.length) { el.innerHTML = '<div class="empty">暂无漏斗数据</div>'; return; }

  const maxPv = Math.max(...steps.map((s) => s.pv || 0), 1);
  // 4 步 + 3 个转化率标注，用固定像素分配高度比百分比更稳
  const gapPx = 15;
  const barH = 52;

  const rows = steps.map((s, i) => {
    const ratio = (s.pv || 0) / maxPv;
    // 最窄保留 40%、最宽只到 92%：留出边距，否则第一步会贴着面板边框
    const widthPct = 40 + ratio * 52;
    const conv = i > 0 ? s.conv_prev : null;
    const convRow = i > 0
      ? `<div style="height:${gapPx}px;display:flex;align-items:center;justify-content:center;gap:6px">
           <svg width="9" height="11" viewBox="0 0 9 11" fill="none">
             <path d="M4.5 0v9M1 6l3.5 3.5L8 6" stroke="#4C5B69" stroke-width="1.2"
                   stroke-linecap="round" stroke-linejoin="round"/>
           </svg>
           <span style="font-size:10.5px;color:#5A6B7A">转化</span>
           <span style="font-size:11.5px;color:${conv != null && conv >= 0.5 ? '#2FD07A' : '#FFB020'};
                        font-family:var(--mono);font-weight:500">${conv == null ? '—' : pct(conv)}</span>
         </div>`
      : '';

    return `${convRow}
<div style="display:flex;flex-direction:column;align-items:center">
  <div style="width:${widthPct.toFixed(1)}%;height:${barH}px;border-radius:6px;
              background:linear-gradient(135deg,rgba(53,214,232,${(0.30 - i * 0.05).toFixed(2)}),
                                          rgba(53,214,232,${(0.09 - i * 0.012).toFixed(2)}));
              border:1px solid rgba(53,214,232,${(0.42 - i * 0.07).toFixed(2)});
              display:flex;align-items:center;justify-content:space-between;
              padding:0 14px;box-shadow:inset 0 0 22px rgba(53,214,232,.10)">
    <span style="font-size:12px;color:#C8D8E2;letter-spacing:.3px">${esc(s.label)}</span>
    <span style="font-family:var(--mono);font-size:${i === 0 ? 16 : 14.5}px;font-weight:600;
                 color:#7BEAF7;font-variant-numeric:tabular-nums">${fmtMoney(s.pv, 0)}</span>
  </div>
</div>`;
  }).join('');

  el.innerHTML = `<div style="display:flex;flex-direction:column;justify-content:center;height:100%;padding:2px 0">${rows}</div>`;
}

/* ==================================================================
   5. 地域地图 —— 经纬度散点（没有 GeoJSON 的替代方案）
   ================================================================== */
function bubbleMap(el, states) {
  const valid = (states || []).filter((s) => s.lat != null && s.lng != null);
  if (!valid.length) { el.innerHTML = '<div class="empty">暂无地域数据</div>'; return; }

  const { w, h } = sizeOf(el);
  const pad = { l: 8, r: 8, t: 10, b: 26 };   // 底部留给说明文字
  const iw = w - pad.l - pad.r;
  const ih = h - pad.t - pad.b;

  // 巴西经纬度边界（与 08-ads-refresh.sql 里过滤越界点用的是同一组数字）
  const LNG0 = -74, LNG1 = -34, LAT0 = 6, LAT1 = -34;

  // 等比例投影：经度 40° × 纬度 40°，取较小的缩放比，保证国土不被拉变形
  const scale = Math.min(iw / (LNG1 - LNG0), ih / (LAT0 - LAT1));
  const offX = pad.l + (iw - (LNG1 - LNG0) * scale) / 2;
  const offY = pad.t + (ih - (LAT0 - LAT1) * scale) / 2;
  const PX = (lng) => offX + (lng - LNG0) * scale;
  const PY = (lat) => offY + (LAT0 - lat) * scale;

  // 经纬网格。没有 GeoJSON 画不出州界，网格是最省的「地图感」来源
  const grid = [
    ...[-70, -60, -50, -40].map((lng) =>
      `<line x1="${PX(lng).toFixed(1)}" y1="${offY.toFixed(1)}" x2="${PX(lng).toFixed(1)}"
             y2="${(offY + (LAT0 - LAT1) * scale).toFixed(1)}"
             stroke="rgba(53,214,232,.06)" stroke-dasharray="2 6"/>`),
    ...[-30, -20, -10, 0].map((lat) =>
      `<line x1="${offX.toFixed(1)}" y1="${PY(lat).toFixed(1)}"
             x2="${(offX + (LNG1 - LNG0) * scale).toFixed(1)}" y2="${PY(lat).toFixed(1)}"
             stroke="rgba(53,214,232,.06)" stroke-dasharray="2 6"/>`),
  ].join('');

  const maxGmv = Math.max(...valid.map((s) => s.gmv || 0), 1);
  // 半径用平方根映射面积，否则 SP 一个点会把其他 25 个州全压成看不见的小点
  const R = (gmv) => 6 + Math.sqrt((gmv || 0) / maxGmv) * 26;

  const dots = valid
    .slice()
    .sort((a, b) => (b.gmv || 0) - (a.gmv || 0))
    .map((s) => {
      const r = R(s.gmv);
      const top = s.rank <= 5;
      const col = top ? '#35D6E8' : '#2C6E7A';
      return `<g>
  <circle cx="${PX(s.lng).toFixed(1)}" cy="${PY(s.lat).toFixed(1)}" r="${r.toFixed(1)}"
          fill="${col}" fill-opacity="${top ? 0.30 : 0.16}"
          stroke="${col}" stroke-opacity="${top ? 0.85 : 0.4}" stroke-width="1"/>
  ${top ? `<text x="${PX(s.lng).toFixed(1)}" y="${(PY(s.lat) - r - 5).toFixed(1)}"
                 fill="#7BEAF7" font-size="10" text-anchor="middle"
                 font-family="var(--mono)" letter-spacing=".4">${esc(s.uf)}</text>` : ''}
</g>`;
    }).join('');

  const top = valid.filter((s) => s.rank <= 3).map((s) => s.uf).join(' / ');

  el.innerHTML = `
<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
  <rect x="${offX.toFixed(1)}" y="${offY.toFixed(1)}"
        width="${((LNG1 - LNG0) * scale).toFixed(1)}" height="${((LAT0 - LAT1) * scale).toFixed(1)}"
        fill="none" stroke="rgba(53,214,232,.10)" rx="6"/>
  ${grid}
  ${dots}
  <text x="${(w / 2).toFixed(1)}" y="${h - 6}" fill="#4C5B69" font-size="10.5"
        text-anchor="middle" letter-spacing=".3">${valid.length} 州有成交 · 气泡面积 = GMV · TOP ${esc(top)}</text>
</svg>`;
}

/* ==================================================================
   6. 订单流水 —— 表格
   ================================================================== */
const STATUS_STYLE = {
  '已签收': ['rgba(47,208,122,.14)', '#2FD07A'],
  '运输中': ['rgba(53,214,232,.14)', '#35D6E8'],
  '已取消': ['rgba(255,77,79,.14)', '#FF4D4F'],
  '无法履约': ['rgba(255,77,79,.14)', '#FF4D4F'],
  '已开票': ['rgba(255,176,32,.14)', '#FFB020'],
  '处理中': ['rgba(255,176,32,.14)', '#FFB020'],
};

function orderTable(el, orders) {
  if (!orders?.length) { el.innerHTML = '<div class="empty">暂无订单</div>'; return; }

  const head = `
<div style="display:grid;grid-template-columns:64px 1fr 46px 1fr 84px 62px;gap:8px;
            padding:0 4px 7px;border-bottom:1px solid rgba(255,255,255,.05);
            font-size:10.5px;color:#4C5B69;letter-spacing:.6px">
  <span>时间</span><span>订单号</span><span>州</span><span>城市</span>
  <span style="text-align:right">金额</span><span style="text-align:right">状态</span>
</div>`;

  const rows = orders.map((o, i) => {
    const [bg, fg] = STATUS_STYLE[o.status] || ['rgba(124,141,155,.14)', '#7C8D9B'];
    return `
<div style="display:grid;grid-template-columns:64px 1fr 46px 1fr 84px 62px;gap:8px;align-items:center;
            padding:7.5px 4px;border-bottom:1px solid rgba(255,255,255,.028);
            ${i === 0 ? 'background:linear-gradient(90deg,rgba(53,214,232,.07),transparent);' : ''}">
  <span style="font-family:var(--mono);font-size:11px;color:#7C8D9B">${esc(o.time)}</span>
  <span style="font-family:var(--mono);font-size:11px;color:#C8D8E2;letter-spacing:.3px">${esc(o.short)}</span>
  <span style="font-size:11px;color:#9AAAB8">${esc(o.uf)}</span>
  <span style="font-size:11.5px;color:#9AAAB8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis"
        title="${esc(o.city)}">${esc(o.city)}</span>
  <span style="font-family:var(--mono);font-size:11.5px;color:#E8F0F5;text-align:right;
               font-variant-numeric:tabular-nums">¥${fmtMoney(o.gmv, 2)}</span>
  <span style="justify-self:end;font-size:10px;padding:2px 6px;border-radius:4px;
               background:${bg};color:${fg};white-space:nowrap">${esc(o.status)}</span>
</div>`;
  }).join('');

  el.innerHTML = head + rows;
}

/* ==================================================================
   7. 链路健康度 —— 四个数字 + 一组状态灯
   ================================================================== */
function healthPanel(el, metrics) {
  if (!metrics?.length) { el.innerHTML = '<div class="empty">暂无链路指标</div>'; return; }

  const by = Object.fromEntries(metrics.map((m) => [m.key, m]));
  const levelColor = (lv) => ({ ok: '#2FD07A', warn: '#FFB020', error: '#FF4D4F' }[lv] || '#7C8D9B');

  const fmtVal = (m, digits = 0) => {
    if (!m || m.value == null) return '—';
    const v = m.value;
    if (Math.abs(v) >= 10000 && digits === 0) return fmtCompact(v);
    return fmtMoney(v, digits);
  };

  const cards = [
    { k: 'kafka_lag', label: 'Kafka 积压', unit: '条', d: 0 },
    { k: 'checkpoint_p95_ms', label: 'Checkpoint P95', unit: 'ms', d: 0 },
    { k: 'e2e_latency_ms', label: '端到端延迟', unit: 'ms', d: 0 },
    { k: 'pipeline_freshness_s', label: '管道新鲜度', unit: 's', d: 0 },
  ].map((c) => {
    const m = by[c.k];
    // title 用 threshold_desc：像「端到端延迟 3849ms」其实是 P3 的单次采样，
    // 不是 P99，这句话只在库里、页面上看不到，容易被当成实时值。
    const tip = m?.desc ? esc(m.desc) : '';
    return `
<div title="${tip}" style="padding:10px 12px;border-radius:8px;background:rgba(255,255,255,.022);
            border:1px solid rgba(255,255,255,.035)">
  <div style="font-size:10.5px;color:#5A6B7A;letter-spacing:.4px">${c.label}</div>
  <div style="margin-top:5px;display:flex;align-items:baseline;gap:4px">
    <span style="font-family:var(--mono);font-size:17px;font-weight:600;color:${levelColor(m?.level)}">
      ${fmtVal(m, c.d)}</span>
    <span style="font-size:10px;color:#4C5B69">${c.unit}</span>
  </div>
</div>`;
  }).join('');

  const quality = ['dup_rate', 'late_ratio', 'canceled_ratio', 'event_age_p99_s']
    .map((k) => {
      const m = by[k];
      if (!m) return '';
      const val = k === 'event_age_p99_s' ? fmtCompact(m.value) + ' s' : fmtMoney(m.value, 2) + '%';
      return `<span style="display:flex;align-items:center;gap:6px;font-size:11px;color:#7C8D9B">
        <i style="width:6px;height:6px;border-radius:50%;background:${levelColor(m.level)};
                  box-shadow:0 0 6px ${levelColor(m.level)}66"></i>
        ${esc(m.name)}<b style="color:#C8D8E2;font-weight:500;font-family:var(--mono)">${val}</b>
      </span>`;
    }).join('');

  // 三条链路：从源到仓到应用，灯色取对应环节最差的那个指标
  const jobs = by['flink_jobs_running'];
  const ckpt = by['checkpoint_p95_ms'];
  const fresh = by['pipeline_freshness_s'];
  const chain = [
    { name: 'MySQL binlog → Flink CDC', ok: (jobs?.value || 0) >= 4 },
    { name: 'Flink → Doris 两阶段提交', ok: (ckpt?.value || 0) > 0 && (ckpt?.value || 0) < 30000 },
    // 这里原来判的是 kafka_lag —— 那是「Kafka 还有多少条没消费」，
    // 与 Doris 侧的物化刷新完全无关：Kafka 追平了它绿，刷新停了它也绿。
    // 改用管道新鲜度（三张主表最近一次成功写入距今），它对得上灯名。
    { name: 'Doris 写入 → 大屏可见', ok: (fresh?.value || 0) < 300 },
  ].map((c) => `
<div style="display:flex;align-items:center;justify-content:space-between;padding:6px 0;
            border-bottom:1px solid rgba(255,255,255,.03)">
  <span style="font-size:11.5px;color:#9AAAB8">${esc(c.name)}</span>
  <span style="display:flex;align-items:center;gap:6px;font-size:11px;color:${c.ok ? '#2FD07A' : '#FF4D4F'}">
    <i style="width:6px;height:6px;border-radius:50%;background:currentColor;
              box-shadow:0 0 7px currentColor"></i>${c.ok ? '正常' : '异常'}
  </span>
</div>`).join('');

  el.innerHTML = `
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px">${cards}</div>
<div style="margin-top:11px;padding-top:9px;border-top:1px solid rgba(255,255,255,.04)">
  <div style="font-size:10.5px;color:#4C5B69;letter-spacing:.6px;margin-bottom:7px">数据质量</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px 14px">${quality}</div>
</div>
<div style="margin-top:9px;padding-top:7px;border-top:1px solid rgba(255,255,255,.04)">${chain}</div>`;
}
