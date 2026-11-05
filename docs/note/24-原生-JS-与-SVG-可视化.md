# 24 · 原生 JS 与 SVG 可视化

> 一句话：不用图表库的理由不是"炫技"，是**这块屏只需要 7 种图形，而每种图形都是一段
> 可以读懂的字符串**。代价是所有细节（坐标、缩放、空态）都要自己管。

---

## 一、通用知识

### 1.1 三种更新策略

| 策略 | 做法 | 适用 |
| --- | --- | --- |
| 全量重绘 | 每次 `innerHTML = render(data)` | 数据量小、结构简单 |
| 增量更新 | 找到节点改 `textContent` | 节点多、变化少 |
| 虚拟 DOM | 框架的 diff | 复杂交互 |

本项目的选择是**全量重绘**：8 个面板实测 < 8ms，比维护 diff 更不容易出错，
也避免"某个字段没更新到"这类只在演示时发现的 bug。

代价要说清：全量重绘会**丢失焦点/滚动位置**。大屏没有输入框、不需要滚动，所以可接受。

### 1.2 `fetch` 的三件事

```js
const res = await fetch('/api/dashboard', { cache: 'no-store' });
if (!res.ok) throw new Error(`HTTP ${res.status}`);
const data = await res.json();
```

- **必须判 `res.ok`**：`fetch` 只有网络错误才 reject，404/500 都是 resolve
- `cache: 'no-store'` 让浏览器不缓存这次请求（与服务器头配合）
- 超时用 `AbortController`：

```js
const ac = new AbortController();
setTimeout(() => ac.abort(), 8000);
await fetch(url, { signal: ac.signal });
```

### 1.3 轮询 / SSE / WebSocket

| | 轮询 | SSE | WebSocket |
| --- | --- | --- | --- |
| 方向 | 客户端拉 | 服务端推（单向） | 双向 |
| 实现成本 | 最低 | 中 | 高 |
| 延迟 | = 轮询周期 | 近实时 | 近实时 |
| 断线重连 | 天然（下次请求） | 浏览器自动 | 要自己写 |

大屏用轮询：`1.5s` 的周期对"经营看板"完全够，而 SSE/WS 要处理重连、心跳、
服务端并发连接 —— 收益不匹配成本。

### 1.4 SVG 的坐标系统

```html
<svg width="600" height="200" viewBox="0 0 600 200">
```

- `viewBox` 定义"用户坐标"，`width/height` 定义"实际像素"
- 两者比例不同就会缩放
- **SVG 的 y 轴向下**，画折线时要翻转：`y = h - value/max*h`

常用元素：

```html
<path d="M0,100 L50,80 L100,120" />       <!-- 折线 -->
<path d="M0,100 L50,80 L100,120 Z" />     <!-- 闭合（填充） -->
<rect x="0" y="0" width="10" height="10" />
<circle cx="50" cy="50" r="4" />
<text x="10" y="20" font-size="12">标签</text>
```

### 1.5 固定画布 + `transform: scale`

```js
function fit() {
  const s = Math.min(window.innerWidth / 1920, window.innerHeight / 1080);
  screenEl.style.transform = `translate(-50%, -50%) scale(${s})`;
}
window.addEventListener('resize', fit);
```

为什么不用 `rem` 或百分比：设计稿按像素画的，整块缩放**不会让 1px 描边在某些分辨率下消失**，
也不会让字号出现小数。

代价：缩放后的文字在低分辨率屏幕上可能糊（浏览器按 GPU 合成）。

### 1.6 CSS Grid 的一个陷阱：`1fr` 不等于"剩下的空间"

```css
grid-template-rows: 1fr 1fr 1fr;      /* 等价于 minmax(auto, 1fr) */
```

`auto` 最小值意味着"内容多高，行就至少多高"。如果行内有一个 SVG 比分配的空间高，
行会被撑开 —— 而撑开之后下一轮算出的空间又变了，**表现为每轮刷新页面都在长高**。

修法三件套，缺一不可：

```css
grid-template-rows: repeat(3, minmax(0, 1fr));
.panel-body { overflow: hidden; }
svg { display: block; }        /* 去掉行内基线的 3~4px 间隙 */
```

`display:block` 那条是因为 inline 元素的基线对齐会留出间隙，累计几轮就明显了。

### 1.7 空态与降级

```js
const text = d.today.minutes > 0 ? fmtMoney(d.today.gmv) : '暂无';
```

判据要选"数据是否存在"，不是"值是否为 0"：

- `minutes > 0` → 今天确实有成交
- `gmv > 0` → 有成交但金额恰好为 0 时会被误判成"没有"

**"没有数据"和"数据是 0"是两种状态，界面上必须能区分。**

---

## 二、在本项目中

### 2.1 结构

| 文件 | 内容 |
| --- | --- |
| `index.html` | 1920×1080 的画布骨架，8 个面板容器 |
| `src/theme.css` | 变量（颜色/字号/间距）、Grid 布局 |
| `src/app.js` | 取数、轮询、缩放、KPI 卡、各面板装配 |
| `src/charts.js` | 7 个手写 SVG 图形函数 |

零依赖：没有 ECharts、没有 D3、没有构建工具。`index.html` 直接引 `src/*.js`。

### 2.2 七个图形函数

| 函数 | 用途 |
| --- | --- |
| `areaChart(el, data)` | 当日累计 GMV 曲线（面积图） |
| `hbarList(el, items)` | 品类排行（横向条） |
| `donut(el, items, title)` | 支付方式占比（环图） |
| `funnelChart(el, steps)` | 转化漏斗 |
| `bubbleMap(el, states)` | 巴西地域气泡图 |
| `orderTable(el, orders)` | 右侧订单流水（8 行） |
| `healthPanel(el, metrics)` | 底部健康度（11 项） |

每个函数都是"拼一个 SVG 字符串 → `el.innerHTML = s`"。
比如折线：

```js
const pts = data.map((d, i) => `${x(i)},${h - d.v / max * h}`).join(' ');
return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
          <polyline points="${pts}" fill="none" stroke="var(--accent)" />
        </svg>`;
```

### 2.3 一轮刷新的流程

```
GET /api/dashboard
  → 一次性拿到 8 个面板的数据
  → 各画各的（全量重绘，实测 < 8ms）
  → setTimeout 1.5s 后再来一轮
```

不做增量更新，也不做"哪个面板变了才重绘"。

### 2.4 `?once=1`

```js
const ONCE = location.search.includes('once=1');
...
if (!ONCE) setTimeout(tick, REFRESH_MS);
```

用途：无头浏览器截图。

无头模式的 `--virtual-time-budget` 会**快进定时器**，于是轮询和首屏请求抢管道 ——
截到的就是一片"加载中…"。加 `?once=1` 只取一轮，截图才稳定。

这条不是为了截图好看，是为了**让首图和实际界面一致**（旧首图还写着已被证伪的
"今日 GMV / LIVE"）。

### 2.5 口径标注写进界面

| 位置 | 标注 | 解决什么 |
| --- | --- | --- |
| 顶栏 | `今日成交 暂无 [CDC]` | 说明这块数据来自 CDC，今天没有 |
| 顶栏 | `交易日 2017-11-24` | 交易面板的时间轴不是今天 |
| 漏斗底部 | `行为流口径：「支付」= 上报 pay 事件的人次` | 与交易面板的"支付"区分 |
| 漏斗底部 | `不写订单表、不产生 GMV，与下方交易面板非同源` | 解释"为什么漏斗在动 GMV 不动" |
| KPI | `成交订单数`（不是"支付订单数"） | 避免与漏斗的"完成支付"撞词 |
| 订单流水 | `CDC 入库 · 最近 8 笔 · 至 2018-09-03` | 说明这是历史数据 |

这些标注不是装饰。没有它们，第一个问题一定是"为什么漏斗在涨，GMV 不变"。

### 2.6 踩过的坑

**坑 1：CSS `1fr` 让页面每轮长高一点**

```
现象：大屏开着不动，每隔一会儿行高变一点，最后溢出
原因：grid-template-rows: 1fr = minmax(auto,1fr)，内容撑开行
      + svg 是 inline，基线留 3~4px 间隙
修法：minmax(0,1fr) + .panel-body{overflow:hidden} + svg{display:block}
```

三件缺一不可 —— 只改一个还会慢速复现。

**坑 2：旧 JS 配新 HTML → 全屏「加载中…」**

```
旧 JS: document.getElementById('todayGmv').textContent = ...
新 HTML: 这个 id 已改名成 todayText
→ null.textContent 抛 TypeError
→ render() 从这一行断掉，8 个面板全停在初始文案
```

详见 `23` 的缓存部分。前端能做的防御是**每个面板独立 try/catch**，
一个坏了不影响其它七个。

**坑 3：`fetch` 不判 `res.ok`**

接口 500 时 `res.json()` 抛异常，被 `catch` 走失败分支 → 顶栏显示"连接中断"，
看起来像网络问题，实际是后端 SQL 错了。加上 `res.ok` 判断能区分开。

**坑 4：`innerHTML` 拼数据要转义**

`orderTable` 里如果商品名含 `<`，会破坏 DOM 结构。本项目数据是受控的（Olist 数据集），
没有做转义 —— 这是个已知取舍，写在这里以免以后接真实数据时忘掉。

---

## 三、速查

```js
// 轮询骨架
const REFRESH_MS = 1500;
async function tick() {
  try {
    const res = await fetch('/api/dashboard', { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    render(await res.json());
    setStatus('ok');
  } catch (e) {
    fails += 1;
    setStatus(fails >= 3 ? 'bad' : 'warn');
  } finally {
    if (!ONCE) setTimeout(tick, REFRESH_MS);
  }
}
```

```css
/* 布局骨架 */
.grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr));
        grid-template-rows: repeat(3, minmax(0, 1fr)); }
.panel-body { overflow: hidden; }
svg { display: block; }
```

| 现象 | 原因 |
| --- | --- |
| 面板高度每轮变一点 | `1fr` + inline svg，用 `minmax(0,1fr)` 三件套 |
| 全屏「加载中…」但接口 200 | 某个 `getElementById` 返回 null，render 断链 |
| 顶栏「连接中断」但后端正常 | `res.ok` 没判 / JSON 解析失败 |
| 截图一片「加载中…」 | 无头浏览器快进定时器，加 `?once=1` |
| 某个面板空白其它正常 | 该面板的 SVG 计算出 NaN（除零/空数组） |

延伸：`23-HTTP-服务与大屏后端.md`（缓存与接口）、`20-指标口径设计.md`（为什么标注）、
`../03-指标口径.md` §2
