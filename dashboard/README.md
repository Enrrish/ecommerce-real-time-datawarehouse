# dashboard

实时大屏工程与设计稿。

## 设计稿

`docs/assets/dashboard.png` 为 1920×1080 深色大屏视觉稿，包含：

| 分区 | 内容 | 数据来源 |
| --- | --- | --- |
| KPI 条 | 今日 GMV / 支付订单 / 支付 UV / 客单价 / 支付转化率 / 链路 P99 延迟 | `ads_realtime_overview` |
| 实时转化漏斗 | 商详 → 加购 → 提交 → 支付 | `ads_funnel_1d` |
| 成交趋势 | 分钟级 GMV 曲线 + 昨日同期对比 | `ads_realtime_overview` |
| 品类排行 | GMV TOP 6 | `ads_category_rank_1d` |
| 支付方式 | 环形图 + 占比 | `ads_payment_method_1d` |
| 实时订单流水 | 最近 7 笔订单滚动 | `ads_order_stream` |
| 链路健康度 | Kafka 积压 / Checkpoint / 反写吞吐 / 反压 / 节点状态 | Flink & Doris 监控指标 |

## 技术选型

大屏为纯静态页面直连 Doris（`/api/query` 走 HTTP 或 MySQL 协议），不使用重型 BI 平台，理由是：

- 面试演示时启动成本低，一条 `python -m http.server` 即可
- 图表样式完全可控，能还原设计稿
- 查询直接打 ADS 层，天然验证指标口径是否正确

推荐方案：**静态 HTML + ECharts + 轻量查询代理**（Node 或 Python，仅做转发与鉴权）。

## 目录约定

```
dashboard/
├── index.html
├── src/
│   ├── charts/          # 各面板图表配置
│   ├── theme.js         # 设计稿色板与字号
│   └── api.js           # 查询封装
└── assets/
```

> 设计稿色板（深色）：背景 `#080B10`，面板 `#0F151C`，主色 `#35D6E8`，上涨 `#FF6A55`，下跌 `#2FD07A`。

## 待补充

- [ ] 页面骨架与 ECharts 图表
- [ ] 查询代理
- [ ] 自动刷新与断线重连
