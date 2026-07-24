# LLM ETH Kline Lab

一个本地 Web 实验台，用于把 ETHUSDT 原始 OHLCV K 线窗口发送给 OpenAI-compatible LLM，观察模型对金融序列的结构化判断。

本项目用于研究、复盘和提示词实验，不构成交易建议，也不保证任何预测准确率或收益。

## 功能

- 选择历史截止时间。
- 选择 1m / 5m / 15m / 30m / 1h / 2h / 4h / 6h / 8h / 12h / 1d / 3d / 1w / 1mo 原K窗口。
- 可选择是否提交当前未收盘K线，未收盘K线由截止时间前已完成的 1m K线聚合并标记 `is_partial=true`。
- 自定义 system prompt 和用户说明。
- 支持 OpenAI-compatible `/chat/completions` API，常见如 DeepSeek、GLM、Kimi 等兼容接口都可接入。
- 支持多模型并发请求。
- 本地归档请求 payload、LLM 返回和元信息。
- 可从 Binance USD-M Futures API 更新 ETHUSDT 1m K线并重建多周期数据。

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 启动

```bash
python3 scripts/llm_raw_kline_web_ui.py --host 127.0.0.1 --port 8765
```

打开：

```text
http://127.0.0.1:8765/
```

## 数据目录

默认数据目录：

```text
data/clean/ethusdt_perp
```

你可以在页面点击“更新K线到最新”，或命令行执行：

```bash
python3 scripts/update_ethusdt_clean_klines.py --symbol ETHUSDT
```

如果本地还没有 `data/clean/ethusdt_perp/ETHUSDT-1m-clean.csv.gz`，首次更新会自动从 Binance 拉取最近一段 1m 数据作为初始底座。
默认初始化最近 365 天，可用 `LLM_KLINE_BOOTSTRAP_DAYS` 调整，最大 1825 天。

如果更新时报 `WinError 10013`、timeout 或连接失败，通常是本机网络、防火墙、代理或地区网络策略阻止访问 Binance Futures API。可以配置代理：

```powershell
$env:HTTPS_PROXY="http://127.0.0.1:7890"
$env:HTTP_PROXY="http://127.0.0.1:7890"
python scripts/update_ethusdt_clean_klines.py --symbol ETHUSDT
```

也可以指定可访问的 Binance USD-M Futures API 地址，多个地址用逗号分隔：

```powershell
$env:LLM_KLINE_BINANCE_FAPI_BASES="https://fapi.binance.com,https://fapi1.binance.com,https://fapi2.binance.com"
python scripts/update_ethusdt_clean_klines.py --symbol ETHUSDT
```

也可以用环境变量指定已有数据目录：

```bash
export LLM_KLINE_CLEAN_ROOT="/path/to/ethusdt_perp"
python3 scripts/llm_raw_kline_web_ui.py
```

## API 设置

页面默认 Base URL：

```text
https://api.openai.com/v1
```

API Key 只保存在你的本机浏览器 localStorage 中，后端归档会隐藏 key，不会写入明文 key。

## 注意

- 仓库仅包含代码和说明文件，请勿提交 `data/`、日志、API key 或大体积回测结果。
- LLM 对K线方向判断不应直接作为交易信号。
- 本项目只是一个实验台，真实交易需要独立风控、成本、滑点和样本外验证。

## License

MIT
