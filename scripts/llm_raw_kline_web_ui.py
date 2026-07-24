#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import html
import json
import math
import os
import re
import requests
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CLEAN_ROOT = Path(os.environ.get("LLM_KLINE_CLEAN_ROOT", str(ROOT / "data" / "clean" / "ethusdt_perp")))
ARCHIVE_ROOT = ROOT / "data" / "llm_raw_kline_web_ui"
UPDATE_SCRIPT = ROOT / "scripts" / "update_ethusdt_clean_klines.py"
BUNDLED_PYTHON = Path(os.environ.get("LLM_KLINE_PYTHON", sys.executable))
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_MODEL = "gpt-4o-mini"

TIMEFRAMES = ("1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo")
TIMEFRAME_LABELS = {
    "1m": "1分钟",
    "5m": "5分钟",
    "15m": "15分钟",
    "30m": "30分钟",
    "1h": "1小时",
    "2h": "2小时",
    "4h": "4小时",
    "6h": "6小时",
    "8h": "8小时",
    "12h": "12小时",
    "1d": "日线",
    "3d": "3日线",
    "1w": "周线",
    "1mo": "月线",
}

TIMEFRAME_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": 10080,
    "1mo": 43200,
}

DEFAULT_SYSTEM_PROMPT = """你是金融K线序列分析实验助手。
只使用用户 payload 中提供的 OHLCV 数据，不使用外部新闻、宏观信息或未提供的数据。
你的任务是给出一个可复盘的结构化判断，而不是提供投资建议。
如果证据不足或冲突，请输出 NO_TRADE。
输出必须是 JSON 对象，不要 markdown，不要多余文字。
"""

DEFAULT_USER_PROMPT = """请输出 JSON：
{
  "action": "LONG|SHORT|NO_TRADE",
  "horizon": "next_1h|next_4h|custom",
  "confidence": 0.0,
  "market_regime": "trend_up|trend_down|range|volatile|unclear",
  "primary_timeframe": "1m|5m|15m|30m|1h|4h|1d|unknown",
  "bull_score": 0,
  "bear_score": 0,
  "direction_evidence": ["最多3条，必须来自payload中的OHLCV"],
  "conflict_evidence": ["最多3条，没有则空数组"],
  "volume_price_read": "一句话说明量价关系",
  "risk_note": "一句话说明主要不确定性",
  "one_sentence": "一句话结论"
}

打分说明：
- bull_score / bear_score 用 0-5 表示证据强度。
- 证据明显偏多时输出 LONG，明显偏空时输出 SHORT。
- 证据不足、冲突或噪声过大时输出 NO_TRADE。
- 不输出止盈止损、仓位或任何保证性收益判断。"""

JSON_OBJECT_RESPONSE_GUARD = "输出必须是 JSON 对象，不要输出 Markdown 或额外文本。"

TREND_TIMEFRAMES = ("1mo", "1w", "1d", "4h", "2h", "1h", "15m", "5m")
TREND_REQUIRED_BARS = 250
TREND_LABELS = {
    "1mo": "月线",
    "1w": "周线",
    "1d": "日线",
    "4h": "4H",
    "2h": "2H",
    "1h": "1H",
    "15m": "15m",
    "5m": "5m",
}


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>LLM 原K回测实验台</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --panel-2: #f0f3f7;
      --line: #d7dde6;
      --text: #1d2430;
      --muted: #647084;
      --accent: #0f766e;
      --accent-2: #164e63;
      --danger: #b42318;
      --good: #047857;
      --shadow: 0 10px 30px rgba(20, 30, 44, .08);
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-size: 14px;
    }
    header {
      padding: 18px 24px 12px;
      border-bottom: 1px solid var(--line);
      background: #ffffff;
      position: sticky;
      top: 0;
      z-index: 5;
    }
    h1 {
      margin: 0;
      font-size: 20px;
      line-height: 1.2;
      letter-spacing: 0;
    }
    .subtitle {
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
    }
    main {
      display: grid;
      grid-template-columns: minmax(420px, 520px) minmax(0, 1fr);
      gap: 16px;
      padding: 16px;
      min-height: calc(100vh - 74px);
    }
    section, aside {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .left {
      display: flex;
      flex-direction: column;
      min-height: 760px;
    }
    .right {
      display: grid;
      grid-template-rows: auto minmax(280px, 1fr) minmax(180px, 300px);
      gap: 12px;
      background: transparent;
      border: 0;
      box-shadow: none;
    }
    .block {
      padding: 14px;
      border-bottom: 1px solid var(--line);
    }
    .block:last-child { border-bottom: 0; }
    .block h2 {
      margin: 0 0 10px;
      font-size: 14px;
      line-height: 1.2;
      color: #101828;
    }
    label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 5px;
    }
    input, select, textarea, button {
      font: inherit;
    }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      color: var(--text);
      padding: 8px 9px;
      outline: none;
    }
    input:focus, select:focus, textarea:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, .12);
    }
    textarea {
      resize: vertical;
      min-height: 150px;
      line-height: 1.45;
    }
    .grid-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .grid-3 {
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 10px;
    }
    .api-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr);
      gap: 10px;
    }
    .row {
      display: grid;
      grid-template-columns: 1fr 110px 34px;
      gap: 8px;
      align-items: end;
      margin-bottom: 8px;
    }
    .row button {
      height: 36px;
      padding: 0;
    }
    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
    }
    button {
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #fff;
      color: var(--text);
      padding: 8px 11px;
      cursor: pointer;
      min-height: 36px;
    }
    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: white;
    }
    button.secondary {
      background: var(--accent-2);
      border-color: var(--accent-2);
      color: white;
    }
    button:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    .hint {
      margin-top: 7px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .status {
      min-height: 22px;
      font-size: 13px;
      color: var(--muted);
    }
    .status.good { color: var(--good); }
    .status.bad { color: var(--danger); }
    .output-panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
      min-width: 0;
    }
    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: #ffffff;
    }
    .panel-title {
      font-weight: 650;
      font-size: 13px;
    }
    .tabs {
      display: flex;
      gap: 6px;
    }
    .tabs button {
      min-height: 30px;
      padding: 5px 9px;
      font-size: 12px;
      background: #fff;
    }
    .tabs button.active {
      border-color: var(--accent);
      color: var(--accent);
      background: rgba(15, 118, 110, .08);
    }
    pre {
      margin: 0;
      padding: 12px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      line-height: 1.48;
      font-size: 12px;
      height: 100%;
      background: #fbfcfe;
    }
    .answer {
      font-size: 13px;
      background: #fff;
    }
    .history {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
      min-height: 0;
    }
    .history-list {
      overflow: auto;
      height: calc(100% - 42px);
      min-height: 120px;
    }
    .history-item {
      border-bottom: 1px solid var(--line);
      padding: 8px 10px;
      cursor: pointer;
    }
    .history-item:hover { background: var(--panel-2); }
    .history-item strong {
      display: block;
      font-size: 12px;
      margin-bottom: 3px;
    }
    .history-item span {
      color: var(--muted);
      font-size: 12px;
    }
    .meta-line {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 12px;
      padding: 0 12px 10px;
      border-bottom: 1px solid var(--line);
      background: #fff;
    }
    .pill {
      display: inline-block;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 7px;
      background: #fff;
    }
    .model-choice-list {
      display: grid;
      gap: 7px;
      margin-top: 8px;
      max-height: 150px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px;
      background: #fbfcfe;
    }
    .model-choice {
      display: grid;
      grid-template-columns: 18px minmax(0, 1fr);
      gap: 7px;
      align-items: start;
      color: var(--text);
      font-size: 12px;
      margin: 0;
    }
    .model-choice input {
      width: auto;
      margin-top: 2px;
    }
    .model-choice span {
      overflow-wrap: anywhere;
      line-height: 1.35;
    }
    .model-empty {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }
    @media (max-width: 960px) {
      main { grid-template-columns: 1fr; }
      .right { grid-template-rows: auto 440px 260px; }
      .api-grid, .grid-2, .grid-3 { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>LLM 原K回测实验台</h1>
    <div class="subtitle">选择历史截止时间和原K窗口，发送给 LLM，返回内容自动归档。<a href="/trend.html" style="color:#0f766e;margin-left:10px">打开趋势判断页</a></div>
  </header>
  <main>
    <section class="left">
      <div class="block">
        <h2>API 设置</h2>
        <div class="api-grid">
          <div>
            <label for="baseUrl">Base URL</label>
            <input id="baseUrl" value="__DEFAULT_BASE_URL__" autocomplete="off" />
          </div>
          <div>
            <label for="apiKey">API Key</label>
            <input id="apiKey" type="password" placeholder="保存在本机浏览器，不归档" autocomplete="off" />
          </div>
        </div>
        <div class="grid-2" style="margin-top:10px">
          <div>
            <label for="model">模型</label>
            <select id="modelSelect"></select>
            <input id="model" value="__DEFAULT_MODEL__" autocomplete="off" style="margin-top:6px" />
            <div class="hint">探测后可在下拉框选择；下方输入框也可手动填模型名。</div>
          </div>
          <div>
            <label for="timeout">超时秒数</label>
            <input id="timeout" type="number" min="10" max="600" value="150" />
          </div>
        </div>
        <details style="margin-top:10px">
          <summary>高级请求通道 / Cloudflare Access</summary>
          <div class="grid-2" style="margin-top:10px">
            <div>
              <label for="cfAccessClientId">CF Access Client ID</label>
              <input id="cfAccessClientId" autocomplete="off" placeholder="可选，Cloudflare Access 服务令牌" />
            </div>
            <div>
              <label for="cfAccessClientSecret">CF Access Client Secret</label>
              <input id="cfAccessClientSecret" type="password" autocomplete="off" placeholder="可选，保存在本机浏览器" />
            </div>
          </div>
          <label for="extraHeaders" style="margin-top:10px">额外请求头 JSON</label>
          <textarea id="extraHeaders" rows="3" placeholder='例如 {"X-Internal-Token":"..."}'></textarea>
          <div class="hint">如果 Base URL 被 Cloudflare challenge 拦截，需要服务端放行 /v1/*，或使用 CF Access Service Token / 专用 API 域名。这里不会归档 secret。</div>
        </details>
        <div class="actions" style="margin-top:10px">
          <button id="fetchModelsBtn">探测模型列表</button>
          <button id="saveModelBtn">保存模型</button>
          <button id="deleteModelBtn">删除保存模型</button>
          <span id="modelStatus" class="status"></span>
        </div>
        <label style="margin-top:12px">
          <input id="multiModelEnabled" type="checkbox" style="width:auto; margin-right:6px" />
          多模型同时请求
        </label>
        <div class="hint">勾选后会把同一份 payload 同时发给下方选中的已保存模型配置；每个模型独立归档。</div>
        <div id="multiModelChoices" class="model-choice-list"></div>
      </div>

      <div class="block">
        <h2>原K窗口</h2>
        <div class="grid-2">
          <div>
            <label for="symbol">交易对</label>
            <input id="symbol" value="ETHUSDT" />
          </div>
          <div>
            <label for="cutoffBjt">历史截止时间，北京时间</label>
            <input id="cutoffBjt" type="datetime-local" step="60" />
          </div>
        </div>
        <div class="hint">截止时间按北京时间解析，后端只取该时刻之前已经收盘的 K 线。</div>
        <div class="grid-2" style="margin-top:10px">
          <div>
            <label for="builtInWindowPreset">实验窗口预设</label>
            <select id="builtInWindowPreset">
              <option value="">选择后替换当前窗口</option>
              <option value="default">default：15m x 20</option>
            </select>
          </div>
          <div>
            <label>&nbsp;</label>
            <button id="applyBuiltInWindowPresetBtn" type="button">应用预设</button>
          </div>
        </div>
        <div class="hint">内置窗口只是最小示例。你可以按研究需要保存自己的窗口配置。</div>
        <div class="grid-2" style="margin-top:10px">
          <div>
            <label for="windowTemplateName">窗口配置标签</label>
            <input id="windowTemplateName" placeholder="例如：短线 5m/15m/1h" />
          </div>
          <div>
            <label for="windowTemplateSelect">已保存窗口配置</label>
            <select id="windowTemplateSelect"></select>
          </div>
        </div>
        <div class="actions" style="margin:10px 0">
          <button id="saveWindowTemplateBtn">保存/修改窗口配置</button>
          <button id="loadWindowTemplateBtn">载入窗口配置</button>
          <button id="deleteWindowTemplateBtn">删除窗口配置</button>
          <span id="windowTemplateStatus" class="status"></span>
        </div>
        <label style="margin-top:10px">
          <input id="includeKlines" type="checkbox" checked style="width:auto; margin-right:6px" />
          提交原K窗口
        </label>
        <div class="hint">取消勾选后，不会提交任何原K，只提交你的提示词和附加说明。</div>
        <label style="margin-top:8px">
          <input id="includePartialKline" type="checkbox" checked style="width:auto; margin-right:6px" />
          包含当前未收盘K线
        </label>
        <div class="hint">勾选后会用截止时间前已完成的1分钟线临时聚合当前周期K线，并标记 is_partial=true。</div>
        <div id="windowRows" style="margin-top:10px"></div>
        <div class="actions">
          <button id="addWindowBtn">增加周期</button>
          <button id="previewBtn" class="secondary">预览 payload</button>
          <button id="updateKlinesBtn">更新K线到最新</button>
          <span id="previewStatus" class="status"></span>
        </div>
        <div id="updateStatus" class="status"></div>
      </div>

      <div class="block">
        <h2>提示词</h2>
        <div class="grid-2">
          <div>
            <label for="builtInPromptPreset">内置提示词预设</label>
            <select id="builtInPromptPreset">
              <option value="">无内置提示词预设</option>
            </select>
          </div>
          <div>
            <label>&nbsp;</label>
            <button id="applyBuiltInPromptPresetBtn" type="button">应用提示词预设</button>
          </div>
        </div>
        <div class="hint">开源版只提供基础默认提示词。你可以在本机浏览器保存自己的模板。</div>
        <div class="grid-2">
          <div>
            <label for="promptTemplateName">模板名称</label>
            <input id="promptTemplateName" placeholder="例如：3推1日线判断" />
          </div>
          <div>
            <label for="promptTemplateSelect">已保存模板</label>
            <select id="promptTemplateSelect"></select>
          </div>
        </div>
        <div class="actions" style="margin:10px 0">
          <button id="savePromptBtn">保存模板</button>
          <button id="loadPromptBtn">载入模板</button>
          <button id="deletePromptBtn">删除模板</button>
          <span id="promptTemplateStatus" class="status"></span>
        </div>
        <label for="systemPrompt">System Prompt</label>
        <textarea id="systemPrompt">__DEFAULT_SYSTEM_PROMPT__</textarea>
        <label for="userPrompt" style="margin-top:10px">附加用户说明</label>
        <textarea id="userPrompt" style="min-height:100px">__DEFAULT_USER_PROMPT__</textarea>
        <div class="hint" style="margin-top:8px">默认提示词只用于研究和复盘，不构成交易建议。请结合自己的验证结果判断。</div>
      </div>

      <div class="block">
        <div class="actions">
          <button id="sendBtn" class="primary">真实请求 LLM 并归档</button>
          <button id="reloadHistoryBtn">刷新历史</button>
          <span id="sendStatus" class="status"></span>
        </div>
      </div>
    </section>

    <aside class="right">
      <div class="output-panel">
        <div class="panel-head">
          <div class="panel-title">本次记录</div>
          <div id="recordId" class="status"></div>
        </div>
        <div id="metaLine" class="meta-line"></div>
      </div>

      <div class="output-panel">
        <div class="panel-head">
          <div class="panel-title">输出</div>
          <div class="tabs">
            <button class="tabBtn active" data-tab="answer">LLM 返回</button>
            <button class="tabBtn" data-tab="payload">Payload</button>
            <button class="tabBtn" data-tab="request">请求体</button>
            <button class="tabBtn" data-tab="meta">Meta</button>
          </div>
        </div>
        <pre id="answerView" class="answer"></pre>
        <pre id="payloadView" style="display:none"></pre>
        <pre id="requestView" style="display:none"></pre>
        <pre id="metaView" style="display:none"></pre>
      </div>

      <div class="history">
        <div class="panel-head">
          <div class="panel-title">归档历史</div>
          <div class="status" id="historyStatus"></div>
        </div>
        <div id="historyList" class="history-list"></div>
      </div>

      <div class="history">
        <div class="panel-head">
          <div class="panel-title">实验排行榜</div>
          <button id="reloadLeaderboardBtn" class="secondary" style="padding:5px 8px">刷新</button>
        </div>
        <div class="hint">口径：chosen_horizon 扣 12bps，并和同批最佳简单基线对比。</div>
        <div id="leaderboardList" class="history-list"></div>
      </div>
    </aside>
  </main>

  <script>
    const TF_OPTIONS = __TIMEFRAME_OPTIONS__;
    let activeTab = "answer";
    let lastPreview = null;
    let probedModels = [];
    const STORAGE_KEYS = {
      settings: "llmRawKlineWebUi.settings.v2",
      models: "llmRawKlineWebUi.modelProfiles.v2",
      promptTemplates: "llmRawKlineWebUi.promptTemplates.v1",
      windowTemplates: "llmRawKlineWebUi.windowTemplates.v1",
    };
    const BUILT_IN_WINDOW_PRESETS = {
      default: [
        { timeframe: "15m", count: 20 },
      ],
    };
    const BUILT_IN_PROMPT_PRESETS = {};

    function $(id) { return document.getElementById(id); }
    function nowSystemLocalInput() {
      const now = new Date();
      now.setSeconds(0, 0);
      now.setMinutes(Math.floor(now.getMinutes() / 5) * 5);
      const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
      return local.toISOString().slice(0, 16);
    }
    function setStatus(id, text, kind="") {
      const el = $(id);
      el.textContent = text || "";
      el.className = "status " + kind;
    }
    function pretty(obj) {
      return JSON.stringify(obj, null, 2);
    }
    function readStorageJson(key, fallback) {
      try {
        const raw = localStorage.getItem(key);
        return raw ? JSON.parse(raw) : fallback;
      } catch {
        return fallback;
      }
    }
    function writeStorageJson(key, value) {
      localStorage.setItem(key, JSON.stringify(value));
    }
    function hostLabel(baseUrl) {
      try { return new URL(baseUrl).host || baseUrl; } catch { return baseUrl || "unknown"; }
    }
    function profileId(baseUrl, model) {
      return `${(baseUrl || "").trim()}|||${(model || "").trim()}`;
    }
    function getSavedSettings() {
      return readStorageJson(STORAGE_KEYS.settings, {});
    }
    function saveSettings(silent=false) {
      const settings = {
        base_url: $("baseUrl").value.trim(),
        api_key: $("apiKey").value,
        model: $("model").value.trim(),
        timeout: Number($("timeout").value || 150),
        cf_access_client_id: $("cfAccessClientId").value.trim(),
        cf_access_client_secret: $("cfAccessClientSecret").value,
        extra_headers: $("extraHeaders").value,
        include_klines: $("includeKlines").checked,
        include_partial_kline: $("includePartialKline").checked,
        multi_model_enabled: $("multiModelEnabled").checked,
        multi_model_ids: collectSelectedMultiModelIds(),
      };
      writeStorageJson(STORAGE_KEYS.settings, settings);
      if (!silent) setStatus("modelStatus", "API 设置已保存到本机浏览器", "good");
    }
    function loadSettings() {
      const settings = getSavedSettings();
      if (settings.base_url) $("baseUrl").value = settings.base_url;
      if (settings.api_key) $("apiKey").value = settings.api_key;
      if (settings.model) $("model").value = settings.model;
      if (settings.timeout) $("timeout").value = settings.timeout;
      if (settings.cf_access_client_id) $("cfAccessClientId").value = settings.cf_access_client_id;
      if (settings.cf_access_client_secret) $("cfAccessClientSecret").value = settings.cf_access_client_secret;
      if (settings.extra_headers) $("extraHeaders").value = settings.extra_headers;
      if (typeof settings.include_klines === "boolean") $("includeKlines").checked = settings.include_klines;
      if (typeof settings.include_partial_kline === "boolean") $("includePartialKline").checked = settings.include_partial_kline;
      if (typeof settings.multi_model_enabled === "boolean") $("multiModelEnabled").checked = settings.multi_model_enabled;
    }
    function getSavedModels() {
      const rows = readStorageJson(STORAGE_KEYS.models, []);
      return Array.isArray(rows) ? rows.filter(x => x && x.base_url && x.model) : [];
    }
    function setSavedModels(rows) {
      writeStorageJson(STORAGE_KEYS.models, rows);
    }
    function renderSavedModels(models=probedModels) {
      probedModels = Array.isArray(models) ? models : [];
      const select = $("modelSelect");
      const currentBase = $("baseUrl").value.trim();
      const currentModel = $("model").value.trim();
      const currentId = profileId(currentBase, currentModel);
      select.innerHTML = "";
      const blank = document.createElement("option");
      blank.value = "";
      blank.textContent = "选择模型或保存配置";
      select.appendChild(blank);

      const saved = getSavedModels();
      if (saved.length) {
        const group = document.createElement("optgroup");
        group.label = "已保存配置";
        for (const profile of saved) {
          const opt = document.createElement("option");
          opt.value = profile.id || profileId(profile.base_url, profile.model);
          opt.dataset.kind = "saved";
          opt.textContent = `${profile.model} @ ${hostLabel(profile.base_url)}`;
          if ((profile.id || profileId(profile.base_url, profile.model)) === currentId) opt.selected = true;
          group.appendChild(opt);
        }
        select.appendChild(group);
      }

      const probed = probedModels.filter(x => saved.every(p => p.model !== x || p.base_url !== currentBase));
      if (probed.length) {
        const group = document.createElement("optgroup");
        group.label = "探测到的模型";
        for (const model of probed) {
          const opt = document.createElement("option");
          opt.value = model;
          opt.dataset.kind = "probed";
          opt.textContent = model;
          if (!select.value && model === currentModel) opt.selected = true;
          group.appendChild(opt);
        }
        select.appendChild(group);
      }
      renderMultiModelChoices();
    }
    function collectSelectedMultiModelIds() {
      return [...document.querySelectorAll("#multiModelChoices input[type=checkbox]:checked")].map(x => x.value);
    }
    function renderMultiModelChoices() {
      const box = $("multiModelChoices");
      if (!box) return;
      const saved = getSavedModels();
      const settings = getSavedSettings();
      const selected = new Set(Array.isArray(settings.multi_model_ids) ? settings.multi_model_ids : []);
      box.innerHTML = "";
      if (!saved.length) {
        const div = document.createElement("div");
        div.className = "model-empty";
        div.textContent = "还没有保存模型。先填写 Base URL / Key / 模型并点“保存模型”。";
        box.appendChild(div);
        return;
      }
      for (const profile of saved) {
        const id = profile.id || profileId(profile.base_url, profile.model);
        const label = document.createElement("label");
        label.className = "model-choice";
        const checked = selected.has(id) ? "checked" : "";
        label.innerHTML = `
          <input type="checkbox" value="${htmlEscape(id)}" ${checked} />
          <span>${htmlEscape(profile.model)} @ ${htmlEscape(hostLabel(profile.base_url))}</span>
        `;
        label.querySelector("input").onchange = () => saveSettings(true);
        box.appendChild(label);
      }
    }
    function htmlEscape(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;",
      }[ch]));
    }
    function saveCurrentModel() {
      const baseUrl = $("baseUrl").value.trim();
      const model = $("model").value.trim();
      if (!baseUrl || !model) {
        setStatus("modelStatus", "Base URL 和模型不能为空", "bad");
        return;
      }
      const id = profileId(baseUrl, model);
      const rows = getSavedModels().filter(x => (x.id || profileId(x.base_url, x.model)) !== id);
      rows.unshift({
        id,
        base_url: baseUrl,
        api_key: $("apiKey").value,
        model,
        timeout: Number($("timeout").value || 150),
        cf_access_client_id: $("cfAccessClientId").value.trim(),
        cf_access_client_secret: $("cfAccessClientSecret").value,
        extra_headers: $("extraHeaders").value,
        updated_at: new Date().toISOString(),
      });
      setSavedModels(rows.slice(0, 50));
      saveSettings(true);
      renderSavedModels();
      $("modelSelect").value = id;
      setStatus("modelStatus", "已保存模型配置和 key 到本机浏览器", "good");
    }
    function deleteCurrentModel() {
      const selected = $("modelSelect").selectedOptions[0];
      const selectedId = selected && selected.dataset.kind === "saved" ? selected.value : profileId($("baseUrl").value.trim(), $("model").value.trim());
      const before = getSavedModels();
      const after = before.filter(x => (x.id || profileId(x.base_url, x.model)) !== selectedId);
      setSavedModels(after);
      renderSavedModels();
      setStatus("modelStatus", before.length === after.length ? "没有匹配的保存模型" : "已删除保存模型", before.length === after.length ? "bad" : "good");
    }
    function selectModelOrProfile() {
      const opt = $("modelSelect").selectedOptions[0];
      if (!opt || !opt.value) return;
      if (opt.dataset.kind === "saved") {
        const profile = getSavedModels().find(x => (x.id || profileId(x.base_url, x.model)) === opt.value);
        if (!profile) return;
        $("baseUrl").value = profile.base_url || "";
        $("apiKey").value = profile.api_key || "";
        $("model").value = profile.model || "";
        $("timeout").value = profile.timeout || $("timeout").value;
        $("cfAccessClientId").value = profile.cf_access_client_id || "";
        $("cfAccessClientSecret").value = profile.cf_access_client_secret || "";
        $("extraHeaders").value = profile.extra_headers || "";
        saveSettings(true);
        setStatus("modelStatus", "已载入保存模型配置", "good");
        return;
      }
      if (opt.dataset.kind === "probed") {
        $("model").value = opt.value;
        saveSettings(true);
      }
    }
    function getPromptTemplates() {
      const rows = readStorageJson(STORAGE_KEYS.promptTemplates, []);
      return Array.isArray(rows) ? rows.filter(x => x && x.name) : [];
    }
    function setPromptTemplates(rows) {
      writeStorageJson(STORAGE_KEYS.promptTemplates, rows);
    }
    function renderPromptTemplates() {
      const select = $("promptTemplateSelect");
      const current = select.value;
      select.innerHTML = "";
      const blank = document.createElement("option");
      blank.value = "";
      blank.textContent = "选择提示词模板";
      select.appendChild(blank);
      for (const item of getPromptTemplates()) {
        const opt = document.createElement("option");
        opt.value = item.name;
        opt.textContent = item.name;
        select.appendChild(opt);
      }
      if (current) select.value = current;
    }
    function savePromptTemplate() {
      const name = $("promptTemplateName").value.trim();
      if (!name) {
        setStatus("promptTemplateStatus", "请先填写模板名称", "bad");
        return;
      }
      const rows = getPromptTemplates().filter(x => x.name !== name);
      rows.unshift({
        name,
        system_prompt: $("systemPrompt").value,
        user_prompt: $("userPrompt").value,
        updated_at: new Date().toISOString(),
      });
      setPromptTemplates(rows.slice(0, 100));
      renderPromptTemplates();
      $("promptTemplateSelect").value = name;
      setStatus("promptTemplateStatus", "模板已保存到本机浏览器", "good");
    }
    function loadPromptTemplate() {
      const name = $("promptTemplateSelect").value || $("promptTemplateName").value.trim();
      const item = getPromptTemplates().find(x => x.name === name);
      if (!item) {
        setStatus("promptTemplateStatus", "没有找到这个模板", "bad");
        return;
      }
      $("promptTemplateName").value = item.name;
      $("systemPrompt").value = item.system_prompt || "";
      $("userPrompt").value = item.user_prompt || "";
      $("promptTemplateSelect").value = item.name;
      setStatus("promptTemplateStatus", "模板已载入", "good");
    }
    function deletePromptTemplate() {
      const name = $("promptTemplateSelect").value || $("promptTemplateName").value.trim();
      const before = getPromptTemplates();
      const after = before.filter(x => x.name !== name);
      setPromptTemplates(after);
      renderPromptTemplates();
      if ($("promptTemplateName").value.trim() === name) $("promptTemplateName").value = "";
      setStatus("promptTemplateStatus", before.length === after.length ? "没有找到这个模板" : "模板已删除", before.length === after.length ? "bad" : "good");
    }
    function applyBuiltInPromptPreset() {
      const name = $("builtInPromptPreset").value;
      const item = BUILT_IN_PROMPT_PRESETS[name];
      if (!item) {
        setStatus("promptTemplateStatus", "请选择一个内置提示词预设", "bad");
        return;
      }
      $("promptTemplateName").value = name;
      $("systemPrompt").value = item.system_prompt;
      $("userPrompt").value = item.user_prompt;
      setStatus("promptTemplateStatus", `已应用内置提示词预设：${name}`, "good");
    }
    function getWindowTemplates() {
      const rows = readStorageJson(STORAGE_KEYS.windowTemplates, []);
      return Array.isArray(rows) ? rows.filter(x => x && x.name && Array.isArray(x.windows)) : [];
    }
    function setWindowTemplates(rows) {
      writeStorageJson(STORAGE_KEYS.windowTemplates, rows);
    }
    function renderWindowTemplates() {
      const select = $("windowTemplateSelect");
      const current = select.value;
      select.innerHTML = "";
      const blank = document.createElement("option");
      blank.value = "";
      blank.textContent = "选择窗口配置";
      select.appendChild(blank);
      for (const item of getWindowTemplates()) {
        const opt = document.createElement("option");
        opt.value = item.name;
        opt.textContent = item.name;
        select.appendChild(opt);
      }
      if (current) select.value = current;
    }
    function clearWindows() {
      $("windowRows").innerHTML = "";
    }
    function applyWindows(windows) {
      clearWindows();
      const rows = Array.isArray(windows) ? windows : [];
      if (!rows.length) {
        addWindow("15m", 60);
        return;
      }
      for (const item of rows) {
        addWindow(item.timeframe || "15m", Number(item.count || 60));
      }
    }
    function applyBuiltInWindowPreset() {
      const name = $("builtInWindowPreset").value;
      if (!name || !BUILT_IN_WINDOW_PRESETS[name]) {
        setStatus("windowTemplateStatus", "请选择一个实验窗口预设", "bad");
        return;
      }
      applyWindows(BUILT_IN_WINDOW_PRESETS[name]);
      $("windowTemplateName").value = name;
      saveSettings(true);
      setStatus("windowTemplateStatus", `已应用内置窗口预设：${name}`, "good");
    }
    function saveWindowTemplate() {
      const name = $("windowTemplateName").value.trim();
      if (!name) {
        setStatus("windowTemplateStatus", "请先填写窗口配置标签", "bad");
        return;
      }
      const windows = collectWindows();
      if (!windows.length) {
        setStatus("windowTemplateStatus", "至少需要一个周期窗口", "bad");
        return;
      }
      const rows = getWindowTemplates().filter(x => x.name !== name);
      rows.unshift({
        name,
        include_klines: $("includeKlines").checked,
        include_partial_kline: $("includePartialKline").checked,
        windows,
        updated_at: new Date().toISOString(),
      });
      setWindowTemplates(rows.slice(0, 100));
      renderWindowTemplates();
      $("windowTemplateSelect").value = name;
      setStatus("windowTemplateStatus", "窗口配置已保存/修改", "good");
    }
    function loadWindowTemplate() {
      const name = $("windowTemplateSelect").value || $("windowTemplateName").value.trim();
      const item = getWindowTemplates().find(x => x.name === name);
      if (!item) {
        setStatus("windowTemplateStatus", "没有找到这个窗口配置", "bad");
        return;
      }
      $("windowTemplateName").value = item.name;
      $("includeKlines").checked = typeof item.include_klines === "boolean" ? item.include_klines : true;
      $("includePartialKline").checked = typeof item.include_partial_kline === "boolean" ? item.include_partial_kline : true;
      applyWindows(item.windows);
      $("windowTemplateSelect").value = item.name;
      saveSettings(true);
      setStatus("windowTemplateStatus", "窗口配置已载入", "good");
    }
    function deleteWindowTemplate() {
      const name = $("windowTemplateSelect").value || $("windowTemplateName").value.trim();
      const before = getWindowTemplates();
      const after = before.filter(x => x.name !== name);
      setWindowTemplates(after);
      renderWindowTemplates();
      if ($("windowTemplateName").value.trim() === name) $("windowTemplateName").value = "";
      setStatus("windowTemplateStatus", before.length === after.length ? "没有找到这个窗口配置" : "窗口配置已删除", before.length === after.length ? "bad" : "good");
    }
    function addWindow(tf="15m", count=60) {
      const box = $("windowRows");
      const row = document.createElement("div");
      row.className = "row";
      const opts = TF_OPTIONS.map(x => `<option value="${x.value}" ${x.value===tf ? "selected" : ""}>${x.label}</option>`).join("");
      row.innerHTML = `
        <div>
          <label>周期</label>
          <select class="tfSelect">${opts}</select>
        </div>
        <div>
          <label>根数</label>
          <input class="countInput" type="number" min="1" max="3000" value="${count}" />
        </div>
        <button title="删除" type="button">×</button>
      `;
      row.querySelector("button").onclick = () => {
        if (box.children.length > 1) row.remove();
      };
      box.appendChild(row);
    }
    function collectWindows() {
      return [...document.querySelectorAll("#windowRows .row")].map(row => ({
        timeframe: row.querySelector(".tfSelect").value,
        count: Number(row.querySelector(".countInput").value || 0),
      })).filter(x => x.timeframe && x.count > 0);
    }
    function collectBase() {
      const includeKlines = $("includeKlines").checked;
      return {
        symbol: $("symbol").value.trim() || "ETHUSDT",
        cutoff_bjt: $("cutoffBjt").value,
        include_klines: includeKlines,
        include_partial_kline: includeKlines && $("includePartialKline").checked,
        windows: includeKlines ? collectWindows() : [],
        system_prompt: $("systemPrompt").value,
        user_prompt: $("userPrompt").value,
      };
    }
    function collectRequest() {
      return {
        ...collectBase(),
        base_url: $("baseUrl").value.trim(),
        api_key: $("apiKey").value,
        model: $("model").value.trim(),
        timeout: Number($("timeout").value || 150),
        cf_access_client_id: $("cfAccessClientId").value.trim(),
        cf_access_client_secret: $("cfAccessClientSecret").value,
        extra_headers: $("extraHeaders").value,
        multi_model_enabled: $("multiModelEnabled").checked,
        model_profiles: collectSelectedModelProfiles(),
      };
    }
    function collectSelectedModelProfiles() {
      if (!$("multiModelEnabled").checked) return [];
      const selected = new Set(collectSelectedMultiModelIds());
      return getSavedModels()
        .filter(profile => selected.has(profile.id || profileId(profile.base_url, profile.model)))
        .map(profile => ({
          id: profile.id || profileId(profile.base_url, profile.model),
          base_url: profile.base_url,
          api_key: profile.api_key || "",
          model: profile.model,
          timeout: Number(profile.timeout || $("timeout").value || 150),
          cf_access_client_id: profile.cf_access_client_id || "",
          cf_access_client_secret: profile.cf_access_client_secret || "",
          extra_headers: profile.extra_headers || "",
        }));
    }
    function renderRecord(record) {
      if (record && record.type === "multi_model_batch") {
        renderBatch({batch: record.batch || {}, records: record.records || []});
        if (record.id) $("recordId").textContent = `id: ${record.id}`;
        return;
      }
      $("recordId").textContent = record.id ? `id: ${record.id}` : "";
      $("answerView").textContent = record.answer || "";
      $("payloadView").textContent = pretty(record.payload || {});
      $("requestView").textContent = pretty(record.request_for_llm || {});
      $("metaView").textContent = pretty(record.meta || {});
      const payload = record.payload || {};
      const spans = [];
      if (payload.cutoff_bjt) spans.push(`<span class="pill">截止 ${payload.cutoff_bjt}</span>`);
      if (payload.cutoff_utc) spans.push(`<span class="pill">UTC ${payload.cutoff_utc}</span>`);
      if (payload.windows) spans.push(`<span class="pill">${payload.windows.map(x => x.timeframe + ":" + (x.requested_closed_count || x.count) + (x.partial_count ? "+partial" : "")).join(" / ")}</span>`);
      if (record.meta && record.meta.model) spans.push(`<span class="pill">模型 ${record.meta.model}</span>`);
      $("metaLine").innerHTML = spans.join("");
    }
    function renderBatch(data) {
      const records = data.records || [];
      const batch = data.batch || {};
      $("recordId").textContent = batch.id ? `batch: ${batch.id}` : "";
      const parts = [];
      for (const record of records) {
        const meta = record.meta || {};
        const title = `${meta.ok === false ? "失败" : "完成"} · ${meta.model || ""} @ ${hostLabel(meta.base_url || "")}`;
        parts.push(`## ${title}\n\n${record.answer || meta.error || ""}`.trim());
      }
      $("answerView").textContent = parts.join("\n\n---\n\n");
      const first = records[0] || {};
      $("payloadView").textContent = pretty(first.payload || {});
      $("requestView").textContent = pretty((records || []).map(record => record.request_for_llm || {}));
      $("metaView").textContent = pretty({batch, records: records.map(record => record.meta || {})});
      const okCount = records.filter(x => (x.meta || {}).ok !== false).length;
      const spans = [
        `<span class="pill">多模型 ${records.length} 个</span>`,
        `<span class="pill">成功 ${okCount} / 失败 ${records.length - okCount}</span>`,
      ];
      if (first.payload && first.payload.cutoff_bjt) spans.push(`<span class="pill">截止 ${first.payload.cutoff_bjt}</span>`);
      $("metaLine").innerHTML = spans.join("");
    }
    async function api(path, body) {
      const resp = await fetch(path, {
        method: body ? "POST" : "GET",
        headers: body ? {"Content-Type": "application/json"} : {},
        body: body ? JSON.stringify(body) : undefined,
      });
      const text = await resp.text();
      let data;
      try { data = JSON.parse(text); } catch { data = {error: text}; }
      if (!resp.ok) throw new Error(data.error || data.detail || text || resp.statusText);
      return data;
    }
    async function fetchModels() {
      setStatus("modelStatus", "探测中...");
      try {
        const data = await api("/api/models", {
          base_url: $("baseUrl").value.trim(),
          api_key: $("apiKey").value,
          timeout: Number($("timeout").value || 60),
          cf_access_client_id: $("cfAccessClientId").value.trim(),
          cf_access_client_secret: $("cfAccessClientSecret").value,
          extra_headers: $("extraHeaders").value,
        });
        probedModels = data.models || [];
        if (data.models && data.models.length) {
          const current = $("model").value.trim();
          const preferred = data.models.includes(current) ? current : data.models[0];
          $("model").value = preferred;
        }
        renderSavedModels(probedModels);
        saveSettings(true);
        setStatus("modelStatus", `发现 ${data.models.length} 个模型`, "good");
      } catch (err) {
        setStatus("modelStatus", err.message, "bad");
      }
    }
    async function updateKlines() {
      setStatus("updateStatus", "更新中，会拉取 Binance 最新1m并重建所有周期...");
      $("updateKlinesBtn").disabled = true;
      try {
        const data = await api("/api/update-klines", {symbol: $("symbol").value.trim() || "ETHUSDT"});
        const r = data.result || {};
        const report = r.report || {};
        setStatus("updateStatus", `更新完成：新增 ${r.fetched_1m_rows || 0} 根1m，最新 ${report.end_bjt || ""}，状态 ${report.status || ""}`, "good");
      } catch (err) {
        setStatus("updateStatus", err.message, "bad");
      } finally {
        $("updateKlinesBtn").disabled = false;
      }
    }
    async function preview() {
      setStatus("previewStatus", "生成中...");
      try {
        const data = await api("/api/preview", collectBase());
        lastPreview = data;
        renderRecord({payload: data.payload, request_for_llm: data.request_for_llm, meta: data.meta, answer: ""});
        setStatus("previewStatus", "已生成", "good");
      } catch (err) {
        setStatus("previewStatus", err.message, "bad");
      }
    }
    async function send() {
      const req = collectRequest();
      if (req.multi_model_enabled && !req.model_profiles.length) {
        setStatus("sendStatus", "请先勾选至少一个已保存模型", "bad");
        return;
      }
      if (!req.multi_model_enabled && !req.api_key) {
        setStatus("sendStatus", "请先输入 API key", "bad");
        return;
      }
      setStatus("sendStatus", "检查K线更新并请求中...");
      $("sendBtn").disabled = true;
      saveSettings(true);
      try {
        const data = await api(req.multi_model_enabled ? "/api/send-multi" : "/api/send", req);
        if (req.multi_model_enabled) {
          renderRecord(data.batch_record || {type: "multi_model_batch", batch: data.batch || {}, records: data.records || []});
          const okCount = (data.records || []).filter(x => (x.meta || {}).ok !== false).length;
          setStatus("sendStatus", `多模型完成：成功 ${okCount}/${(data.records || []).length}，已归档`, okCount ? "good" : "bad");
        } else {
          renderRecord(data.record);
          setStatus("sendStatus", "已完成并归档", "good");
        }
        await loadHistory();
      } catch (err) {
        setStatus("sendStatus", err.message, "bad");
      } finally {
        $("sendBtn").disabled = false;
      }
    }
    async function loadHistory() {
      setStatus("historyStatus", "读取中...");
      try {
        const data = await api("/api/history");
        const list = $("historyList");
        list.innerHTML = "";
        for (const item of data.items || []) {
          const div = document.createElement("div");
          div.className = "history-item";
          const windows = (item.windows || []).map(x => x.timeframe + ":" + (x.requested_closed_count || x.count) + (x.partial_count ? "+partial" : "")).join(" / ");
          const modelText = item.type === "multi_model_batch" ? `多模型汇总 ${item.ok_count || 0}/${item.count || 0}` : (item.model || "");
          div.innerHTML = `<strong>${item.created_bjt || item.id}</strong><span>${modelText} · ${item.cutoff_bjt || ""} · ${windows}</span>`;
          div.onclick = async () => {
            const record = await api("/api/history/" + encodeURIComponent(item.id));
            renderRecord(record.record);
          };
          list.appendChild(div);
        }
        setStatus("historyStatus", `${(data.items || []).length} 条`);
      } catch (err) {
        setStatus("historyStatus", err.message, "bad");
      }
    }
    async function loadLeaderboard() {
      const box = $("leaderboardList");
      box.innerHTML = "<div class='hint'>加载中...</div>";
      try {
        const data = await api("/api/leaderboard");
        const rows = data.rows || [];
        box.innerHTML = "";
        if (!rows.length) {
          box.innerHTML = "<div class='hint'>暂无排行榜，先运行 scripts/llm_intraday_prompt_leaderboard.py</div>";
          return;
        }
        for (const row of rows.slice(0, 8)) {
          const div = document.createElement("div");
          div.className = "history-item";
          const edge = Number(row.edge_vs_best_baseline_bps || 0);
          const edgeText = Number.isFinite(edge) ? `${edge.toFixed(2)}bps` : "";
          const avg = Number(row.avg_bps_cost12 || 0);
          const avgText = Number.isFinite(avg) ? `${avg.toFixed(2)}bps` : "";
          const verdict = row.verdict || "UNKNOWN";
          div.innerHTML = `
            <strong>${htmlEscape(verdict)} · ${htmlEscape(row.prompt)} / ${htmlEscape(row.window)} · ${htmlEscape(row.split)}</strong>
            <span>${htmlEscape(row.run_id)}</span>
            <span>扣成本均值 ${avgText} · 最佳基线 ${htmlEscape(row.best_baseline || "")} ${htmlEscape(row.best_baseline_bps || "")} · 超额 ${edgeText}</span>
            <span>${htmlEscape(row.verdict_reason || "")}</span>
          `;
          box.appendChild(div);
        }
      } catch (err) {
        box.innerHTML = `<div class='hint'>排行榜加载失败：${htmlEscape(err.message || err)}</div>`;
      }
    }
    document.querySelectorAll(".tabBtn").forEach(btn => {
      btn.onclick = () => {
        activeTab = btn.dataset.tab;
        document.querySelectorAll(".tabBtn").forEach(x => x.classList.toggle("active", x === btn));
        for (const id of ["answer", "payload", "request", "meta"]) {
          $(id + "View").style.display = id === activeTab ? "block" : "none";
        }
      };
    });
    $("fetchModelsBtn").onclick = fetchModels;
    $("saveModelBtn").onclick = saveCurrentModel;
    $("deleteModelBtn").onclick = deleteCurrentModel;
    $("modelSelect").onchange = selectModelOrProfile;
    $("savePromptBtn").onclick = savePromptTemplate;
    $("loadPromptBtn").onclick = loadPromptTemplate;
    $("deletePromptBtn").onclick = deletePromptTemplate;
    $("applyBuiltInPromptPresetBtn").onclick = applyBuiltInPromptPreset;
    $("promptTemplateSelect").onchange = () => {
      if ($("promptTemplateSelect").value) $("promptTemplateName").value = $("promptTemplateSelect").value;
    };
    $("saveWindowTemplateBtn").onclick = saveWindowTemplate;
    $("loadWindowTemplateBtn").onclick = loadWindowTemplate;
    $("deleteWindowTemplateBtn").onclick = deleteWindowTemplate;
    $("applyBuiltInWindowPresetBtn").onclick = applyBuiltInWindowPreset;
    $("windowTemplateSelect").onchange = () => {
      if ($("windowTemplateSelect").value) $("windowTemplateName").value = $("windowTemplateSelect").value;
    };
    $("previewBtn").onclick = preview;
    $("sendBtn").onclick = send;
    $("reloadHistoryBtn").onclick = loadHistory;
    $("reloadLeaderboardBtn").onclick = loadLeaderboard;
    $("updateKlinesBtn").onclick = updateKlines;
    $("addWindowBtn").onclick = () => addWindow("15m", 60);
    for (const id of ["baseUrl", "apiKey", "model", "timeout", "cfAccessClientId", "cfAccessClientSecret", "extraHeaders"]) {
      $(id).addEventListener("input", () => {
        saveSettings(true);
        if (id === "baseUrl" || id === "model") renderSavedModels();
      });
    }
    $("includeKlines").addEventListener("change", () => saveSettings(true));
    $("includePartialKline").addEventListener("change", () => saveSettings(true));
    $("multiModelEnabled").addEventListener("change", () => saveSettings(true));
    $("cutoffBjt").value = nowSystemLocalInput();
    loadSettings();
    renderSavedModels();
    renderPromptTemplates();
    renderWindowTemplates();
    applyWindows(BUILT_IN_WINDOW_PRESETS.default);
    loadHistory();
    loadLeaderboard();
  </script>
</body>
</html>
"""


TREND_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>ETH 多周期趋势判断</title>
  <style>
    :root {
      color-scheme: light;
      --bg:#f6f7f9; --panel:#fff; --line:#d7dde6; --text:#1d2430; --muted:#647084;
      --accent:#0f766e; --danger:#b42318; --good:#047857; --warn:#a16207;
      --shadow:0 10px 30px rgba(20,30,44,.08);
      font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    }
    *{box-sizing:border-box}
    body{margin:0;background:var(--bg);color:var(--text);font-size:14px}
    header{padding:16px 22px;background:#fff;border-bottom:1px solid var(--line);position:sticky;top:0;z-index:2}
    h1{margin:0;font-size:20px;letter-spacing:0}
    .subtitle{margin-top:6px;color:var(--muted);font-size:13px}
    main{display:grid;grid-template-columns:360px minmax(0,1fr);gap:14px;padding:14px}
    section,.panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow)}
    section{padding:14px;height:max-content}
    label{display:block;color:var(--muted);font-size:12px;margin:10px 0 5px}
    input,button{font:inherit}
    input{width:100%;border:1px solid var(--line);border-radius:6px;padding:8px 9px;background:#fff;color:var(--text)}
    input:focus{border-color:var(--accent);outline:none;box-shadow:0 0 0 3px rgba(15,118,110,.12)}
    button{border:1px solid var(--accent);background:var(--accent);color:white;border-radius:6px;min-height:36px;padding:8px 11px;cursor:pointer}
    button.secondary{border-color:var(--line);background:#fff;color:var(--text)}
    .actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:12px}
    .status{font-size:13px;color:var(--muted);min-height:20px}
    .status.good{color:var(--good)} .status.bad{color:var(--danger)}
    .grid{display:grid;gap:14px}
    .panel{overflow:hidden}
    .head{display:flex;justify-content:space-between;gap:10px;align-items:center;padding:10px 12px;border-bottom:1px solid var(--line);background:#fff}
    .title{font-weight:650;font-size:13px}
    .body{padding:12px}
    table{width:100%;border-collapse:collapse;font-size:12px}
    th,td{border-bottom:1px solid var(--line);padding:8px;text-align:left;vertical-align:top}
    th{color:var(--muted);font-weight:650;background:#fbfcfe}
    .pill{display:inline-block;border:1px solid var(--line);border-radius:999px;padding:2px 7px;background:#fff;font-size:12px}
    .bull{color:var(--good);font-weight:650}
    .bear{color:var(--danger);font-weight:650}
    .range{color:var(--warn);font-weight:650}
    pre{margin:0;white-space:pre-wrap;word-break:break-word;line-height:1.5;font-size:13px}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}
    @media(max-width:900px){main{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <header>
    <h1>ETH 多周期趋势判断</h1>
    <div class="subtitle">纯脚本计算 EMA 背景、价格结构确认、多周期冲突和交易倾向。</div>
  </header>
  <main>
    <section>
      <label for="symbol">交易对</label>
      <input id="symbol" value="ETHUSDT" />
      <label for="cutoffBjt">历史截止时间，北京时间</label>
      <input id="cutoffBjt" type="datetime-local" step="60" />
      <label style="margin-top:12px">
        <input id="includePartialKline" type="checkbox" checked style="width:auto;margin-right:6px" />
        包含当前未收盘K线
      </label>
      <div class="actions">
        <button id="analyzeBtn">计算趋势</button>
        <button id="updateBtn" class="secondary">先更新K线</button>
      </div>
      <div id="status" class="status"></div>
    </section>
    <div class="grid">
      <div class="panel">
        <div class="head">
          <div class="title">多周期趋势评分</div>
          <div id="meta" class="status"></div>
        </div>
        <div class="body"><div id="scoreTable"></div></div>
      </div>
      <div class="panel">
        <div class="head"><div class="title">结构确认</div></div>
        <div class="body"><pre id="structureView"></pre></div>
      </div>
      <div class="panel">
        <div class="head"><div class="title">综合趋势结论</div></div>
        <div class="body"><pre id="summaryView"></pre></div>
      </div>
      <div class="panel">
        <div class="head"><div class="title">原始 JSON</div></div>
        <div class="body"><pre id="jsonView" class="mono"></pre></div>
      </div>
    </div>
  </main>
  <script>
    function $(id){return document.getElementById(id)}
    function nowSystemLocalInput(){
      const now=new Date(); now.setSeconds(0,0); now.setMinutes(Math.floor(now.getMinutes()/5)*5);
      const local=new Date(now.getTime()-now.getTimezoneOffset()*60000);
      return local.toISOString().slice(0,16);
    }
    function setStatus(text,kind=""){ $("status").textContent=text||""; $("status").className="status "+kind }
    function biasClass(v){ return v==="bullish" ? "bull" : (v==="bearish" ? "bear" : "range") }
    async function api(path, body){
      const resp=await fetch(path,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body||{})});
      const text=await resp.text(); let data; try{data=JSON.parse(text)}catch{data={error:text}}
      if(!resp.ok) throw new Error(data.error||text||resp.statusText);
      return data;
    }
    function render(data){
      $("meta").textContent=`${data.symbol} · ${data.cutoff_bjt}`;
      const rows=data.timeframes||[];
      $("scoreTable").innerHTML=`<table>
        <thead><tr><th>周期</th><th>多/空分</th><th>趋势判断</th><th>结构</th><th>EMA</th><th>最后K线</th></tr></thead>
        <tbody>${rows.map(x=>`<tr>
          <td><strong>${x.label}</strong></td>
          <td><span class="bull">${x.bullScore}</span> / <span class="bear">${x.bearScore}</span></td>
          <td class="${biasClass(x.finalBias)}">${x.trendLabel}</td>
          <td>${x.structureLabel}</td>
          <td>EMA20 ${fmt(x.ema.ema20)}<br>EMA50 ${fmt(x.ema.ema50)}<br>EMA200 ${fmt(x.ema.ema200)}<br>${x.ema.ema20_slope}/${x.ema.ema50_slope}</td>
          <td>${x.last_time_bjt}${x.last_is_partial ? "<br><span class='pill'>未收盘</span>" : ""}<br>close ${fmt(x.close)}</td>
        </tr>`).join("")}</tbody></table>`;
      const s=data.structureConfirmation||{};
      $("structureView").textContent=[
        "高低点结构：", s.highLowStructure||"",
        "",
        "突破/跌破质量：", s.breakoutQuality||"",
        "",
        "回踩质量：", s.pullbackQuality||"",
        "",
        "价格接受/拒绝：", s.priceAcceptance||"",
        "",
        "EMA 与结构是否一致：", s.emaStructureConsistency||"",
      ].join("\n");
      const m=data.summary||{};
      $("summaryView").textContent=[
        `主趋势：${m.mainTrend||""}`,
        `波段方向：${m.swingDirection||""}`,
        `交易方向：${m.tradeDirection||""}`,
        `是否存在多周期冲突：${m.multiTimeframeConflict ? "是" : "否"}`,
        `当前适合：${m.currentAction||""}`,
        "",
        "交易含义：",
        m.tradeMeaning||"",
        "",
        "一句话结论：",
        m.oneSentence||"",
      ].join("\n");
      $("jsonView").textContent=JSON.stringify(data,null,2);
    }
    function fmt(x){ return x===null||x===undefined ? "-" : Number(x).toFixed(2) }
    async function analyze(){
      setStatus("计算中，会先检查K线是否需要更新...");
      $("analyzeBtn").disabled=true;
      try{
        const data=await api("/api/trend",{symbol:$("symbol").value||"ETHUSDT",cutoff_bjt:$("cutoffBjt").value,include_partial_kline:$("includePartialKline").checked});
        render(data); setStatus("已完成","good");
      }catch(err){ setStatus(err.message,"bad") }
      finally{ $("analyzeBtn").disabled=false }
    }
    async function updateKlines(){
      setStatus("更新K线中...");
      $("updateBtn").disabled=true;
      try{ await api("/api/update-klines",{symbol:$("symbol").value||"ETHUSDT"}); setStatus("K线已更新","good") }
      catch(err){ setStatus(err.message,"bad") }
      finally{ $("updateBtn").disabled=false }
    }
    $("cutoffBjt").value=nowSystemLocalInput();
    $("analyzeBtn").onclick=analyze;
    $("updateBtn").onclick=updateKlines;
    analyze();
  </script>
</body>
</html>
"""


def parse_utc(text: str) -> datetime:
    value = str(text or "").strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_bjt_local(text: str) -> datetime:
    value = str(text or "").strip()
    if not value:
        raise ValueError("缺少历史截止时间")
    if re.search(r"(Z|[+-]\d\d:\d\d)$", value):
        return parse_utc(value)
    dt = datetime.fromisoformat(value)
    return dt.replace(tzinfo=timezone(timedelta(hours=8))).astimezone(timezone.utc)


def iso_bjt(dt: datetime) -> str:
    return dt.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S+08:00")


def fnum(value: Any, default: float = math.nan) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def floor_dt_by_minutes(dt: datetime, minutes: int) -> datetime:
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    day_start = dt.replace(hour=0, minute=0)
    elapsed_minutes = int((dt - day_start).total_seconds() // 60)
    return day_start + timedelta(minutes=(elapsed_minutes // minutes) * minutes)


class KlineStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self._lock = threading.Lock()
        self._cache: dict[str, list[dict[str, Any]]] = {}
        self._latest_source_open: dict[str, datetime] = {}

    def available_timeframes(self) -> list[str]:
        out: list[str] = []
        for tf in TIMEFRAMES:
            if (self.root / f"ETHUSDT-{tf}-clean.csv.gz").exists():
                out.append(tf)
        return out

    def clear_cache(self) -> None:
        with self._lock:
            self._cache.clear()
            self._latest_source_open.clear()

    def load(self, symbol: str, timeframe: str) -> list[dict[str, Any]]:
        if symbol.upper() != "ETHUSDT":
            raise ValueError("当前 clean 数据只内置 ETHUSDT")
        if timeframe not in TIMEFRAMES:
            raise ValueError(f"不支持的周期: {timeframe}")
        path = self.root / f"{symbol.upper()}-{timeframe}-clean.csv.gz"
        if not path.exists():
            raise ValueError(f"缺少 clean K线文件: {path}")
        key = f"{symbol.upper()}:{timeframe}"
        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                return cached
            rows: list[dict[str, Any]] = []
            try:
                with gzip.open(path, "rt", encoding="utf-8") as fp:
                    reader = csv.DictReader(fp)
                    for row in reader:
                        dt = parse_utc(row["timestamp_utc"])
                        rows.append(
                            {
                                "time": dt.isoformat(timespec="seconds"),
                                "time_bjt": row.get("timestamp_bjt") or iso_bjt(dt),
                                "dt": dt,
                                "open": fnum(row.get("open")),
                                "high": fnum(row.get("high")),
                                "low": fnum(row.get("low")),
                                "close": fnum(row.get("close")),
                                "volume": fnum(row.get("volume")),
                            }
                        )
            except (EOFError, gzip.BadGzipFile) as exc:
                raise RuntimeError(f"K线压缩文件不完整，请先点击“更新K线到最新”重建: {path}") from exc
            rows.sort(key=lambda x: x["dt"])
            self._cache[key] = rows
            if timeframe == "1m" and rows:
                self._latest_source_open[symbol.upper()] = rows[-1]["dt"]
            return rows

    def load_1m_tail(self, symbol: str) -> list[dict[str, Any]]:
        symbol = symbol.upper()
        tail_path = self.root / f"{symbol}-1m-tail-clean.csv.gz"
        if not tail_path.exists():
            return self.load(symbol, "1m")
        key = f"{symbol}:1m_tail"
        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                return cached
            rows: list[dict[str, Any]] = []
            try:
                with gzip.open(tail_path, "rt", encoding="utf-8") as fp:
                    reader = csv.DictReader(fp)
                    for row in reader:
                        dt = parse_utc(row["timestamp_utc"])
                        rows.append(
                            {
                                "time": dt.isoformat(timespec="seconds"),
                                "time_bjt": row.get("timestamp_bjt") or iso_bjt(dt),
                                "dt": dt,
                                "open": fnum(row.get("open")),
                                "high": fnum(row.get("high")),
                                "low": fnum(row.get("low")),
                                "close": fnum(row.get("close")),
                                "volume": fnum(row.get("volume")),
                            }
                        )
            except (EOFError, gzip.BadGzipFile) as exc:
                raise RuntimeError(f"1m tail压缩文件不完整，请先点击“更新K线到最新”重建: {tail_path}") from exc
            rows.sort(key=lambda x: x["dt"])
            self._cache[key] = rows
            if rows:
                self._latest_source_open[symbol] = rows[-1]["dt"]
            return rows

    def latest_source_open(self, symbol: str) -> datetime | None:
        symbol = symbol.upper()
        with self._lock:
            cached = self._latest_source_open.get(symbol)
        if cached is not None:
            return cached
        meta_path = self.root / "baseline_metadata.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                value = meta.get("target_open_utc")
                if value:
                    latest = parse_utc(str(value))
                    with self._lock:
                        self._latest_source_open[symbol] = latest
                    return latest
            except Exception:
                pass
        report_path = self.root / "data_quality_report.csv"
        if report_path.exists():
            try:
                with report_path.open("r", encoding="utf-8") as fp:
                    row = next(csv.DictReader(fp), None)
                value = row.get("end_utc") if row else None
                if value:
                    latest = parse_utc(str(value))
                    with self._lock:
                        self._latest_source_open[symbol] = latest
                    return latest
            except Exception:
                pass
        rows = self.load(symbol, "1m")
        return rows[-1]["dt"] if rows else None

    def partial_bar(self, symbol: str, timeframe: str, cutoff: datetime) -> dict[str, Any] | None:
        if timeframe == "1m":
            return None
        minutes = TIMEFRAME_MINUTES.get(timeframe)
        if not minutes or minutes < 2:
            return None
        latest_open = self.latest_source_open(symbol)
        if latest_open is None:
            return None
        available_until = min(cutoff, latest_open + timedelta(minutes=1))
        period_start = floor_dt_by_minutes(cutoff, minutes)
        period_end = period_start + timedelta(minutes=minutes)
        if not (period_start < available_until < period_end):
            return None
        one_min_rows = self.load_1m_tail(symbol)
        selected: list[dict[str, Any]] = []
        for row in one_min_rows:
            close_at = row["dt"] + timedelta(minutes=1)
            if period_start <= row["dt"] and close_at <= available_until:
                selected.append(row)
        if not selected:
            return None
        return {
            "time": period_end.isoformat(timespec="seconds"),
            "time_bjt": iso_bjt(period_end),
            "open_time": period_start.isoformat(timespec="seconds"),
            "open_time_bjt": iso_bjt(period_start),
            "partial_until": available_until.isoformat(timespec="seconds"),
            "partial_until_bjt": iso_bjt(available_until),
            "is_partial": True,
            "partial_source": "aggregated_from_completed_1m_bars_before_cutoff",
            "completed_1m_count": len(selected),
            "expected_1m_count": minutes,
            "open": selected[0]["open"],
            "high": max(float(x["high"]) for x in selected),
            "low": min(float(x["low"]) for x in selected),
            "close": selected[-1]["close"],
            "volume": sum(float(x["volume"]) for x in selected),
        }

    def window(self, symbol: str, timeframe: str, cutoff: datetime, count: int, include_partial: bool = False) -> list[dict[str, Any]]:
        if count < 1:
            raise ValueError("K线根数必须大于 0")
        if count > 3000:
            raise ValueError("单个周期最多 3000 根")
        rows = self.load(symbol, timeframe)
        latest_open = self.latest_source_open(symbol)
        selected: list[dict[str, Any]] = []
        for row in rows:
            # 1m clean data keeps Binance's original open timestamp. Resampled
            # files are right-labelled by candle close time.
            complete_at = row["dt"] + timedelta(minutes=1) if timeframe == "1m" else row["dt"]
            if latest_open is not None:
                latest_complete_at = latest_open + timedelta(minutes=1) if timeframe == "1m" else latest_open
                if complete_at > latest_complete_at:
                    continue
            if complete_at <= cutoff:
                selected.append(row)
        selected = selected[-count:]
        if len(selected) < count:
            raise ValueError(f"{timeframe} 在截止时间前只有 {len(selected)} 根，少于请求的 {count} 根")
        out: list[dict[str, Any]] = []
        for row in selected:
            item = {k: v for k, v in row.items() if k != "dt"}
            item["is_partial"] = False
            if timeframe == "1m":
                close_dt = row["dt"] + timedelta(minutes=1)
                item["close_time"] = close_dt.isoformat(timespec="seconds")
                item["close_time_bjt"] = iso_bjt(close_dt)
            out.append(item)
        if include_partial:
            partial = self.partial_bar(symbol, timeframe, cutoff)
            if partial is not None:
                last_time = parse_utc(str(out[-1]["time"])) if out else None
                partial_time = parse_utc(str(partial["time"]))
                if last_time is None or partial_time > last_time:
                    out.append(partial)
        return out


STORE = KlineStore(CLEAN_ROOT)


def safe_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_{os.getpid()}_{int(time.time() * 1000) % 100000:05d}_{uuid.uuid4().hex[:8]}"


def ensure_archive() -> None:
    (ARCHIVE_ROOT / "requests").mkdir(parents=True, exist_ok=True)


def build_payload(data: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    symbol = str(data.get("symbol") or "ETHUSDT").strip().upper()
    cutoff = parse_bjt_local(str(data.get("cutoff_bjt") or ""))
    include_klines = bool(data.get("include_klines", True))
    include_partial_kline = bool(data.get("include_partial_kline", False))
    windows: list[dict[str, Any]] = []
    if include_klines:
        windows_in = data.get("windows") or []
        if not isinstance(windows_in, list) or not windows_in:
            raise ValueError("至少需要一个原K窗口")
        for item in windows_in:
            timeframe = str(item.get("timeframe") or "").strip()
            count = int(item.get("count") or 0)
            klines = STORE.window(symbol, timeframe, cutoff, count, include_partial=include_partial_kline)
            partial_count = sum(1 for kline in klines if kline.get("is_partial"))
            windows.append(
                {
                    "timeframe": timeframe,
                    "timeframe_label": TIMEFRAME_LABELS.get(timeframe, timeframe),
                    "time_semantics": "time=open_time; close_time=time+1m" if timeframe == "1m" else "time=bar_close_time; if is_partial=true then time=current_period_scheduled_close_time",
                    "requested_closed_count": count,
                    "count": len(klines),
                    "partial_count": partial_count,
                    "include_partial_kline": include_partial_kline,
                    "start_utc": klines[0]["time"],
                    "end_utc": klines[-1]["time"],
                    "start_bjt": klines[0]["time_bjt"],
                    "end_bjt": klines[-1]["time_bjt"],
                    "klines": klines,
                }
            )
    data_rules = [
        "不得使用输入窗口之外的数据。",
        "如果证据不足或冲突，应输出观望/不确定。",
    ]
    if include_klines:
        data_rules.insert(0, "所有K线均为历史截止时间之前已经收盘的原始OHLCV。")
        if include_partial_kline:
            data_rules.insert(1, "若K线字段 is_partial=true，该K线不是未来完整K线，而是由截止时间前已完成的1分钟K线临时聚合；只能把它当作当前未收盘K线。")
    else:
        data_rules.insert(0, "本次不包含原K窗口，只根据用户文本与本payload内字段推理。")
    payload = {
        "experiment": "raw_kline_llm_probe",
        "symbol": symbol,
        "include_klines": include_klines,
        "include_partial_kline": include_partial_kline,
        "cutoff_utc": cutoff.isoformat(timespec="seconds"),
        "cutoff_bjt": iso_bjt(cutoff),
        "sequence_order": "oldest_to_newest",
        "windows": windows,
        "user_prompt": str(data.get("user_prompt") or ""),
        "data_rules": data_rules,
    }
    system_prompt = str(data.get("system_prompt") or DEFAULT_SYSTEM_PROMPT)
    user_prompt = str(data.get("user_prompt") or "")
    if "json" not in (system_prompt + "\n" + user_prompt).lower():
        system_prompt = system_prompt.rstrip() + "\n\n" + JSON_OBJECT_RESPONSE_GUARD
    request_for_llm = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False, separators=(",", ":"))},
        ]
    }
    meta = {
        "symbol": symbol,
        "include_klines": include_klines,
        "include_partial_kline": include_partial_kline,
        "cutoff_utc": payload["cutoff_utc"],
        "cutoff_bjt": payload["cutoff_bjt"],
        "windows": [{k: w[k] for k in ("timeframe", "requested_closed_count", "count", "partial_count", "start_bjt", "end_bjt")} for w in windows],
        "kline_source": str(CLEAN_ROOT),
    }
    return payload, request_for_llm, meta


def cutoff_from_request(data: dict[str, Any]) -> datetime:
    return parse_bjt_local(str(data.get("cutoff_bjt") or ""))


def ema_series(values: list[float], period: int) -> list[float | None]:
    if len(values) < period:
        return [None] * len(values)
    out: list[float | None] = [None] * len(values)
    sma = sum(values[:period]) / period
    out[period - 1] = sma
    alpha = 2.0 / (period + 1.0)
    prev = sma
    for i in range(period, len(values)):
        prev = values[i] * alpha + prev * (1.0 - alpha)
        out[i] = prev
    return out


def pct(value: float, base: float) -> float:
    if not base:
        return 0.0
    return value / base * 100.0


def find_swings(rows: list[dict[str, Any]], left: int = 2, right: int = 2) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    highs: list[dict[str, Any]] = []
    lows: list[dict[str, Any]] = []
    if len(rows) < left + right + 1:
        return highs, lows
    for i in range(left, len(rows) - right):
        high = float(rows[i]["high"])
        low = float(rows[i]["low"])
        if all(high >= float(rows[j]["high"]) for j in range(i - left, i + right + 1) if j != i):
            highs.append({"idx": i, "time_bjt": rows[i]["time_bjt"], "price": high})
        if all(low <= float(rows[j]["low"]) for j in range(i - left, i + right + 1) if j != i):
            lows.append({"idx": i, "time_bjt": rows[i]["time_bjt"], "price": low})
    return highs[-8:], lows[-8:]


def slope_label(current: float | None, previous: float | None, close: float) -> tuple[str, float]:
    if current is None or previous is None:
        return "不足", 0.0
    change = pct(current - previous, close)
    if change > 0.08:
        return "明显向上", change
    if change > 0.02:
        return "向上", change
    if change < -0.08:
        return "明显向下", change
    if change < -0.02:
        return "向下", change
    return "走平", change


def score_label(bull_score: int, bear_score: int, close: float, ema20: float | None, ema50: float | None, ema200: float | None) -> str:
    if ema20 is not None and ema50 is not None and ema200 is not None:
        ema_spread = (max(ema20, ema50, ema200) - min(ema20, ema50, ema200)) / close if close else 0
        if ema_spread < 0.012 and abs(close - ema20) / close < 0.01 and abs(close - ema50) / close < 0.012:
            return "震荡：EMA20/50/200缠绕，价格反复穿越均线区"
        if close > ema20 > ema50 and close < ema200:
            return "长期空头背景下的短中期反弹"
        if close < ema20 < ema50 and close > ema200:
            return "长期多头背景下的短中期回调"
    if bull_score >= 5:
        return "强多头趋势"
    if bear_score >= 5:
        return "强空头趋势"
    if bull_score >= 4 and bull_score > bear_score:
        return "多头趋势"
    if bear_score >= 4 and bear_score > bull_score:
        return "空头趋势"
    if bull_score >= 3 and bull_score > bear_score:
        return "偏多 / 修复中"
    if bear_score >= 3 and bear_score > bull_score:
        return "偏空 / 转弱中"
    return "震荡或方向不清"


def analyze_timeframe(symbol: str, timeframe: str, cutoff: datetime, include_partial: bool) -> dict[str, Any]:
    available_rows = [row for row in STORE.load(symbol, timeframe) if (row["dt"] + timedelta(minutes=1) if timeframe == "1m" else row["dt"]) <= cutoff]
    desired_bars = min(TREND_REQUIRED_BARS, len(available_rows))
    if desired_bars < 60 and timeframe not in {"1mo", "1w"}:
        raise ValueError(f"{timeframe} 在截止时间前只有 {desired_bars} 根，无法稳定计算趋势")
    if desired_bars < 12:
        raise ValueError(f"{timeframe} 在截止时间前只有 {desired_bars} 根，无法计算趋势")
    rows = STORE.window(symbol, timeframe, cutoff, desired_bars, include_partial=include_partial)
    closes = [float(x["close"]) for x in rows]
    ema20s = ema_series(closes, 20)
    ema50s = ema_series(closes, 50)
    ema200s = ema_series(closes, 200)
    last = rows[-1]
    close = float(last["close"])
    ema20 = ema20s[-1]
    ema50 = ema50s[-1]
    ema200 = ema200s[-1]
    slope_lookback = min(5, max(1, len(rows) - 1))
    ema20_prev = ema20s[-1 - slope_lookback] if len(ema20s) > slope_lookback else None
    ema50_prev = ema50s[-1 - slope_lookback] if len(ema50s) > slope_lookback else None
    ema20_slope_label, ema20_slope_pct = slope_label(ema20, ema20_prev, close)
    ema50_slope_label, ema50_slope_pct = slope_label(ema50, ema50_prev, close)
    bull_score = 0
    bear_score = 0
    bull_checks = {
        "close_gt_ema20": bool(ema20 is not None and close > ema20),
        "ema20_gt_ema50": bool(ema20 is not None and ema50 is not None and ema20 > ema50),
        "close_gt_ema200": bool(ema200 is not None and close > ema200),
        "ema50_gt_ema200": bool(ema50 is not None and ema200 is not None and ema50 > ema200),
        "ema20_ema50_slope_up": ema20_slope_pct > 0.02 and ema50_slope_pct > 0.02,
    }
    bear_checks = {
        "close_lt_ema20": bool(ema20 is not None and close < ema20),
        "ema20_lt_ema50": bool(ema20 is not None and ema50 is not None and ema20 < ema50),
        "close_lt_ema200": bool(ema200 is not None and close < ema200),
        "ema50_lt_ema200": bool(ema50 is not None and ema200 is not None and ema50 < ema200),
        "ema20_ema50_slope_down": ema20_slope_pct < -0.02 and ema50_slope_pct < -0.02,
    }
    bull_score = sum(1 for v in bull_checks.values() if v)
    bear_score = sum(1 for v in bear_checks.values() if v)
    highs, lows = find_swings(rows[:-1] if last.get("is_partial") else rows)
    last_highs = highs[-3:]
    last_lows = lows[-3:]
    higher_highs = len(last_highs) >= 2 and last_highs[-1]["price"] > last_highs[-2]["price"]
    lower_highs = len(last_highs) >= 2 and last_highs[-1]["price"] < last_highs[-2]["price"]
    higher_lows = len(last_lows) >= 2 and last_lows[-1]["price"] > last_lows[-2]["price"]
    lower_lows = len(last_lows) >= 2 and last_lows[-1]["price"] < last_lows[-2]["price"]
    prev_high = last_highs[-1]["price"] if last_highs else None
    prev_low = last_lows[-1]["price"] if last_lows else None
    breakout_quality = "无明确前高/前低突破信息"
    if prev_high is not None and close > prev_high:
        breakout_quality = f"收盘站上最近摆动高点 {prev_high:.2f}，突破偏有效"
    elif prev_low is not None and close < prev_low:
        breakout_quality = f"收盘跌破最近摆动低点 {prev_low:.2f}，跌破偏有效"
    elif prev_high is not None and close <= prev_high and (float(last["high"]) >= prev_high):
        breakout_quality = f"刺破/接近前高 {prev_high:.2f} 后未站稳，存在上方拒绝"
    elif prev_low is not None and close >= prev_low and (float(last["low"]) <= prev_low):
        breakout_quality = f"刺破/接近前低 {prev_low:.2f} 后收回，存在下方承接"
    pullback_quality = "无明确回踩均线反应"
    if ema20 is not None and float(last["low"]) <= ema20 <= max(float(last["open"]), close) and close > ema20:
        pullback_quality = "回踩/下探 EMA20 后收回，短线承接较好"
    elif ema50 is not None and float(last["low"]) <= ema50 <= max(float(last["open"]), close) and close > ema50:
        pullback_quality = "回踩/下探 EMA50 后收回，中期承接较好"
    elif ema20 is not None and float(last["high"]) >= ema20 >= min(float(last["open"]), close) and close < ema20:
        pullback_quality = "反弹 EMA20 后回落，短线卖压较明显"
    elif ema50 is not None and float(last["high"]) >= ema50 >= min(float(last["open"]), close) and close < ema50:
        pullback_quality = "反弹 EMA50 后回落，中期卖压较明显"
    if higher_highs and higher_lows:
        structure_label = "多头结构：高点抬高、低点抬高"
    elif lower_highs and lower_lows:
        structure_label = "空头结构：高点降低、低点降低"
    elif higher_lows and not higher_highs:
        structure_label = "低点抬高但未突破前高，偏修复/蓄势"
    elif lower_highs and not lower_lows:
        structure_label = "高点降低但未跌破前低，偏压制/震荡"
    else:
        structure_label = "结构震荡或摆动点不足"
    ema_label = score_label(bull_score, bear_score, close, ema20, ema50, ema200)
    conflict = False
    if bull_score >= 4 and "空头结构" in structure_label:
        conflict = True
    if bear_score >= 4 and "多头结构" in structure_label:
        conflict = True
    if conflict:
        trend_label = "趋势冲突：EMA方向和价格结构不一致"
    else:
        trend_label = ema_label
    if conflict:
        final_bias = "conflict"
    elif bull_score >= 3 and bull_score > bear_score and ("多头" in structure_label or "修复" in structure_label or close > (ema20 or close)):
        final_bias = "bullish"
    elif bear_score >= 3 and bear_score > bull_score and ("空头" in structure_label or "压制" in structure_label or close < (ema20 or close)):
        final_bias = "bearish"
    else:
        final_bias = "range"
    return {
        "timeframe": timeframe,
        "label": TREND_LABELS.get(timeframe, timeframe),
        "bars": len(rows),
        "last_time_bjt": last["time_bjt"],
        "last_is_partial": bool(last.get("is_partial")),
        "close": close,
        "ema": {
            "ema20": round(ema20, 4) if ema20 is not None else None,
            "ema50": round(ema50, 4) if ema50 is not None else None,
            "ema200": round(ema200, 4) if ema200 is not None else None,
            "ema20_slope": ema20_slope_label,
            "ema50_slope": ema50_slope_label,
            "ema20_slope_pct": round(ema20_slope_pct, 4),
            "ema50_slope_pct": round(ema50_slope_pct, 4),
        },
        "bullScore": bull_score,
        "bearScore": bear_score,
        "bullChecks": bull_checks,
        "bearChecks": bear_checks,
        "trendLabel": trend_label,
        "structureLabel": structure_label,
        "conflictFlag": conflict,
        "finalBias": final_bias,
        "structure": {
            "recentSwingHighs": last_highs,
            "recentSwingLows": last_lows,
            "higherHighs": higher_highs,
            "higherLows": higher_lows,
            "lowerHighs": lower_highs,
            "lowerLows": lower_lows,
            "breakoutQuality": breakout_quality,
            "pullbackQuality": pullback_quality,
            "priceAcceptance": "市场接受更高价格" if close > (prev_high or close + 1) else ("市场接受更低价格" if close < (prev_low or close - 1) else "价格仍在最近结构区间内"),
        },
    }


def trend_side_from_score(item: dict[str, Any]) -> str:
    if item["conflictFlag"]:
        return "conflict"
    if item["finalBias"] == "bullish":
        return "bullish"
    if item["finalBias"] == "bearish":
        return "bearish"
    return "range"


def analyze_trend(data: dict[str, Any]) -> dict[str, Any]:
    symbol = str(data.get("symbol") or "ETHUSDT").strip().upper()
    cutoff = cutoff_from_request(data)
    include_partial = bool(data.get("include_partial_kline", True))
    refresh_meta = ensure_klines_fresh_for_cutoff(symbol, cutoff)
    frames = [tf for tf in TREND_TIMEFRAMES if tf in STORE.available_timeframes()]
    analyses = [analyze_timeframe(symbol, tf, cutoff, include_partial) for tf in frames]
    by_tf = {item["timeframe"]: item for item in analyses}
    weekly = by_tf.get("1w")
    daily = by_tf.get("1d")
    h4 = by_tf.get("4h")
    h2 = by_tf.get("2h")
    h1 = by_tf.get("1h")
    primary = trend_side_from_score(weekly) if weekly else "range"
    swing = trend_side_from_score(daily) if daily else "range"
    trade_votes = [trend_side_from_score(x) for x in (h4, h2, h1) if x]
    bull_votes = trade_votes.count("bullish")
    bear_votes = trade_votes.count("bearish")
    if bull_votes > bear_votes:
        trade_direction = "偏多"
    elif bear_votes > bull_votes:
        trade_direction = "偏空"
    else:
        trade_direction = "震荡/等待确认"
    conflict = False
    conflict_reasons: list[str] = []
    if primary in {"bullish", "bearish"} and swing in {"bullish", "bearish"} and primary != swing:
        conflict = True
        conflict_reasons.append("周线与日线方向相反")
    if any(item["conflictFlag"] for item in analyses):
        conflict = True
        conflict_reasons.append("至少一个周期 EMA 与价格结构冲突")
    if primary == "bullish" and swing == "bullish" and bull_votes >= 1:
        current_action = "顺势做多"
    elif primary == "bearish" and swing == "bearish" and bear_votes >= 1:
        current_action = "顺势做空"
    elif conflict:
        current_action = "只做短线"
    else:
        current_action = "观望"
    if current_action == "顺势做多":
        trade_meaning = "偏多：等待 4H/2H/1H 回踩 EMA20/EMA50 或前高突破后回踩不破，15m/5m 出现承接再考虑入场。"
    elif current_action == "顺势做空":
        trade_meaning = "偏空：等待 4H/2H/1H 反弹 EMA20/EMA50 或关键位被拒绝，15m/5m 出现转弱再考虑入场。"
    elif current_action == "只做短线":
        trade_meaning = "大周期存在冲突，只适合轻仓短线；不要把 15m/5m 的触发当成主趋势单。"
    else:
        trade_meaning = "震荡或证据不足，优先等价格接近区间边界，不在区间中部追单。"
    one_sentence = f"{TREND_LABELS.get('1w')} {weekly['trendLabel'] if weekly else '未知'}，{TREND_LABELS.get('1d')} {daily['trendLabel'] if daily else '未知'}，交易方向：{trade_direction}，当前适合：{current_action}。"
    return {
        "symbol": symbol,
        "cutoff_utc": cutoff.isoformat(timespec="seconds"),
        "cutoff_bjt": iso_bjt(cutoff),
        "include_partial_kline": include_partial,
        "auto_kline_refresh": refresh_meta,
        "timeframes": analyses,
        "structureConfirmation": {
            "highLowStructure": "；".join(f"{item['label']}：{item['structureLabel']}" for item in analyses),
            "breakoutQuality": "；".join(f"{item['label']}：{item['structure']['breakoutQuality']}" for item in analyses if item["timeframe"] in {"1d", "4h", "2h", "1h", "15m"}),
            "pullbackQuality": "；".join(f"{item['label']}：{item['structure']['pullbackQuality']}" for item in analyses if item["timeframe"] in {"1d", "4h", "2h", "1h", "15m"}),
            "priceAcceptance": "；".join(f"{item['label']}：{item['structure']['priceAcceptance']}" for item in analyses if item["timeframe"] in {"1d", "4h", "2h", "1h"}),
            "emaStructureConsistency": "存在冲突" if conflict else "大体一致或未出现强冲突",
        },
        "summary": {
            "mainTrend": primary,
            "swingDirection": swing,
            "tradeDirection": trade_direction,
            "multiTimeframeConflict": conflict,
            "conflictReasons": conflict_reasons,
            "currentAction": current_action,
            "tradeMeaning": trade_meaning,
            "oneSentence": one_sentence,
        },
    }


def parse_extra_headers(value: Any) -> dict[str, str]:
    if value in {None, ""}:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"额外请求头 JSON 格式错误: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError("额外请求头必须是 JSON 对象")
    headers: dict[str, str] = {}
    for key, val in value.items():
        name = str(key or "").strip()
        if not name:
            continue
        if name.lower() in {"authorization", "content-type"}:
            raise ValueError(f"额外请求头不能覆盖 {name}")
        headers[name] = str(val)
    return headers


def build_llm_headers(data: dict[str, Any], *, include_json: bool, require_auth: bool) -> dict[str, str]:
    api_key = str(data.get("api_key") or "")
    if require_auth and not api_key:
        raise ValueError("缺少 API key")
    headers: dict[str, str] = {"Accept": "application/json", "User-Agent": "openai-python/1.0"}
    if include_json:
        headers["Content-Type"] = "application/json"
    if api_key:
        headers["Authorization"] = "Bearer " + api_key
    cf_id = str(data.get("cf_access_client_id") or "").strip()
    cf_secret = str(data.get("cf_access_client_secret") or "")
    if cf_id and cf_secret:
        headers["CF-Access-Client-Id"] = cf_id
        headers["CF-Access-Client-Secret"] = cf_secret
    elif cf_id or cf_secret:
        raise ValueError("CF Access Client ID 和 Secret 必须同时填写")
    headers.update(parse_extra_headers(data.get("extra_headers")))
    return headers


def normalize_llm_http_error(status: int, headers: Any, detail: str) -> str:
    lower = detail.lower()
    if "cloudflare" in lower or "cf-ray" in lower or "attention required" in lower or "just a moment" in lower:
        return (
            f"HTTP {status}: Cloudflare 拦截了机器请求。"
            "请在 CF 后台给 /v1/* 跳过 Bot/WAF challenge，或使用 CF Access Service Token，"
            "在页面高级请求通道里填写 CF-Access-Client-Id / Secret，或者换一个不经 challenge 的专用 API Base URL。"
        )
    return f"HTTP {status}: {detail[:2000]}"


def read_llm_json_response(resp: requests.Response) -> dict[str, Any]:
    if not resp.ok:
        raise RuntimeError(normalize_llm_http_error(resp.status_code, resp.headers, resp.text))
    try:
        raw = resp.json()
    except Exception as exc:
        raise ValueError(f"LLM 返回不是 JSON: {resp.text[:1000]}") from exc
    if not isinstance(raw, dict):
        raise ValueError("LLM 返回 JSON 顶层必须是对象")
    return raw


def call_models(data: dict[str, Any]) -> list[str]:
    base_url = str(data.get("base_url") or DEFAULT_BASE_URL).strip()
    timeout = int(data.get("timeout") or 60)
    url = base_url.rstrip("/") + "/models"
    headers = build_llm_headers(data, include_json=False, require_auth=False)
    resp = requests.get(url, headers=headers, timeout=timeout)
    raw = read_llm_json_response(resp)
    data = raw.get("data", raw if isinstance(raw, list) else [])
    models: list[str] = []
    for item in data:
        if isinstance(item, dict):
            mid = item.get("id") or item.get("model") or item.get("name")
        else:
            mid = item
        if mid:
            models.append(str(mid))
    return sorted(dict.fromkeys(models))


def call_llm(data: dict[str, Any], request_for_llm: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    base_url = str(data.get("base_url") or DEFAULT_BASE_URL).strip()
    model = str(data.get("model") or DEFAULT_MODEL).strip()
    api_key = str(data.get("api_key") or "")
    timeout = int(data.get("timeout") or 150)
    if not base_url:
        raise ValueError("缺少 Base URL")
    if not model:
        raise ValueError("缺少模型名")
    if not api_key:
        raise ValueError("缺少 API key")
    body = {
        "model": model,
        "messages": request_for_llm["messages"],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    headers = build_llm_headers(data, include_json=True, require_auth=True)
    url = base_url.rstrip("/") + "/chat/completions"
    started = time.time()
    resp = requests.post(url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"), headers=headers, timeout=timeout)
    raw = read_llm_json_response(resp)
    elapsed = time.time() - started
    answer = raw["choices"][0]["message"]["content"]
    meta = {
        "ok": True,
        "base_url": base_url,
        "model": raw.get("model", model),
        "usage": raw.get("usage", {}),
        "elapsed_sec": round(elapsed, 3),
    }
    archived_body = {k: v for k, v in body.items() if k != "api_key"}
    return answer, meta, archived_body


def call_llm_profile(profile: dict[str, Any], request_for_llm: dict[str, Any]) -> tuple[str, dict[str, Any], dict[str, Any]]:
    data = {
        "base_url": profile.get("base_url"),
        "api_key": profile.get("api_key"),
        "model": profile.get("model"),
        "timeout": profile.get("timeout"),
        "cf_access_client_id": profile.get("cf_access_client_id"),
        "cf_access_client_secret": profile.get("cf_access_client_secret"),
        "extra_headers": profile.get("extra_headers"),
    }
    return call_llm(data, request_for_llm)


def sanitize_model_profiles(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError("多模型请求缺少 model_profiles")
    profiles: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        base_url = str(item.get("base_url") or "").strip()
        model = str(item.get("model") or "").strip()
        api_key = str(item.get("api_key") or "")
        timeout = int(item.get("timeout") or 150)
        cf_access_client_id = str(item.get("cf_access_client_id") or "").strip()
        cf_access_client_secret = str(item.get("cf_access_client_secret") or "")
        extra_headers = item.get("extra_headers") or ""
        if not base_url or not model:
            continue
        key = (base_url, model)
        if key in seen:
            continue
        seen.add(key)
        profiles.append(
            {
                "id": str(item.get("id") or f"{base_url}|||{model}"),
                "base_url": base_url,
                "api_key": api_key,
                "model": model,
                "timeout": max(10, min(timeout, 600)),
                "cf_access_client_id": cf_access_client_id,
                "cf_access_client_secret": cf_access_client_secret,
                "extra_headers": extra_headers,
            }
        )
    if not profiles:
        raise ValueError("请至少选择一个已保存模型")
    if len(profiles) > 12:
        raise ValueError("单次最多同时请求 12 个模型")
    missing = [p["model"] for p in profiles if not p.get("api_key")]
    if missing:
        raise ValueError("以下模型缺少 API key: " + ", ".join(missing))
    return profiles


def request_body_for_profile(profile: dict[str, Any], request_for_llm: dict[str, Any]) -> dict[str, Any]:
    return {
        "model": profile.get("model"),
        "messages": request_for_llm["messages"],
        "temperature": 0,
    }


def call_multi_llm(data: dict[str, Any], payload: dict[str, Any], request_for_llm: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    profiles = sanitize_model_profiles(data.get("model_profiles"))
    batch_id = safe_id()
    results: list[dict[str, Any]] = []

    def worker(profile: dict[str, Any]) -> dict[str, Any]:
        started = time.time()
        try:
            answer, llm_meta, archived_body = call_llm_profile(profile, request_for_llm)
            record = {
                "payload": payload,
                "request_for_llm": {**archived_body, "messages": request_for_llm["messages"]},
                "answer": answer,
                "meta": {**meta, **llm_meta, "batch_id": batch_id, "profile_id": profile.get("id")},
            }
            return record
        except Exception as exc:
            elapsed = time.time() - started
            record = {
                "payload": payload,
                "request_for_llm": request_body_for_profile(profile, request_for_llm),
                "answer": "",
                "meta": {
                    **meta,
                    "ok": False,
                    "batch_id": batch_id,
                    "profile_id": profile.get("id"),
                    "base_url": profile.get("base_url"),
                    "model": profile.get("model"),
                    "elapsed_sec": round(elapsed, 3),
                    "error": f"{type(exc).__name__}: {exc}",
                },
            }
            return record

    with ThreadPoolExecutor(max_workers=min(6, len(profiles))) as executor:
        futures = {executor.submit(worker, profile): idx for idx, profile in enumerate(profiles)}
        ordered: list[dict[str, Any] | None] = [None] * len(profiles)
        for future in as_completed(futures):
            ordered[futures[future]] = future.result()

    for record in ordered:
        if record is None:
            continue
        results.append(archive_record(record))

    ok_count = sum(1 for record in results if (record.get("meta") or {}).get("ok") is not False)
    batch = {
        "id": batch_id,
        "count": len(results),
        "ok_count": ok_count,
        "error_count": len(results) - ok_count,
    }
    batch_record = archive_batch_record(batch, payload, request_for_llm, results)
    return {
        "batch": {
            **batch,
            "archive_id": batch_record.get("id"),
        },
        "batch_record": batch_record,
        "records": results,
    }


def update_klines(symbol: str) -> dict[str, Any]:
    python = BUNDLED_PYTHON if BUNDLED_PYTHON.exists() else Path(sys.executable)
    if not UPDATE_SCRIPT.exists():
        raise FileNotFoundError(str(UPDATE_SCRIPT))
    cmd = [
        str(python),
        str(UPDATE_SCRIPT),
        "--symbol",
        symbol.upper(),
        "--clean-root",
        str(CLEAN_ROOT),
    ]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True, timeout=600)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"K线更新失败: {detail[-2000:]}")
    text = proc.stdout.strip()
    match = re.search(r"\{.*\}\s*$", text, flags=re.S)
    if not match:
        raise RuntimeError(f"K线更新输出无法解析: {text[-1000:]}")
    result = json.loads(match.group(0))
    STORE.clear_cache()
    return result


def ensure_klines_fresh_for_cutoff(symbol: str, cutoff: datetime) -> dict[str, Any]:
    symbol = symbol.upper()
    latest_open = STORE.latest_source_open(symbol)
    latest_complete = latest_open + timedelta(minutes=1) if latest_open else None
    if latest_complete is not None and latest_complete >= cutoff:
        return {
            "updated": False,
            "reason": "local_data_covers_cutoff",
            "latest_open_utc": latest_open.isoformat(timespec="seconds"),
            "latest_complete_utc": latest_complete.isoformat(timespec="seconds"),
            "cutoff_utc": cutoff.isoformat(timespec="seconds"),
        }
    result = update_klines(symbol)
    latest_open_after = STORE.latest_source_open(symbol)
    latest_complete_after = latest_open_after + timedelta(minutes=1) if latest_open_after else None
    return {
        "updated": True,
        "reason": "local_data_lagged_cutoff",
        "latest_open_before_utc": latest_open.isoformat(timespec="seconds") if latest_open else None,
        "latest_complete_before_utc": latest_complete.isoformat(timespec="seconds") if latest_complete else None,
        "latest_open_after_utc": latest_open_after.isoformat(timespec="seconds") if latest_open_after else None,
        "latest_complete_after_utc": latest_complete_after.isoformat(timespec="seconds") if latest_complete_after else None,
        "cutoff_utc": cutoff.isoformat(timespec="seconds"),
        "update_result": result,
    }


def archive_record(record: dict[str, Any]) -> dict[str, Any]:
    ensure_archive()
    record_id = safe_id()
    record["id"] = record_id
    record["created_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    record["created_bjt"] = iso_bjt(datetime.now(timezone.utc))
    out_dir = ARCHIVE_ROOT / "requests" / record_id
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "record.json").write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "answer.md").write_text(str(record.get("answer") or ""), encoding="utf-8")
    (out_dir / "payload.json").write_text(json.dumps(record.get("payload") or {}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "request_for_llm.json").write_text(json.dumps(record.get("request_for_llm") or {}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "meta.json").write_text(json.dumps(record.get("meta") or {}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return record


def archive_batch_record(batch: dict[str, Any], payload: dict[str, Any], request_for_llm: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
    answers = []
    for record in records:
        meta = record.get("meta") or {}
        title = f"{'失败' if meta.get('ok') is False else '完成'} · {meta.get('model', '')} @ {meta.get('base_url', '')}"
        body = str(record.get("answer") or meta.get("error") or "")
        answers.append(f"## {title}\n\n{body}".strip())
    compact_records = []
    for record in records:
        compact_records.append(
            {
                "id": record.get("id"),
                "created_bjt": record.get("created_bjt"),
                "answer": record.get("answer", ""),
                "request_for_llm": record.get("request_for_llm", {}),
                "meta": record.get("meta", {}),
            }
        )
    batch_record = {
        "type": "multi_model_batch",
        "batch": batch,
        "payload": payload,
        "request_for_llm": {
            "shared_messages": request_for_llm.get("messages", []),
            "model_requests": [record.get("request_for_llm", {}) for record in records],
        },
        "answer": "\n\n---\n\n".join(answers),
        "meta": {
            "ok": batch.get("ok_count", 0) > 0,
            "batch_id": batch.get("id"),
            "count": batch.get("count", 0),
            "ok_count": batch.get("ok_count", 0),
            "error_count": batch.get("error_count", 0),
            "models": [(record.get("meta") or {}).get("model", "") for record in records],
        },
        "records": compact_records,
    }
    return archive_record(batch_record)


def list_history() -> list[dict[str, Any]]:
    ensure_archive()
    items: list[dict[str, Any]] = []
    for path in sorted((ARCHIVE_ROOT / "requests").iterdir(), reverse=True):
        record_path = path / "record.json"
        if not record_path.exists():
            continue
        try:
            record = json.loads(record_path.read_text(encoding="utf-8"))
            payload = record.get("payload") or {}
            meta = record.get("meta") or {}
            batch = record.get("batch") or {}
            items.append(
                {
                    "id": record.get("id") or path.name,
                    "type": record.get("type") or "single_model",
                    "created_bjt": record.get("created_bjt", ""),
                    "cutoff_bjt": payload.get("cutoff_bjt", ""),
                    "model": meta.get("model", ""),
                    "count": batch.get("count") or meta.get("count"),
                    "ok_count": batch.get("ok_count") or meta.get("ok_count"),
                    "windows": [{k: w.get(k) for k in ("timeframe", "requested_closed_count", "count", "partial_count")} for w in payload.get("windows", [])],
                }
            )
        except Exception:
            continue
    return items[:200]


def read_record(record_id: str) -> dict[str, Any]:
    if not re.fullmatch(r"[A-Za-z0-9T_Z.-]+", record_id):
        raise ValueError("非法记录 ID")
    path = ARCHIVE_ROOT / "requests" / record_id / "record.json"
    if not path.exists():
        raise FileNotFoundError(record_id)
    return json.loads(path.read_text(encoding="utf-8"))


def read_leaderboard() -> list[dict[str, Any]]:
    path = ROOT / "data" / "llm_intraday_prompt_eval" / "leaderboard.csv"
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            rows.append(dict(row))
    return rows


def json_error(handler: BaseHTTPRequestHandler, status: int, message: str) -> None:
    body = json.dumps({"error": message}, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    server_version = "RawKlineLLMWebUI/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        print("[%s] %s" % (datetime.now().strftime("%H:%M:%S"), fmt % args), flush=True)

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8") or "{}")

    def send_json(self, value: Any, status: int = 200) -> None:
        body = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        try:
            if self.path in {"/", "/index.html"}:
                tf_options = [{"value": tf, "label": f"{TIMEFRAME_LABELS.get(tf, tf)} ({tf})"} for tf in STORE.available_timeframes()]
                page = (
                    INDEX_HTML.replace("__DEFAULT_BASE_URL__", html.escape(DEFAULT_BASE_URL))
                    .replace("__DEFAULT_MODEL__", html.escape(DEFAULT_MODEL))
                    .replace("__DEFAULT_SYSTEM_PROMPT__", html.escape(DEFAULT_SYSTEM_PROMPT))
                    .replace("__DEFAULT_USER_PROMPT__", html.escape(DEFAULT_USER_PROMPT))
                    .replace("__TIMEFRAME_OPTIONS__", json.dumps(tf_options, ensure_ascii=False))
                )
                body = page.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/trend.html":
                body = TREND_HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/api/history":
                self.send_json({"items": list_history()})
                return
            if self.path == "/api/leaderboard":
                self.send_json({"rows": read_leaderboard()})
                return
            if self.path.startswith("/api/history/"):
                record_id = urllib.parse.unquote(self.path.rsplit("/", 1)[-1])
                self.send_json({"record": read_record(record_id)})
                return
            if self.path == "/api/config":
                self.send_json({"timeframes": STORE.available_timeframes(), "clean_root": str(CLEAN_ROOT)})
                return
            json_error(self, HTTPStatus.NOT_FOUND, "not found")
        except FileNotFoundError as exc:
            json_error(self, HTTPStatus.NOT_FOUND, str(exc))
        except Exception as exc:
            json_error(self, HTTPStatus.INTERNAL_SERVER_ERROR, f"{type(exc).__name__}: {exc}")

    def do_POST(self) -> None:
        try:
            data = self.read_json()
            if self.path == "/api/models":
                models = call_models(data)
                self.send_json({"models": models})
                return
            if self.path == "/api/preview":
                payload, request_for_llm, meta = build_payload(data)
                self.send_json({"payload": payload, "request_for_llm": request_for_llm, "meta": meta})
                return
            if self.path == "/api/trend":
                self.send_json(analyze_trend(data))
                return
            if self.path == "/api/send":
                refresh_meta = ensure_klines_fresh_for_cutoff(
                    str(data.get("symbol") or "ETHUSDT"),
                    cutoff_from_request(data),
                )
                payload, request_for_llm, meta = build_payload(data)
                answer, llm_meta, archived_body = call_llm(data, request_for_llm)
                record = {
                    "payload": payload,
                    "request_for_llm": {**archived_body, "messages": request_for_llm["messages"]},
                    "answer": answer,
                    "meta": {**meta, **llm_meta, "auto_kline_refresh": refresh_meta},
                }
                record = archive_record(record)
                self.send_json({"record": record})
                return
            if self.path == "/api/send-multi":
                refresh_meta = ensure_klines_fresh_for_cutoff(
                    str(data.get("symbol") or "ETHUSDT"),
                    cutoff_from_request(data),
                )
                payload, request_for_llm, meta = build_payload(data)
                meta = {**meta, "auto_kline_refresh": refresh_meta}
                self.send_json(call_multi_llm(data, payload, request_for_llm, meta))
                return
            if self.path == "/api/update-klines":
                result = update_klines(str(data.get("symbol") or "ETHUSDT"))
                self.send_json({"result": result, "timeframes": STORE.available_timeframes()})
                return
            json_error(self, HTTPStatus.NOT_FOUND, "not found")
        except urllib.error.HTTPError as exc:
            json_error(self, exc.code or 502, normalize_llm_http_error(exc))
        except Exception as exc:
            json_error(self, HTTPStatus.BAD_REQUEST, f"{type(exc).__name__}: {exc}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="LLM 原K回测 Web UI")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()
    ensure_archive()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"LLM 原K回测实验台已启动: {url}", flush=True)
    print(f"归档目录: {ARCHIVE_ROOT / 'requests'}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
