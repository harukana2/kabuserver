"""
US Stock Scanner (Webull OpenAPI edition) - day-trade & long-term candidate screener
=====================================================================================

元スクリプト(webull_ma_cross_bot_v3.py)からの主な変更点
--------------------------------------------------------
1. 価格・出来高・テクニカル計算用の生データ取得元を yfinance から
   **Webull OpenAPI (公式 webull-openapi-python-sdk)** に変更。
2. ファンダメンタルズ(アナリスト目標株価・時価総額・PER・PBR・決算日など)は
   Webull OpenAPI では取得できないため(実機確認済み。下記フェーズ2で詳述)、
   **yfinance** から取得するハイブリッド構成にしている。
3. スキャン対象ユニバースを「S&P500 + Nasdaq100」から
   **NYSE + Nasdaq + NYSE American(AMEX) 上場の全銘柄**まで拡大。
   ユニバース自体は Webull API では一覧取得できないため、NASDAQ Trader が
   公式に公開しているシンボルディレクトリ(nasdaqlisted.txt / otherlisted.txt)
   を使用(Wikipediaスクレイピングではなく公式データソース)。
   → テスト銘柄・ワラント・ユニット等は除外し、普通株/ETFを対象に整形。

重要な注意事項(必ず一読してください)
----------------------------------------
- Webull OpenAPI の利用には
    (1) Webull OpenAPI Management での App Key / App Secret 発行
    (2) 対象マーケットデータの有効なサブスクリプション(米国株・ETFの
        historical / real-time データには "Market Data" サブスクリプションが
        別途必要。無い場合 403 が返ります)
  が必要です。詳細: https://developer.webull.com/apis/docs/market-data-api/getting-started
- 本スクリプトは Webull 公式ドキュメント(2026年9月時点でのpublicドキュメント)に
  基づいて実装していますが、実際の JSON レスポンス構造の細部は SDK の
  バージョンにより変わる可能性があります。**このサンドボックス環境は
  ネットワークアクセスができないため、実際にAPIを呼び出しての動作確認は
  行えていません。** 特に以下の箇所は、お手元で1回テスト実行して
  レスポンスの実際のJSON構造を確認し、`_parse_batch_bars_response` を
  調整してください。
- **ファンダメンタルズ(決算日・PER・PBR・時価総額・アナリスト目標株価・
  セクター)はWebull OpenAPIでは取得できません**(実機確認済み。
  `data_client.market_data`/`data_client.screener` の利用可能メソッドを
  ダンプした結果、価格・出来高・分割イベント・スクリーナー系しか存在せず、
  企業ファンダメンタルズを返すエンドポイントが無いことが判明しました)。
  そのため本スクリプトは、価格・出来高・テクニカル指標はWebull OpenAPI、
  ファンダメンタルズはyfinanceという**ハイブリッド構成**になっています。
  yfinanceはYahoo Financeの非公式ラッパーのため、遅延・欠損・レート制限
  (連続アクセスで一時的にブロックされることがある)が起こり得る点に
  注意してください。
- レート制限: Webull Market Data API (HTTP) は概ね 300 requests / 60s
  (エンドポイント単位でさらに個別の上限あり。例: screener系は60/60s)。
  本スクリプトは保守的なバッチサイズとスリープを入れていますが、
  実際の契約プラン・エンドポイントのレート制限は必ず
  https://developer.webull.com/apis/docs/rate-limits で確認してください。
  yfinance側もshortlist銘柄1件ごとに呼ぶため、`FUNDAMENTALS_STAGE_TOP_N`を
  むやみに大きくしすぎない・スリープを削らないことを推奨します。
- 銘柄数が数千に及ぶため、初回フルスキャンはかなりの実行時間・APIコール数に
  なります。GitHub Actions等で毎回全銘柄を回す場合はタイムアウトや
  コスト(API呼び出し回数)に注意してください。UNIVERSE_LIMIT で上限を
  かけられるようにしてあります。
- スコアはヒューリスティックであり利益を保証するものではありません。
  本スクリプトは投資助言ではありません。発注前に必ずブローカーで
  最新の価格・決算日をご確認ください。

必要なインストール
--------------------
    pip install --upgrade webull-openapi-python-sdk pandas numpy requests yfinance

必要な環境変数
----------------
    WEBULL_APP_KEY
    WEBULL_APP_SECRET
    WEBULL_REGION            (省略時 "us")
    WEBULL_API_ENDPOINT      (省略時 "api.webull.com"。sandboxなら "api.sandbox.webull.com")
    GMAIL_USER / GMAIL_APP_PASSWORD / GMAIL_TO  (メール送信用、元スクリプトと同じ)
"""

import os
import sys
import time
import json
import smtplib
import traceback
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# --------------------------------------------------------------------------
# Web viewer (GitHub Pages) 用のデータ出力先
# --------------------------------------------------------------------------
# GitHub PagesはリポジトリのDocs/フォルダしか公開しないため、閲覧ページ
# (docs/index.html)から見えるよう、データも docs/data/ 配下に保存する。
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs", "data")
INDEX_JSON_PATH = os.path.join(DATA_DIR, "index.json")
# 保持するスナップショット数の上限(リポジトリの肥大化防止。Noneで無制限)
MAX_SNAPSHOTS_KEPT = 90

import numpy as np
import pandas as pd
import requests
import yfinance as yf

# Webull OpenAPI 公式SDK
# pip install --upgrade webull-openapi-python-sdk
from webull.core.client import ApiClient
from webull.data.data_client import DataClient
from webull.data.common.category import Category
from webull.data.common.timespan import Timespan


# --------------------------------------------------------------------------
# Configuration (tweak freely)
# --------------------------------------------------------------------------

# 技術面ステージから、コストの高いファンダメンタルズ取得に進める候補数
# デバッグ高速化用: 環境変数 SCAN_FUNDAMENTALS_TOP_N で上書き可能(空文字列は無視)
FUNDAMENTALS_STAGE_TOP_N = int(os.environ.get("SCAN_FUNDAMENTALS_TOP_N") or 400)

# S&P500構成銘柄のうち、ファンダメンタルズ取得の対象にする上限件数
# (activity_score = 出来高・値動きの活発さ が高い順に選ぶ)。
# None なら無制限(S&P500全銘柄が対象になり、候補数が数百件規模になる)。
# 候補数の合計をおおよそ FUNDAMENTALS_STAGE_TOP_N + SP500_STAGE_TOP_N 件に抑えたい場合はここを調整。
# 環境変数 SCAN_SP500_TOP_N でも上書き可能。
SP500_STAGE_TOP_N = int(os.environ.get("SCAN_SP500_TOP_N") or 600)

# メールに実際に載せる銘柄数
DAY_TRADE_LIST_SIZE = 15
LONG_TERM_LIST_SIZE = 15

# S&P500など主要企業欄に載せる銘柄数(スコア上位)
MAJOR_LIST_SIZE = 15

# S&P500構成銘柄リストの取得元(GitHub公開データセット。定期更新されている)
SP500_LIST_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"

# この価格未満は除外(ペニー株はスプレッドが広く執行リスクが高いため)
MIN_PRICE = 5.0

# 20日平均売買代金がこれ未満の銘柄は除外(流動性フィルタ)
MIN_AVG_DOLLAR_VOLUME = 20_000_000  # $20M/day

# Webull OpenAPI(JP)の実機確認の結果、バッチ日足取得エンドポイントは
# 1リクエストあたり symbols を1〜20件までしか受け付けない
# (それ以上は ILLEGAL_PARAMETER: symbols size must be between 1 and 20)。
# 20を超える値を設定しないこと。
BATCH_SIZE = 20

# バッチ間のスリープ(秒)。レート制限(目安 300req/60s = 平均0.2秒間隔)に
# 対して余裕を持たせている。
BATCH_SLEEP_SEC = 0.5

# 日足バー取得本数(約6ヶ月分)の目安。
# 注意: 実機確認済みの呼び出し方 get_batch_history_bar(symbols, category, timespan)
# は3引数のみで、件数(count)引数は渡していない(SDK/契約側のデフォルト件数が
# 返る)。この定数は現状コード内では未使用だが、将来的にAPIが件数指定に
# 対応した場合のために残してある。
HISTORY_BAR_COUNT = 130

# ユニバースの上限(None なら無制限=NYSE+NASDAQ+AMEX全銘柄)。
# 初回テスト時はここを 300 などにして動作確認することを推奨。
# デバッグ高速化用: 環境変数 SCAN_UNIVERSE_LIMIT が設定されていればそちらを優先
# (例: SCAN_UNIVERSE_LIMIT=100 python us_stock_scanner_webull_openapi.py)
UNIVERSE_LIMIT = None
if os.environ.get("SCAN_UNIVERSE_LIMIT"):
    UNIVERSE_LIMIT = int(os.environ["SCAN_UNIVERSE_LIMIT"])

# NASDAQ Trader公式シンボルディレクトリ(公式データソース。HTMLスクレイピングではない)
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDirectory/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDirectory/otherlisted.txt"


# --------------------------------------------------------------------------
# Webull OpenAPI client セットアップ
# --------------------------------------------------------------------------

# region_id ごとの本番/テスト環境エンドポイント既定値。
# region と endpoint(接続先ホスト)は必ずセットで一致させること。
# 例: JP用のApp Key(先頭が "jp." のキー)を us の api.webull.com に投げると
#     「環境不一致」で 401 UNAUTHORIZED になる。
_DEFAULT_ENDPOINTS = {
    "us": {"prod": "api.webull.com", "test": "us-openapi-alb.uat.webullbroker.com"},
    "jp": {"prod": "api.webull.co.jp", "test": "jp-openapi-alb.uat.webullbroker.com"},
}


def build_webull_client() -> DataClient:
    app_key = os.environ.get("WEBULL_APP_KEY")
    app_secret = os.environ.get("WEBULL_APP_SECRET")

    # WEBULL_REGION が未指定の場合、App Key の先頭が "jp." なら jp、それ以外は us と推定する
    region = os.environ.get("WEBULL_REGION")
    if not region:
        region = "jp" if app_key.startswith("jp.") else "us"

    env = "test" if os.environ.get("WEBULL_USE_SANDBOX", "").lower() in ("1", "true", "yes") else "prod"
    default_endpoint = _DEFAULT_ENDPOINTS.get(region, _DEFAULT_ENDPOINTS["us"])[env]
    endpoint = os.environ.get("WEBULL_API_ENDPOINT", default_endpoint)

    print(f"[info] Webull OpenAPI region={region} endpoint={endpoint}")

    api_client = ApiClient(app_key, app_secret, region)
    api_client.add_endpoint(region, endpoint)
    return DataClient(api_client)


# --------------------------------------------------------------------------
# ユニバース構築: NYSE + NASDAQ + AMEX 全銘柄(公式シンボルディレクトリより)
# --------------------------------------------------------------------------

def _load_nasdaq_symbol_file(url: str) -> pd.DataFrame:
    headers = {
        # User-Agent無しだとブロック/簡易ページが返ることがあるため付与
        "User-Agent": "Mozilla/5.0 (compatible; StockScanner/1.0)",
        "Accept": "text/plain,*/*",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    print(f"[debug] GET {url} -> status={resp.status_code} bytes={len(resp.content)} "
          f"content-type={resp.headers.get('Content-Type')}")
    resp.raise_for_status()

    text = resp.text
    first_line = text.splitlines()[0] if text.splitlines() else "<empty body>"
    if "|" not in first_line:
        raise ValueError(
            f"unexpected response from {url} (not pipe-delimited); "
            f"first line: {first_line[:200]!r} / total length: {len(text)}"
        )

    lines = text.splitlines()
    # 最終行はファイルクリエイト日時などのフッター("File Creation Time...")
    lines = [l for l in lines if l and not l.startswith("File Creation Time")]
    from io import StringIO
    df = pd.read_csv(
        StringIO("\n".join(lines)),
        sep="|",
        engine="python",
        on_bad_lines="skip",
    )
    return df


def _load_github_mirror_symbols() -> set[str]:
    """
    NASDAQ Trader (www.nasdaqtrader.com) が社内ネットワーク等でブロックされる
    ケースがあるため、GitHub Actionsで毎晩更新されている公開ミラー
    (rreichel3/US-Stock-Symbols, NASDAQの公式リストをそのまま集計したもの)を
    第一候補として使う。raw.githubusercontent.com は一般に到達性が高い。
    """
    base = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; StockScanner/1.0)"}
    tickers: set[str] = set()
    for exch in ["nasdaq", "nyse", "amex"]:
        url = f"{base}/{exch}/{exch}_tickers.json"
        resp = requests.get(url, headers=headers, timeout=30)
        print(f"[debug] GET {url} -> status={resp.status_code} bytes={len(resp.content)}")
        resp.raise_for_status()
        data = resp.json()
        tickers |= {str(s) for s in data if s}
    return tickers


def load_sp500_symbols() -> set[str]:
    """
    S&P500構成銘柄シンボルの取得(GitHub公開データセットより)。
    失敗した場合は空集合を返す(呼び出し側で「主要企業欄」自体をスキップする)。
    """
    headers = {"User-Agent": "Mozilla/5.0 (compatible; StockScanner/1.0)"}
    try:
        resp = requests.get(SP500_LIST_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        col = "Symbol" if "Symbol" in df.columns else df.columns[0]
        syms = {str(s).strip().replace(".", "-") for s in df[col].tolist() if s}
        print(f"[info] loaded {len(syms)} S&P500 symbols")
        return syms
    except Exception as e:
        print(f"[warn] failed to load S&P500 list: {e}")
        return set()


def build_universe() -> list[str]:
    tickers: set[str] = set()

    # --- 第一候補: GitHub上の公開ミラー(NASDAQ/NYSE/AMEX) ---
    try:
        tickers |= _load_github_mirror_symbols()
        print(f"[info] loaded {len(tickers)} symbols from GitHub mirror")
    except Exception as e:
        print(f"[warn] failed to load GitHub mirror symbol lists: {e}")

    # --- フォールバック: NASDAQ Trader公式シンボルディレクトリ ---
    if not tickers:
        try:
            df = _load_nasdaq_symbol_file(NASDAQ_LISTED_URL)
            df = df[df["Test Issue"] == "N"]
            syms = df["Symbol"].astype(str).tolist()
            tickers |= set(syms)
        except Exception as e:
            print(f"[warn] failed to load nasdaqlisted.txt: {e}")

        try:
            df = _load_nasdaq_symbol_file(OTHER_LISTED_URL)
            df = df[df["Test Issue"] == "N"]
            df = df[df["Exchange"].isin(["N", "A", "P"])]
            symbol_col = "ACT Symbol" if "ACT Symbol" in df.columns else "Symbol"
            syms = df[symbol_col].astype(str).tolist()
            tickers |= set(syms)
        except Exception as e:
            print(f"[warn] failed to load otherlisted.txt: {e}")

    # 優先株・ワラント・ユニット等の記号(.、$、複数のハイフンなど)を除外し、
    # Webull/一般的なティッカー表記(ドット→ハイフン)に正規化
    cleaned = set()
    for t in tickers:
        if not t or not isinstance(t, str):
            continue
        t = t.strip()
        if not t or "$" in t:
            continue
        # 優先株・ワラント・ユニット等でよく付く記号をラフに除外
        if any(ch in t for ch in ["^", "/", "="]):
            continue
        cleaned.add(t.replace(".", "-"))

    cleaned = sorted(cleaned)

    if not cleaned:
        # 万一シンボルディレクトリが取得できない場合のフォールバック
        cleaned = [
            "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO",
            "AMD", "NFLX", "CRM", "ADBE", "INTC", "SMCI", "PATH", "PLTR",
        ]

    if UNIVERSE_LIMIT:
        cleaned = cleaned[:UNIVERSE_LIMIT]

    return cleaned


# --------------------------------------------------------------------------
# Technical indicators (元スクリプトと同じロジック)
# --------------------------------------------------------------------------

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def bollinger_position(series: pd.Series, period: int = 20, num_std: float = 2.0) -> float:
    """
    直近値がボリンジャーバンド内のどの位置にあるかを 0(下限)〜1(上限)の
    スケールで返す(0.5がミッドバンド=SMA上)。バンド幅が0の場合はNaN。
    """
    sma = series.rolling(period).mean()
    std = series.rolling(period).std()
    upper = sma + num_std * std
    lower = sma - num_std * std
    last = series.iloc[-1]
    u, l = upper.iloc[-1], lower.iloc[-1]
    if pd.isna(u) or pd.isna(l) or (u - l) == 0:
        return np.nan
    return float((last - l) / (u - l))


def atr_pct(df: pd.DataFrame, period: int = 14) -> float:
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    last_close = close.iloc[-1]
    if pd.isna(atr) or last_close == 0:
        return np.nan
    return float(atr / last_close * 100)


def compute_technical_row(symbol: str, df: pd.DataFrame) -> dict | None:
    if df is None or df.empty or len(df) < 60:
        return None
    df = df.dropna(subset=["Close", "Volume"])
    if len(df) < 60:
        return None

    close = df["Close"]
    volume = df["Volume"]
    last_price = float(close.iloc[-1])
    if last_price < MIN_PRICE:
        return None

    avg_dollar_vol = float((close.tail(20) * volume.tail(20)).mean())
    if avg_dollar_vol < MIN_AVG_DOLLAR_VOLUME:
        return None

    day_change_pct = float((close.iloc[-1] / close.iloc[-2] - 1) * 100) if len(close) > 1 else 0.0
    rel_volume = float(volume.iloc[-1] / volume.tail(20).mean()) if volume.tail(20).mean() > 0 else np.nan

    sma20 = close.rolling(20).mean().iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    sma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else np.nan
    rsi14 = float(rsi(close).iloc[-1])
    atrp = atr_pct(df)

    mom_1m = float((close.iloc[-1] / close.iloc[-21] - 1) * 100) if len(close) > 21 else np.nan
    mom_3m = float((close.iloc[-1] / close.iloc[-63] - 1) * 100) if len(close) > 63 else np.nan

    # MACD(トレンドの勢い・転換の目安。予測ではなく直近の値動きの整理)
    macd_line, signal_line, hist = macd(close)
    macd_hist = float(hist.iloc[-1]) if pd.notna(hist.iloc[-1]) else np.nan
    macd_hist_prev = float(hist.iloc[-2]) if len(hist) > 1 and pd.notna(hist.iloc[-2]) else np.nan
    macd_bullish_cross = bool(
        pd.notna(macd_hist) and pd.notna(macd_hist_prev) and macd_hist_prev <= 0 < macd_hist
    )
    macd_bearish_cross = bool(
        pd.notna(macd_hist) and pd.notna(macd_hist_prev) and macd_hist_prev >= 0 > macd_hist
    )

    # ボリンジャーバンド上の位置(0=下限付近、1=上限付近)
    bb_pos = bollinger_position(close)

    # 出来高トレンド(直近5日平均 ÷ 直近20日平均。1超なら出来高が増加基調)
    vol5 = volume.tail(5).mean()
    vol20 = volume.tail(20).mean()
    vol_trend = float(vol5 / vol20) if vol20 > 0 else np.nan

    return {
        "symbol": symbol,
        "price": last_price,
        "day_change_pct": day_change_pct,
        "rel_volume": rel_volume,
        "rsi14": rsi14,
        "atr_pct": atrp,
        "above_sma20": bool(last_price > sma20) if pd.notna(sma20) else None,
        "above_sma50": bool(last_price > sma50) if pd.notna(sma50) else None,
        "above_sma200": bool(last_price > sma200) if pd.notna(sma200) else None,
        "mom_1m": mom_1m,
        "mom_3m": mom_3m,
        "avg_dollar_vol": avg_dollar_vol,
        "macd_hist": macd_hist,
        "macd_bullish_cross": macd_bullish_cross,
        "macd_bearish_cross": macd_bearish_cross,
        "bb_pos": bb_pos,
        "vol_trend": vol_trend,
    }


# --------------------------------------------------------------------------
# Webull OpenAPI からの価格/出来高ヒストリー取得
# --------------------------------------------------------------------------

def _resolve_daily_timespan():
    """
    インストールされているSDKのTimespan列挙型から、日足に対応するメンバーを
    名前ベースで自動検出する(バージョンによってD/D1/DAY等、名称が異なるため。
    動作確認済みのwebull_ma_cross_bot_v3.pyから移植)。
    """
    candidates = [m for m in Timespan.__members__ if m.upper() in ("D", "D1", "1D") or "DAY" in m.upper()]
    if not candidates:
        available = list(Timespan.__members__.keys())
        raise RuntimeError(
            f"日足に対応するTimespanが見つかりません。利用可能な値: {available}"
        )
    if "D" in candidates:
        name = "D"
    elif "DAY" in candidates:
        name = "DAY"
    else:
        name = candidates[0]
    return Timespan[name]


def _extract_bars(data) -> list:
    """
    get_batch_history_bar系レスポンスから bars のリストを取り出す(構造ゆれに対応)。
    実機確認の結果、このSDKは "result" キーにbars配列を入れて返すことが多いため、
    "bars"/"data" に加えて "result" も候補に含める。
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        bars = data.get("bars") or data.get("result") or data.get("data") or []
        if isinstance(bars, dict):
            bars = bars.get("bars") or bars.get("result") or bars.get("data") or []
        return bars if isinstance(bars, list) else []
    return []


def _bar_row_to_ohlcv(b) -> dict | None:
    if isinstance(b, dict):
        get = lambda *keys: next((b[k] for k in keys if k in b and b[k] is not None), None)
        o, h, l, c, v = get("open", "o"), get("high", "h"), get("low", "l"), get("close", "c"), get("volume", "v")
        ts = get("timestamp", "time", "t", "trade_time")
    elif isinstance(b, (list, tuple)) and len(b) >= 6:
        # よくあるOHLCVバー配列表現: [timestamp, open, high, low, close, volume, ...]
        ts, o, h, l, c, v = b[0], b[1], b[2], b[3], b[4], b[5]
    else:
        return None
    try:
        return {
            "Open": float(o), "High": float(h), "Low": float(l),
            "Close": float(c), "Volume": float(v), "ts": ts,
        }
    except (TypeError, ValueError):
        return None


def _parse_batch_bars_response(data, chunk: list) -> tuple[dict[str, pd.DataFrame], bool]:
    """
    get_batch_history_bar のレスポンスを {symbol: OHLCVのDataFrame} に変換する。
    レスポンス構造の解釈はwebull_ma_cross_bot_v3.pyで実機確認済みのロジックを移植:
    - {"AAPL": {"bars": [...]}, ...} / {"AAPL": [...bars...], ...} のdict-of-symbol形式
    - {"result": [{"symbol": "AAPL", "result": [...bars...]}, ...]} 形式(実機で確認された形)
    - [{"symbol": "AAPL", "bars": [...]}, ...] のlist形式
    """
    result: dict[str, pd.DataFrame] = {}
    parsed_any = False

    def _rows_to_df(rows: list) -> pd.DataFrame | None:
        ohlcv_rows = [r for r in (_bar_row_to_ohlcv(b) for b in rows) if r is not None]
        if not ohlcv_rows:
            return None
        df = pd.DataFrame(ohlcv_rows)
        if "ts" in df.columns and df["ts"].notna().any():
            df = df.sort_values("ts")
        return df.drop(columns=["ts"], errors="ignore").reset_index(drop=True)

    if isinstance(data, dict):
        for sym in chunk:
            if sym in data:
                sym_data = data[sym]
                bars = sym_data if isinstance(sym_data, list) else _extract_bars(sym_data)
                df = _rows_to_df(bars)
                if df is not None:
                    result[sym] = df
                    parsed_any = True
        if not parsed_any:
            items = data.get("result") or data.get("data") or data.get("results") or []
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and "symbol" in item:
                        sym = item["symbol"]
                        bars = _extract_bars(item)
                        df = _rows_to_df(bars)
                        if df is not None:
                            result[sym] = df
                            parsed_any = True
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "symbol" in item:
                sym = item["symbol"]
                bars = _extract_bars(item)
                df = _rows_to_df(bars)
                if df is not None:
                    result[sym] = df
                    parsed_any = True

    return result, parsed_any


import re as _re

_INVALID_SYMBOL_RE = _re.compile(r"does not exist in the category\.\s*\[([^\]]*)\]", _re.IGNORECASE)


def _extract_invalid_symbols(err_msg: str) -> list[str]:
    """
    'The symbols does not exist in the category. [PHGE, XXXX].' のような
    INVALID_SYMBOL エラーメッセージから、該当シンボルのリストを取り出す。
    """
    m = _INVALID_SYMBOL_RE.search(err_msg or "")
    if not m:
        return []
    return [s.strip() for s in m.group(1).split(",") if s.strip()]


def download_history_batched(data_client: DataClient, tickers: list[str]) -> dict[str, pd.DataFrame]:
    timespan = _resolve_daily_timespan()
    out: dict[str, pd.DataFrame] = {}
    n = len(tickers)
    invalid_symbols_seen = 0

    for i in range(0, n, BATCH_SIZE):
        chunk = tickers[i:i + BATCH_SIZE]
        if (i // BATCH_SIZE) % 20 == 0:
            print(f"[info] downloading history {i}-{i + len(chunk)} / {n} (Webull OpenAPI)")

        # 無効シンボル(Webull側に存在しない銘柄)を1件ずつ除外しながら再試行する。
        # NASDAQ公式リスト/GitHubミラーにはあるがWebullが未対応の銘柄
        # (一部ワラント・優先株・新規上場直後の銘柄など)が一定数混じるため必須。
        for retry in range(BATCH_SIZE + 1):  # 最悪1件ずつ全部除外しても終わるようにする
            if not chunk:
                break
            try:
                res = data_client.market_data.get_batch_history_bar(
                    chunk, Category.US_STOCK.name, timespan.name
                )
            except Exception as e:
                msg = str(e)
                invalid = _extract_invalid_symbols(msg)
                if invalid:
                    invalid_symbols_seen += len(invalid)
                    chunk = [s for s in chunk if s not in invalid]
                    continue  # 除外して同じバッチを再試行
                print(f"[warn] batch bars call failed for chunk starting {chunk[0]}: {e}")
                chunk = []
                break
            else:
                break

        if not chunk:
            time.sleep(BATCH_SLEEP_SEC)
            continue

        if res.status_code == 403:
            print("[error] 403: OpenAPIの市場データサブスクリプションが未契約の可能性があります。中断します。")
            break
        if res.status_code != 200:
            print(f"[warn] batch bars HTTP {res.status_code} for chunk starting {chunk[0]}: {res.text[:300]}")
            time.sleep(BATCH_SLEEP_SEC * 2)
            continue

        data = res.json()
        chunk_result, parsed_any = _parse_batch_bars_response(data, chunk)
        out.update(chunk_result)

        if not parsed_any and i == 0:
            # 最初のバッチだけ、解釈できなかった場合に生JSONの先頭を出す(デバッグ用)
            print(f"[debug] batch bars response (raw, first 500 chars): {str(data)[:500]}")

        time.sleep(BATCH_SLEEP_SEC)

    if invalid_symbols_seen:
        print(f"[info] Webull非対応のため除外したシンボル数: {invalid_symbols_seen}")
    return out


# --------------------------------------------------------------------------
# Fundamentals(ハイブリッド構成: Webull OpenAPI には決算日・PER・PBR・
# 時価総額・アナリスト目標株価・セクター等を取得する手段が存在しないため、
# ここだけ yfinance を使う)
# --------------------------------------------------------------------------
#
# 実機確認の結果、Webull OpenAPI の data_client.market_data /
# data_client.screener で利用可能なメソッドは以下のみだった:
#   market_data: get_batch_history_bar, get_corp_action, get_eod_bar,
#                get_footprint, get_history_bar, get_noii_bars,
#                get_noii_snapshot, get_quotes, get_snapshot, get_tick
#   screener:    get_52whl, get_gainers_losers, get_high_dividend,
#                get_market_sectors, get_market_sectors_detail,
#                get_most_active, list_52whl, list_gainers_losers,
#                list_high_dividend, list_market_sectors,
#                list_market_sectors_detail
#
# この中に決算日・PER・PBR・時価総額・アナリスト目標株価・企業プロフィール
# (セクター)を返すエンドポイントは存在しない。
# ("get_corp_action" は公式ドキュメント上も株式分割・逆分割のみを扱う
#  エンドポイントで、決算日や配当は含まれない。)
# そのため、価格・出来高・テクニカル指標は引き続きWebull OpenAPI(実機確認
# 済みで動作する)を使い、ファンダメンタルズだけ yfinance(動作実績のある
# 枯れたライブラリ)から取得するハイブリッド構成にしている。

def fetch_fundamentals(symbol: str) -> dict:
    """
    アナリスト目標株価・時価総額・セクター・次回決算日・PER・PBR・
    現金/有利子負債(ネットキャッシュ比率算出用)を yfinance から取得する。
    """
    result = {
        "target_mean": None,
        "market_cap": None,
        "next_earnings": None,
        "recommendation": None,
        "sector": None,
        "per": None,
        "pbr": None,
        "total_cash": None,
        "total_debt": None,
        "net_cash_ratio": None,
    }

    try:
        tk = yf.Ticker(symbol)
        info = tk.info or {}
        result["target_mean"] = info.get("targetMeanPrice")
        result["market_cap"] = info.get("marketCap")
        result["recommendation"] = info.get("recommendationKey")
        result["sector"] = info.get("sector")
        # PERはtrailing優先、無ければforward。PBRはpriceToBook。
        result["per"] = info.get("trailingPE") or info.get("forwardPE")
        result["pbr"] = info.get("priceToBook")
        result["total_cash"] = info.get("totalCash")
        result["total_debt"] = info.get("totalDebt")
    except Exception as e:
        print(f"[warn] yfinance info fetch failed for {symbol}: {e}")

    # ネットキャッシュ比率 = (現金 - 有利子負債) / 時価総額
    # プラスが大きいほど「実質無借金・現金余力が厚い」目安(あくまで簡易指標)
    if result["total_cash"] is not None and result["market_cap"]:
        net_cash = result["total_cash"] - (result["total_debt"] or 0)
        try:
            result["net_cash_ratio"] = float(net_cash) / float(result["market_cap"]) * 100
        except (TypeError, ZeroDivisionError):
            result["net_cash_ratio"] = None

    try:
        tk = yf.Ticker(symbol)
        cal = tk.get_earnings_dates(limit=4)
        if cal is not None and not cal.empty:
            future = cal[cal.index >= pd.Timestamp.now(tz=cal.index.tz)]
            if not future.empty:
                result["next_earnings"] = future.index[0].strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[warn] yfinance earnings fetch failed for {symbol}: {e}")

    return result


# --------------------------------------------------------------------------
# Scoring (1-100, heuristic — NOT a probability of profit) — 元スクリプトと同じ
# --------------------------------------------------------------------------

def clamp(v, lo=1, hi=100):
    return max(lo, min(hi, v))


def score_day_trade(row: dict) -> tuple[float, float]:
    atrp = row.get("atr_pct") or 0
    relvol = row.get("rel_volume") or 1
    rsi14 = row.get("rsi14") or 50
    day_chg = abs(row.get("day_change_pct") or 0)

    opp = (
        min(atrp, 15) / 15 * 40 +
        min(relvol, 5) / 5 * 35 +
        min(day_chg, 15) / 15 * 25
    )

    rsi_extreme = abs(rsi14 - 50) / 50 * 100
    risk = (
        min(atrp, 15) / 15 * 50 +
        rsi_extreme * 0.3 +
        (20 if relvol < 0.8 else 0)
    )
    return clamp(opp), clamp(risk)


def score_long_term(row: dict, fund: dict) -> tuple[float, float]:
    mom1 = row.get("mom_1m") or 0
    mom3 = row.get("mom_3m") or 0
    above50 = row.get("above_sma50")
    above200 = row.get("above_sma200")
    rsi14 = row.get("rsi14") or 50
    price = row.get("price") or 0
    target = fund.get("target_mean")
    per = fund.get("per")
    pbr = fund.get("pbr")
    net_cash_ratio = fund.get("net_cash_ratio")

    upside_pct = None
    if target and price:
        upside_pct = (target - price) / price * 100

    opp = 0
    opp += 15 if above50 else 0
    opp += 15 if above200 else 0
    opp += clamp(min(max(mom1, -20), 20) / 20 * 15 + 15, 0, 30)
    opp += clamp(min(max(mom3, -30), 30) / 30 * 10 + 10, 0, 20)
    if upside_pct is not None:
        opp += clamp(min(max(upside_pct, -10), 40) / 40 * 20, 0, 20)
    # バリュエーション(割安なほど加点。業種によって適正水準は異なるため
    # あくまでラフな目安)
    if per is not None and per > 0:
        if per < 15:
            opp += 8
        elif per < 25:
            opp += 4
    if pbr is not None and pbr > 0 and pbr < 3:
        opp += 4
    # ネットキャッシュが厚い(実質無借金以上)ほど財務の安全余地として加点
    if net_cash_ratio is not None and net_cash_ratio > 10:
        opp += 6

    risk = 0
    risk += 25 if (above200 is False) else 5
    risk += abs(rsi14 - 50) / 50 * 30
    mc = fund.get("market_cap")
    if mc:
        if mc < 2_000_000_000:
            risk += 30
        elif mc < 10_000_000_000:
            risk += 15
    else:
        risk += 10
    # 割高(高PER)や、有利子負債が現金を大きく上回る(ネットキャッシュが
    # 大幅マイナス)場合はリスク側に加点
    if per is not None and per > 40:
        risk += 15
    if net_cash_ratio is not None and net_cash_ratio < -20:
        risk += 15

    return clamp(opp), clamp(risk)


# --------------------------------------------------------------------------
# 予想カテゴリ(大きく値上がり/少し値上がり/変動なし/少し値下がり/大きく値下がり)
# --------------------------------------------------------------------------
#
# 注意: これもテクニカル指標から機械的に導いたルールベースの目安であり、
# 将来の値動きを保証するものではありません(投資助言ではありません)。
# day: 短期(目安1〜5営業日)、long: 長期(目安1〜3ヶ月)。

PREDICTION_CATEGORIES = ["大きく値上がり", "少し値上がり", "変動なし", "少し値下がり", "大きく値下がり"]


def predict_category(row: dict, kind: str) -> str:
    rsi14 = row.get("rsi14") or 50
    above200 = row.get("above_sma200")
    above50 = row.get("above_sma50")
    above20 = row.get("above_sma20")
    mom1 = row.get("mom_1m")
    mom3 = row.get("mom_3m")
    atrp = row.get("atr_pct") or 0
    macd_up = row.get("macd_bullish_cross")
    macd_down = row.get("macd_bearish_cross")

    def _isnan(v):
        return v is None or (isinstance(v, float) and np.isnan(v))

    signals_up, signals_down = 0, 0
    if above200 is True:
        signals_up += 1
    elif above200 is False:
        signals_down += 1
    if above50 is True:
        signals_up += 1
    elif above50 is False:
        signals_down += 1
    if above20 is True:
        signals_up += 1
    elif above20 is False:
        signals_down += 1
    if macd_up:
        signals_up += 1
    if macd_down:
        signals_down += 1

    mom = mom1 if kind == "day" else mom3
    mom_strong = 6 if kind == "day" else 15
    mom_weak = 1.5 if kind == "day" else 4
    if not _isnan(mom):
        if mom > mom_strong:
            signals_up += 2
        elif mom > mom_weak:
            signals_up += 1
        elif mom < -mom_strong:
            signals_down += 2
        elif mom < -mom_weak:
            signals_down += 1

    net = signals_up - signals_down

    # 過熱感/売られすぎによる短期反転バイアス(短期予想のみ強めに反映)
    if kind == "day":
        if rsi14 >= 75 and net <= 1:
            net -= 2
        if rsi14 <= 25 and net >= -1:
            net += 2

    # ボラティリティ・モメンタムが小さい場合は「変動なし」寄りに補正
    flat_zone = (atrp < 1.5) if kind == "day" else (_isnan(mom3) or abs(mom3) < 2)

    if net >= 3:
        category = "大きく値上がり"
    elif net >= 1:
        category = "少し値上がり"
    elif net <= -3:
        category = "大きく値下がり"
    elif net <= -1:
        category = "少し値下がり"
    else:
        category = "変動なし"

    if flat_zone and category in ("少し値上がり", "少し値下がり"):
        category = "変動なし"

    return category


def prediction_horizon_end(run_dt: datetime, kind: str) -> str:
    """予想の的中判定を行う期限日(この日までの値動きで判定する)"""
    delta_days = 7 if kind == "day" else 95  # 目安: 短期=5営業日強、長期=約3ヶ月
    return (run_dt + timedelta(days=delta_days)).strftime("%Y-%m-%d")


# --------------------------------------------------------------------------
# 現状の説明・値動きの見立て・投資タイミングの目安(ルールベースの解説文)
# --------------------------------------------------------------------------
#
# 重要: 以下はすべて「過去の価格・出来高・移動平均などのテクニカル指標から
# 導いたルールベースの解説」であり、将来の株価を予測するものではありません。
# 「予測」という言葉は使わず、あくまで直近の値動きの整理・傾向の説明・
# エントリー判断の一般的な目安として提示します。投資助言ではありません。

def _trend_label(row: dict) -> str:
    above20 = row.get("above_sma20")
    above50 = row.get("above_sma50")
    above200 = row.get("above_sma200")
    if above200 is None:
        long_term = "データ不足"
    elif above50 and above200:
        long_term = "中長期は上昇トレンド"
    elif (not above50) and (not above200):
        long_term = "中長期は下降トレンド"
    else:
        long_term = "中長期はトレンド転換の途中(方向感が定まっていない)"

    if above20 is True:
        short_term = "短期(20日線)は上向き"
    elif above20 is False:
        short_term = "短期(20日線)は下向き"
    else:
        short_term = "短期の方向感は不明瞭"

    return f"{long_term}、{short_term}"


def _situation_text(row: dict, fund: dict | None = None) -> str:
    """現状の説明(直近の値動き・出来高・過熱感を事実ベースで要約)"""
    parts = [_trend_label(row)]

    rel_vol = row.get("rel_volume")
    vol_trend = row.get("vol_trend")
    if rel_vol is not None and not (isinstance(rel_vol, float) and np.isnan(rel_vol)):
        if rel_vol >= 1.5:
            parts.append(f"直近の出来高は平常時の{rel_vol:.1f}倍と急増")
        elif vol_trend is not None and not (isinstance(vol_trend, float) and np.isnan(vol_trend)) and vol_trend >= 1.2:
            parts.append("直近5日の出来高は増加基調")

    rsi14 = row.get("rsi14")
    if rsi14 is not None and not (isinstance(rsi14, float) and np.isnan(rsi14)):
        if rsi14 >= 70:
            parts.append(f"RSI{rsi14:.0f}で短期的に買われすぎ水準")
        elif rsi14 <= 30:
            parts.append(f"RSI{rsi14:.0f}で短期的に売られすぎ水準")

    bb_pos = row.get("bb_pos")
    if bb_pos is not None and not (isinstance(bb_pos, float) and np.isnan(bb_pos)):
        if bb_pos >= 0.95:
            parts.append("ボリンジャーバンド上限付近まで値幅が拡大")
        elif bb_pos <= 0.05:
            parts.append("ボリンジャーバンド下限付近まで下落")

    if row.get("macd_bullish_cross"):
        parts.append("MACDがゴールデンクロス直後")
    elif row.get("macd_bearish_cross"):
        parts.append("MACDがデッドクロス直後")

    if fund and fund.get("next_earnings"):
        days_note = ""
        try:
            ed = datetime.strptime(str(fund["next_earnings"])[:10], "%Y-%m-%d")
            days = (ed.date() - datetime.now().date()).days
            if 0 <= days <= 14:
                days_note = f"(残り{days}日、値動きが荒くなりやすい時期)"
        except Exception:
            pass
        parts.append(f"次回決算予定: {fund['next_earnings']}{days_note}")

    per = fund.get("per") if fund else None
    pbr = fund.get("pbr") if fund else None
    net_cash_ratio = fund.get("net_cash_ratio") if fund else None
    val_parts = []
    if per is not None:
        val_parts.append(f"PER {per:.1f}倍")
    if pbr is not None:
        val_parts.append(f"PBR {pbr:.1f}倍")
    if net_cash_ratio is not None:
        if net_cash_ratio > 0:
            val_parts.append(f"ネットキャッシュ比率+{net_cash_ratio:.0f}%(財務余力あり)")
        else:
            val_parts.append(f"ネットキャッシュ比率{net_cash_ratio:.0f}%(負債超過)")
    if val_parts:
        parts.append("、".join(val_parts))

    return "。".join(parts) + "。"


def _outlook_text(row: dict) -> str:
    """
    値動きの見立て(あくまでテクニカル指標に基づく傾向の整理。断定・保証は避ける)
    """
    rsi14 = row.get("rsi14") or 50
    above200 = row.get("above_sma200")
    mom1 = row.get("mom_1m")
    bb_pos = row.get("bb_pos")

    signals_up, signals_down = 0, 0
    if above200:
        signals_up += 1
    elif above200 is False:
        signals_down += 1
    if row.get("above_sma20"):
        signals_up += 1
    elif row.get("above_sma20") is False:
        signals_down += 1
    if row.get("macd_bullish_cross"):
        signals_up += 1
    if row.get("macd_bearish_cross"):
        signals_down += 1
    if mom1 is not None and not (isinstance(mom1, float) and np.isnan(mom1)):
        if mom1 > 3:
            signals_up += 1
        elif mom1 < -3:
            signals_down += 1

    if rsi14 >= 75 or (bb_pos is not None and not (isinstance(bb_pos, float) and np.isnan(bb_pos)) and bb_pos >= 0.97):
        return "過熱感が強く、短期的には反落・一服のリスクに注意が必要な局面"
    if rsi14 <= 25 or (bb_pos is not None and not (isinstance(bb_pos, float) and np.isnan(bb_pos)) and bb_pos <= 0.03):
        return "売られすぎの反発を試す可能性はあるが、下降トレンド継続のリスクも残る局面"
    if signals_up - signals_down >= 2:
        return "複数の指標がそろって上向きで、トレンドフォロー型の押し目待ちに適した局面"
    if signals_down - signals_up >= 2:
        return "複数の指標が下向きで、無理な逆張りより様子見が優先される局面"
    return "上下どちらとも決め手に欠け、方向感が定まるまで様子見が妥当な局面"


def _timing_text(row: dict, fund: dict | None, kind: str) -> str:
    """
    エントリー検討の一般的な目安(いつ買う/様子見するかの考え方の整理であり、
    タイミングを保証するものではない)。kind: 'day' or 'long'
    """
    rsi14 = row.get("rsi14") or 50
    above200 = row.get("above_sma200")
    above50 = row.get("above_sma50")
    atrp = row.get("atr_pct") or 0
    rel_vol = row.get("rel_volume") or 1
    per = fund.get("per") if fund else None
    net_cash_ratio = fund.get("net_cash_ratio") if fund else None

    days_to_earnings = None
    if fund and fund.get("next_earnings"):
        try:
            ed = datetime.strptime(str(fund["next_earnings"])[:10], "%Y-%m-%d")
            days_to_earnings = (ed.date() - datetime.now().date()).days
        except Exception:
            pass

    earnings_caveat = ""
    if days_to_earnings is not None and 0 <= days_to_earnings <= 7:
        earnings_caveat = "(決算発表が目前のため、跨いでの新規エントリーはギャップリスクに注意)"

    if kind == "day":
        if rsi14 >= 75:
            return "過熱感が高く飛び乗りは避け、押し目や出来高減少を確認してからが無難" + earnings_caveat
        if rel_vol >= 2 and 40 <= rsi14 <= 65:
            return "出来高急増を伴う初動段階。損切りラインを浅めに設定した短期エントリーの検討余地あり" + earnings_caveat
        if atrp >= 6:
            return "値幅(ボラティリティ)が大きく、ポジションサイズを抑えた上での分割エントリーが無難" + earnings_caveat
        return "明確な優位性は限定的。値動きが出るまで様子見が無難" + earnings_caveat
    else:  # long
        cheap = per is not None and per > 0 and per < 15
        rich_cash = net_cash_ratio is not None and net_cash_ratio > 10
        if above200 and above50 and 40 <= rsi14 <= 60:
            base = "トレンド継続中で過熱感も低く、押し目があれば段階的な買い増しを検討しやすい局面"
            if cheap or rich_cash:
                base += "(バリュエーション・財務面からも下値の安心感がある方)"
            return base + earnings_caveat
        if above200 and rsi14 >= 70:
            return "トレンドは良好だが短期的に過熱気味。急がず調整を待つのも一案" + earnings_caveat
        if above200 is False:
            if cheap or rich_cash:
                return "中長期トレンドは下向きだが、バリュエーション・財務面は魅力的。底打ちを確認してからの分割エントリーが無難" + earnings_caveat
            return "中長期トレンドがまだ下向きのため、明確な底打ちシグナルを待つのが無難" + earnings_caveat
        return "トレンド・過熱感ともに中立。決算やイベント通過後の値動きを確認してから判断するのが無難" + earnings_caveat


def build_commentary(row: dict, fund: dict | None, kind: str) -> dict:
    return {
        "situation": _situation_text(row, fund),
        "outlook": _outlook_text(row),
        "timing": _timing_text(row, fund, kind),
    }


# --------------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------------

def run_scan():
    data_client = build_webull_client()

    universe = build_universe()
    print(f"[info] universe size (NYSE+NASDAQ+AMEX): {len(universe)}")

    sp500_set = load_sp500_symbols()

    history = download_history_batched(data_client, universe)
    print(f"[info] got history for {len(history)} tickers")

    tech_rows = []
    for sym, df in history.items():
        row = compute_technical_row(sym, df)
        if row:
            row["is_sp500"] = sym in sp500_set
            tech_rows.append(row)
    tech_df = pd.DataFrame(tech_rows)
    print(f"[info] passed liquidity/price filters: {len(tech_df)}")

    if tech_df.empty:
        raise RuntimeError("No tickers passed the technical filters — aborting.")

    tech_df["activity_score"] = (
        tech_df["atr_pct"].fillna(0) * 1.0 +
        tech_df["rel_volume"].fillna(1) * 10 +
        tech_df["mom_1m"].abs().fillna(0) * 0.5
    )

    # 出来高・値動き上位のショートリストに加え、S&P500構成銘柄はスコアに関わらず
    # 「主要企業欄」用に必ずファンダメンタルズ取得の対象へ含める
    activity_top = tech_df.sort_values("activity_score", ascending=False).head(
        FUNDAMENTALS_STAGE_TOP_N
    )
    # デバッグ高速化用: SCAN_SKIP_SP500=1 でS&P500全銘柄への無条件追加をスキップ
    # (通常運用時はS&P500構成銘柄を「主要企業欄」用に必ず含めるため入れている)
    skip_sp500 = os.environ.get("SCAN_SKIP_SP500", "").lower() in ("1", "true", "yes")
    if sp500_set and not skip_sp500:
        sp500_rows = tech_df[tech_df["is_sp500"]]
        if SP500_STAGE_TOP_N is not None:
            sp500_rows = sp500_rows.sort_values("activity_score", ascending=False).head(
                SP500_STAGE_TOP_N
            )
    else:
        sp500_rows = tech_df.iloc[0:0]
    shortlist = (
        pd.concat([activity_top, sp500_rows])
        .drop_duplicates(subset="symbol")
        .reset_index(drop=True)
    )
    print(
        f"[info] fundamentals取得対象: {len(shortlist)}銘柄 "
        f"(出来高上位{len(activity_top)} + S&P500 {len(sp500_rows)}、重複除去後)"
    )

    candidates = []
    for _, row in shortlist.iterrows():
        sym = row["symbol"]
        fund = fetch_fundamentals(sym)
        row_d = row.to_dict()
        dt_opp, dt_risk = score_day_trade(row_d)
        lt_opp, lt_risk = score_long_term(row_d, fund)
        day_commentary = build_commentary(row_d, fund, "day")
        long_commentary = build_commentary(row_d, fund, "long")
        candidates.append({
            **row_d,
            **fund,
            "day_opportunity": dt_opp,
            "day_risk": dt_risk,
            "long_opportunity": lt_opp,
            "long_risk": lt_risk,
            "day_situation": day_commentary["situation"],
            "day_outlook": day_commentary["outlook"],
            "day_timing": day_commentary["timing"],
            "long_situation": long_commentary["situation"],
            "long_outlook": long_commentary["outlook"],
            "long_timing": long_commentary["timing"],
            "day_prediction": predict_category(row_d, "day"),
            "long_prediction": predict_category(row_d, "long"),
        })
        time.sleep(0.3)

    cand_df = pd.DataFrame(candidates)

    day_trade_list = (
        cand_df.sort_values("day_opportunity", ascending=False)
        .head(DAY_TRADE_LIST_SIZE)
        .to_dict("records")
    )
    long_term_list = (
        cand_df.sort_values("long_opportunity", ascending=False)
        .head(LONG_TERM_LIST_SIZE)
        .to_dict("records")
    )
    # 主要企業(S&P500)欄: 中長期スコア順。S&P500リストが取得できなかった場合は空になる
    major_pool = cand_df[cand_df["is_sp500"]] if "is_sp500" in cand_df.columns else cand_df.iloc[0:0]
    major_list = (
        major_pool.sort_values("long_opportunity", ascending=False)
        .head(MAJOR_LIST_SIZE)
        .to_dict("records")
    )

    return day_trade_list, long_term_list, major_list, universe, len(cand_df), history


# --------------------------------------------------------------------------
# Email rendering + sending (元スクリプトと同じ)
# --------------------------------------------------------------------------

def fmt_pct(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def fmt_num(v, digits=2):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"{v:.{digits}f}"


def render_row_day(r):
    return f"""
    <tr>
      <td><b>{r['symbol']}</b></td>
      <td>${fmt_num(r['price'])}</td>
      <td>{fmt_pct(r.get('day_change_pct'))}</td>
      <td>{fmt_num(r.get('atr_pct'))}%</td>
      <td>{fmt_num(r.get('rel_volume'))}x</td>
      <td>{fmt_num(r.get('rsi14'), 0)}</td>
      <td style="color:#c0392b"><b>{fmt_num(r['day_opportunity'],0)}</b></td>
      <td style="color:#8e44ad"><b>{fmt_num(r['day_risk'],0)}</b></td>
      <td><b>{r.get('day_prediction','—')}</b></td>
      <td>{r.get('next_earnings') or '不明'}</td>
      <td style="font-size:12px;">{r.get('day_situation','—')}</td>
      <td style="font-size:12px;">{r.get('day_outlook','—')}</td>
      <td style="font-size:12px;">{r.get('day_timing','—')}</td>
    </tr>"""


def fmt_ratio(v, suffix="倍"):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"{v:.1f}{suffix}"


def render_row_long(r):
    upside = None
    if r.get("target_mean") and r.get("price"):
        upside = (r["target_mean"] - r["price"]) / r["price"] * 100
    return f"""
    <tr>
      <td><b>{r['symbol']}</b></td>
      <td>${fmt_num(r['price'])}</td>
      <td>{fmt_pct(r.get('mom_3m'))}</td>
      <td>{'○' if r.get('above_sma200') else '×'}</td>
      <td>{fmt_pct(upside)}</td>
      <td>{fmt_ratio(r.get('per'))}</td>
      <td>{fmt_ratio(r.get('pbr'))}</td>
      <td>{fmt_pct(r.get('net_cash_ratio'))}</td>
      <td>{r.get('sector') or '—'}</td>
      <td style="color:#c0392b"><b>{fmt_num(r['long_opportunity'],0)}</b></td>
      <td style="color:#8e44ad"><b>{fmt_num(r['long_risk'],0)}</b></td>
      <td><b>{r.get('long_prediction','—')}</b></td>
      <td>{r.get('next_earnings') or '不明'}</td>
      <td style="font-size:12px;">{r.get('long_situation','—')}</td>
      <td style="font-size:12px;">{r.get('long_outlook','—')}</td>
      <td style="font-size:12px;">{r.get('long_timing','—')}</td>
    </tr>"""


def render_row_major(r):
    upside = None
    if r.get("target_mean") and r.get("price"):
        upside = (r["target_mean"] - r["price"]) / r["price"] * 100
    return f"""
    <tr>
      <td><b>{r['symbol']}</b></td>
      <td>${fmt_num(r['price'])}</td>
      <td>{fmt_pct(r.get('day_change_pct'))}</td>
      <td>{fmt_pct(r.get('mom_3m'))}</td>
      <td>{'○' if r.get('above_sma200') else '×'}</td>
      <td>{fmt_pct(upside)}</td>
      <td>{fmt_ratio(r.get('per'))}</td>
      <td>{fmt_ratio(r.get('pbr'))}</td>
      <td>{fmt_pct(r.get('net_cash_ratio'))}</td>
      <td>{r.get('sector') or '—'}</td>
      <td><b>{r.get('long_prediction','—')}</b></td>
      <td>{r.get('next_earnings') or '不明'}</td>
      <td style="font-size:12px;">{r.get('long_situation','—')}</td>
      <td style="font-size:12px;">{r.get('long_outlook','—')}</td>
      <td style="font-size:12px;">{r.get('long_timing','—')}</td>
    </tr>"""


def render_email_html(day_list, long_list, major_list, universe_size, scanned_size):
    now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M JST")
    day_rows = "".join(render_row_day(r) for r in day_list)
    long_rows = "".join(render_row_long(r) for r in long_list)
    major_rows = "".join(render_row_major(r) for r in major_list)
    major_section = ""
    if major_list:
        major_section = f"""
    <h3>S&P500など主要企業</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      <tr style="background:#222;color:#fff;">
        <th>銘柄</th><th>現在値</th><th>前日比</th><th>3ヶ月騰落率</th><th>200日線上</th>
        <th>目標株価乖離</th><th>PER</th><th>PBR</th><th>ネットキャッシュ比率</th>
        <th>セクター</th><th>予想(1〜3ヶ月)</th><th>次回決算</th>
        <th>現状</th><th>値動きの見立て</th><th>投資タイミングの目安</th>
      </tr>
      {major_rows}
    </table>
    """
    else:
        major_section = """
    <h3>S&P500など主要企業</h3>
    <p style="font-size:12px;color:#666;">
      今回はS&P500構成銘柄リストの取得に失敗したため、この欄は表示できませんでした。
    </p>
    """

    return f"""
    <html><body style="font-family:sans-serif;color:#222;">
    <h2>米国株スキャン結果 ({now})</h2>
    <p style="font-size:12px;color:#666;">
      対象ユニバース: NYSE + NASDAQ + NYSE American, {universe_size}銘柄 /
      データ取得成功: {scanned_size}銘柄 (データ提供元: Webull OpenAPI)
    </p>
    <p style="background:#fff3cd;border:1px solid #ffe08a;padding:10px;border-radius:6px;">
      「利益期待スコア」「リスクスコア」は出来高・値幅・トレンド・アナリスト評価などから
      算出した相対的な目安(1〜100)であり、将来の値動きや利益を保証するものではありません。
      「現状」「値動きの見立て」「投資タイミングの目安」も、RSI・移動平均・MACD・
      ボリンジャーバンド・PER/PBR・ネットキャッシュ比率など過去の指標から機械的に
      導いたルールベースの説明であり、将来の株価を予測するものではありません。
      PER/PBR/ネットキャッシュ比率はWebull OpenAPIから取得できた場合のみ表示され、
      取得できない銘柄は「—」表示になります。本メールは投資助言ではありません。
      発注前に必ずご自身のブローカー(Webull等)で最新の価格・決算日をご確認ください。
    </p>

    {major_section}

    <h3 style="margin-top:24px;">デイトレード候補</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      <tr style="background:#222;color:#fff;">
        <th>銘柄</th><th>現在値</th><th>前日比</th><th>ATR%</th><th>相対出来高</th>
        <th>RSI14</th><th>利益期待</th><th>リスク</th><th>予想(1〜5営業日)</th><th>次回決算</th>
        <th>現状</th><th>値動きの見立て</th><th>投資タイミングの目安</th>
      </tr>
      {day_rows}
    </table>

    <h3 style="margin-top:24px;">長期・成長期待候補</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      <tr style="background:#222;color:#fff;">
        <th>銘柄</th><th>現在値</th><th>3ヶ月騰落率</th><th>200日線上</th>
        <th>目標株価乖離</th><th>PER</th><th>PBR</th><th>ネットキャッシュ比率</th>
        <th>セクター</th><th>利益期待</th><th>リスク</th><th>予想(1〜3ヶ月)</th><th>次回決算</th>
        <th>現状</th><th>値動きの見立て</th><th>投資タイミングの目安</th>
      </tr>
      {long_rows}
    </table>

    <p style="margin-top:24px;font-size:12px;color:#666;">
      データ出典: 価格・出来高・テクニカル指標はWebull OpenAPI(米国市場向けMarket Data API)、
      決算日・PER・PBR・時価総額・アナリスト目標株価・セクターはyfinance(Yahoo Finance)—
      いずれも遅延・欠損の可能性があります。<br>
      S&P500構成銘柄リスト出典: GitHub公開データセット(datasets/s-and-p-500-companies)。<br>
      決算日は取得できない場合があります。発表日をまたぐ保有はギャップリスクに注意してください。
    </p>
    </body></html>
    """


def _json_safe(v):
    """NaN/NaT/numpy型/日付型などをJSONに安全な値に変換する"""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return None if np.isnan(v) else float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (np.datetime64, pd.Timestamp)):
        if pd.isna(v):
            return None
        return pd.Timestamp(v).strftime("%Y-%m-%d")
    if isinstance(v, (datetime,)):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, float):
        return v
    return v


def _clean_records(records: list[dict]) -> list[dict]:
    return [{k: _json_safe(v) for k, v in r.items()} for r in records]


def _atomic_write_json(path: str, obj) -> None:
    """
    一時ファイルに書き込んでから os.replace でファイル差し替えを行う。
    書き込み途中で例外が起きても、既存の path の中身は破壊されない
    (open(path, "w") で直接書くと、途中失敗時にファイルが空になるバグがあった)。
    """
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)  # 同一ファイルシステム内でのrenameはアトミック


# --------------------------------------------------------------------------
# 分析タブ用データ: 銘柄ごとの株価履歴(日足/当日イントラデイ)と予想ログ
# --------------------------------------------------------------------------
#
# GitHub Pages上の分析タブ(index.html)は、指定した銘柄の株価推移グラフと
# 過去の予想・的中判定を表示するために以下のファイル群を参照する。
# リポジトリの肥大化を避けるため、対象は「メールに実際に載った銘柄」
# (day_trade_list + long_term_list + major_list に登場した銘柄)に限定する。

HISTORY_DIR = os.path.join(DATA_DIR, "history")     # 銘柄ごとの日足終値履歴
INTRADAY_DIR = os.path.join(DATA_DIR, "intraday")   # 銘柄ごとの当日イントラデイ履歴
PREDICTIONS_LOG_PATH = os.path.join(DATA_DIR, "predictions_log.json")

MAX_HISTORY_DAYS_KEPT = 200        # 日足履歴の保持日数(長期3ヶ月分析+バックテストに十分な余裕)
MAX_INTRADAY_POINTS_KEPT = 200     # 当日イントラデイの保持ポイント数上限
MAX_PREDICTIONS_LOG_KEPT = 8000    # 予想ログ全体の保持件数上限


def _watched_symbols(day_list, long_list, major_list) -> set[str]:
    return {
        r.get("symbol")
        for r in (day_list + long_list + major_list)
        if r.get("symbol")
    }


def update_price_histories(history: dict, watched_symbols: set[str], run_dt: datetime) -> None:
    """
    日足終値履歴を銘柄ごとの data/history/<SYMBOL>.json に保存する。
    同じ日(JST基準)の終値は、1日に何度スキャンを回しても1エントリのみ保持
    (最新の値で上書き)する。
    """
    try:
        os.makedirs(HISTORY_DIR, exist_ok=True)
        today_str = run_dt.strftime("%Y-%m-%d")
        for sym in watched_symbols:
            df = history.get(sym)
            try:
                if df is None or df.empty or "Close" not in df.columns:
                    continue
                close = float(df["Close"].dropna().iloc[-1])
                if np.isnan(close):
                    continue
                path = os.path.join(HISTORY_DIR, f"{sym}.json")
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    with open(path, "r", encoding="utf-8") as f:
                        hist = json.load(f)
                else:
                    hist = []
                if hist and hist[-1]["date"] == today_str:
                    hist[-1]["close"] = close
                else:
                    hist.append({"date": today_str, "close": close})
                if MAX_HISTORY_DAYS_KEPT:
                    hist = hist[-MAX_HISTORY_DAYS_KEPT:]
                _atomic_write_json(path, hist)
            except Exception as e:
                print(f"[warn] price history 更新失敗 ({sym}): {e}")
    except Exception as e:
        print(f"[warn] price history 更新処理に失敗しました: {e}")
        traceback.print_exc()


def update_intraday_prices(day_list, long_list, major_list, run_dt: datetime) -> None:
    """
    当日のイントラデイ株価点を data/intraday/<SYMBOL>.json に追記する。
    日付(JST)が変わったら自動的にリセットされる。分析タブの「1日」スケールの
    グラフに使用する。
    """
    try:
        os.makedirs(INTRADAY_DIR, exist_ok=True)
        today_str = run_dt.strftime("%Y-%m-%d")
        ts_str = run_dt.strftime("%H:%M")
        latest_price = {}
        for r in (day_list + long_list + major_list):
            sym = r.get("symbol")
            price = r.get("price")
            if sym and price is not None:
                latest_price[sym] = price  # 同一銘柄が複数リストにあれば同じ値のはず
        for sym, price in latest_price.items():
            try:
                path = os.path.join(INTRADAY_DIR, f"{sym}.json")
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    with open(path, "r", encoding="utf-8") as f:
                        day_data = json.load(f)
                else:
                    day_data = {"date": today_str, "points": []}
                if day_data.get("date") != today_str:
                    day_data = {"date": today_str, "points": []}
                day_data["points"].append({"t": ts_str, "price": float(price)})
                if MAX_INTRADAY_POINTS_KEPT:
                    day_data["points"] = day_data["points"][-MAX_INTRADAY_POINTS_KEPT:]
                _atomic_write_json(path, day_data)
            except Exception as e:
                print(f"[warn] intraday 更新失敗 ({sym}): {e}")
    except Exception as e:
        print(f"[warn] intraday 更新処理に失敗しました: {e}")
        traceback.print_exc()


def append_predictions_log(day_list, long_list, major_list, run_dt: datetime) -> None:
    """
    「いつ・どの銘柄に・どの予想をしたか」を1つのJSONファイル
    (data/predictions_log.json)に集約して追記する。分析タブはこのログと
    data/history/<SYMBOL>.json の実際の終値を突き合わせて的中判定を行う。
    同じ銘柄・同じ予想種別(day/long)・同じ日(JST)の予想は、1日に何度
    スキャンを回しても1件のみ記録する。
    """
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        if os.path.exists(PREDICTIONS_LOG_PATH) and os.path.getsize(PREDICTIONS_LOG_PATH) > 0:
            with open(PREDICTIONS_LOG_PATH, "r", encoding="utf-8") as f:
                log = json.load(f)
        else:
            log = {"entries": []}

        made_date = run_dt.strftime("%Y-%m-%d")
        existing_keys = {
            (e.get("symbol"), e.get("kind"), e.get("made_date")) for e in log["entries"]
        }

        new_entries = []
        seen_this_run = set()

        def _maybe_add(r, kind, pred_field):
            sym = r.get("symbol")
            category = r.get(pred_field)
            price = r.get("price")
            if not sym or not category or price is None:
                return
            key = (sym, kind, made_date)
            if key in existing_keys or key in seen_this_run:
                return
            seen_this_run.add(key)
            new_entries.append({
                "symbol": sym,
                "kind": kind,
                "made_date": made_date,
                "made_at": run_dt.strftime("%Y-%m-%d %H:%M:%S JST"),
                "horizon_end": prediction_horizon_end(run_dt, kind),
                "category": category,
                "price_at_prediction": price,
            })

        for r in (day_list + major_list):
            _maybe_add(r, "day", "day_prediction")
        for r in (long_list + major_list):
            _maybe_add(r, "long", "long_prediction")

        log["entries"].extend(_clean_records(new_entries))
        if MAX_PREDICTIONS_LOG_KEPT:
            log["entries"] = log["entries"][-MAX_PREDICTIONS_LOG_KEPT:]
        _atomic_write_json(PREDICTIONS_LOG_PATH, log)
        print(f"[info] predictions_log 追記: {len(new_entries)}件")
    except Exception as e:
        print(f"[warn] predictions_log 更新に失敗しました: {e}")
        traceback.print_exc()


def save_json_snapshot(day_list, long_list, major_list, universe_size, scanned_size) -> str | None:
    """
    スキャン結果をJSONスナップショットとして data/ に保存し、
    data/index.json (スナップショット一覧) を更新する。
    Web閲覧ページ(docs/index.html)はこのファイル群を読み込んで
    ソート・過去データ閲覧を行う。
    失敗してもメール送信自体は止めたくないので、例外はここで握りつぶし
    Noneを返す(呼び出し側で警告ログのみ出す)。
    """
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        now = datetime.now(timezone(timedelta(hours=9)))
        run_id = now.strftime("%Y%m%d_%H%M%S")
        filename = f"scan_{run_id}.json"
        filepath = os.path.join(DATA_DIR, filename)

        snapshot = {
            "run_id": run_id,
            "generated_at_jst": now.strftime("%Y-%m-%d %H:%M:%S JST"),
            "universe_size": universe_size,
            "scanned_size": scanned_size,
            "day_trade": _clean_records(day_list),
            "long_term": _clean_records(long_list),
            "major": _clean_records(major_list),
        }
        _atomic_write_json(filepath, snapshot)

        # index.json (全スナップショットの一覧。閲覧ページの日付セレクタ用)
        if os.path.exists(INDEX_JSON_PATH) and os.path.getsize(INDEX_JSON_PATH) > 0:
            with open(INDEX_JSON_PATH, "r", encoding="utf-8") as f:
                index = json.load(f)
        else:
            index = {"snapshots": []}

        index["snapshots"].append({
            "run_id": run_id,
            "file": filename,
            "generated_at_jst": snapshot["generated_at_jst"],
        })
        # 新しい順に並べ、上限を超えた古いスナップショットは削除
        index["snapshots"].sort(key=lambda s: s["run_id"], reverse=True)
        if MAX_SNAPSHOTS_KEPT:
            removed = index["snapshots"][MAX_SNAPSHOTS_KEPT:]
            index["snapshots"] = index["snapshots"][:MAX_SNAPSHOTS_KEPT]
            for r in removed:
                old_path = os.path.join(DATA_DIR, r["file"])
                if os.path.exists(old_path):
                    os.remove(old_path)

        _atomic_write_json(INDEX_JSON_PATH, index)

        print(f"[info] JSONスナップショット保存: {filepath}")
        return filepath
    except Exception as e:
        print(f"[warn] JSONスナップショット保存に失敗しました: {e}")
        traceback.print_exc()
        return None


def send_email(html_body: str) -> bool:
    """
    メールを送信する。GMAIL_USER / GMAIL_APP_PASSWORD が未設定の場合は
    送信せず False を返す(呼び出し側でHTMLファイル保存にフォールバックする)。
    """

    gmail_user = os.environ["GMAIL_USER"]
    gmail_app_password = os.environ["GMAIL_APP_PASSWORD"]
    gmail_to = os.environ.get("GMAIL_TO", gmail_user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"あめりか米国株スキャン結果 {datetime.now().strftime('%Y-%m-%d')}"
    msg["From"] = gmail_user
    msg["To"] = gmail_to
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, [gmail_to], msg.as_string())
    print("[info] email sent")
    return True

def main():
    try:
        day_list, long_list, major_list, universe, scanned_size, history = run_scan()
        html = render_email_html(day_list, long_list, major_list, len(universe), scanned_size)

        # Web閲覧ページ(GitHub Pages)用にJSONスナップショットを保存
        save_json_snapshot(day_list, long_list, major_list, len(universe), scanned_size)

        # 分析タブ用: 株価履歴・予想ログを更新(失敗してもメール送信は止めない)
        run_dt = datetime.now(timezone(timedelta(hours=9)))
        watched = _watched_symbols(day_list, long_list, major_list)
        update_price_histories(history, watched, run_dt)
        update_intraday_prices(day_list, long_list, major_list, run_dt)
        append_predictions_log(day_list, long_list, major_list, run_dt)

        sent = False
        try:
            sent = send_email(html)
        except Exception as e:
            print(f"[warn] email send failed: {e}")

        if not sent:
            out_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f"scan_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
            )
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"[info] メール未送信のため結果をHTMLファイルに保存しました: {out_path}")
    except Exception:
        print("[error] scan failed:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
