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
- **保有株(ポジション)分析機能について**: `fetch_holdings()` / `analyze_holdings()`
  はWebull公式ドキュメント(Account List / Account Positions)に基づいて実装
  していますが、こちらも同様に実機での動作確認ができていません。Trading API
  はMarket Data APIとは別に権限付与が必要な場合があるため、401エラーになる
  場合はApp KeyのTrading権限設定を確認してください。うまく動かない場合は
  `SCAN_SKIP_HOLDINGS=1` を設定すればスキャン本体には影響しません。
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
    WEBULL_TRADE_API_ENDPOINT (保有株取得用。省略時は WEBULL_API_ENDPOINT と同じロジックの既定値)
    WEBULL_ACCOUNT_ID        (保有株を取得する口座IDを固定したい場合。省略時は口座一覧から自動取得)
    SCAN_SKIP_HOLDINGS       (1/true/yes で保有株分析をスキップ。App Keyに未Trading権限の場合などに利用)
    GMAIL_USER / GMAIL_APP_PASSWORD / GMAIL_TO  (メール送信用、元スクリプトと同じ)
"""

import os
import sys
import math
import time
import json
import random
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
from webull.trade.trade_client import TradeClient

# 強化学習(文脈的バンディット)による予想モデル。scikit-learn が入っていない
# 環境でも本体スクリプトが止まらないよう、失敗しても従来のルールベース予想に
# フォールバックする。
try:
    import ml_learning as rl
    RL_AVAILABLE = rl.SKLEARN_AVAILABLE
except Exception as _rl_import_err:  # noqa: N816
    rl = None
    RL_AVAILABLE = False
    print(f"[warn] ml_learning(強化学習モジュール)を読み込めませんでした。"
          f"ルールベース予想にフォールバックします: {_rl_import_err}")

# ニュース見出しの辞書ベース(自前・無料)センチメントスコアリング。
# yfinanceのnewsが取れない/形式が変わった場合でも本体は止めず、
# 中立(0.0)にフォールバックする。
try:
    import news_sentiment as ns
    NEWS_SENTIMENT_AVAILABLE = True
except Exception as _ns_import_err:  # noqa: N816
    ns = None
    NEWS_SENTIMENT_AVAILABLE = False
    print(f"[warn] news_sentiment(ニュース感情分析モジュール)を読み込めませんでした。"
          f"ニュース特徴量は中立値で扱います: {_ns_import_err}")


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


def build_webull_trade_client() -> TradeClient:
    """
    保有株(ポジション)取得用の Trading API クライアント。
    Market Data API 用の build_webull_client() と同じ App Key/Secret/region を
    使い回す前提だが、エンドポイントホストが Market Data API と異なる場合に
    備えて WEBULL_TRADE_API_ENDPOINT で個別に上書きできるようにしてある
    (未設定時は WEBULL_API_ENDPOINT のロジックと同じ既定値を使う)。
    Trading API の利用には、Webull OpenAPI Management 側で Trading 権限が
    有効になっている必要がある(Market Data権限のみのAppKeyでは401になる)。
    """
    app_key = os.environ.get("WEBULL_APP_KEY")
    app_secret = os.environ.get("WEBULL_APP_SECRET")

    region = os.environ.get("WEBULL_REGION")
    if not region:
        region = "jp" if app_key.startswith("jp.") else "us"

    env = "test" if os.environ.get("WEBULL_USE_SANDBOX", "").lower() in ("1", "true", "yes") else "prod"
    default_endpoint = _DEFAULT_ENDPOINTS.get(region, _DEFAULT_ENDPOINTS["us"])[env]
    endpoint = os.environ.get("WEBULL_TRADE_API_ENDPOINT", os.environ.get("WEBULL_API_ENDPOINT", default_endpoint))

    print(f"[info] Webull OpenAPI (Trading) region={region} endpoint={endpoint}")

    api_client = ApiClient(app_key, app_secret, region)
    api_client.add_endpoint(region, endpoint)
    return TradeClient(api_client)


def _to_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _pick(d: dict, *keys):
    """
    dictから、複数の候補キー名のうち最初に見つかった「意味のある値」を返す。
    Webull OpenAPIのレスポンスはSDKバージョン・エンドポイントによって
    snake_case / camelCase / 別名(quantity, position など)が混在するため、
    キー名を決め打ちすると保有数量などが None になってしまう。
    ネストしたdict("position": {...} 等)も1段だけ再帰的に探索する。
    """
    if not isinstance(d, dict):
        return None
    lowered = {str(k).lower().replace("_", ""): v for k, v in d.items()}
    for k in keys:
        kk = str(k).lower().replace("_", "")
        if kk in lowered:
            v = lowered[kk]
            if v is not None and v != "":
                return v
    # 1段だけネストを探索
    for v in d.values():
        if isinstance(v, dict):
            got = _pick(v, *keys)
            if got is not None:
                return got
    return None


# 保有数量を表しうるキー名の候補(実機レスポンスの表記ゆれ対策)
QTY_KEYS = (
    "qty", "quantity", "position", "position_qty", "positionQty",
    "holding_qty", "holdingQty", "holding_quantity", "holdingQuantity",
    "shares", "share_qty", "total_qty", "totalQuantity", "total_quantity",
    "available_qty", "availableQuantity", "available_quantity",
    "long_qty", "longQuantity", "sellable_qty", "sellableQuantity",
)
UNIT_COST_KEYS = (
    "unit_cost", "unitCost", "avg_cost", "avgCost", "average_cost", "averageCost",
    "cost_price", "costPrice", "avg_price", "avgPrice", "open_price", "openPrice",
)
TOTAL_COST_KEYS = ("total_cost", "totalCost", "cost", "cost_amount", "costAmount", "position_cost", "positionCost")
LAST_PRICE_KEYS = ("last_price", "lastPrice", "market_price", "marketPrice", "price", "close", "latest_price", "latestPrice")
MARKET_VALUE_KEYS = ("market_value", "marketValue", "mkt_val", "mktVal", "position_value", "positionValue", "total_market_value")
PL_KEYS = ("unrealized_profit_loss", "unrealizedProfitLoss", "unrealized_pl", "unrealizedPl", "unrealized_pnl", "unrealizedPnl", "float_profit_loss")
PL_RATE_KEYS = ("unrealized_profit_loss_rate", "unrealizedProfitLossRate", "unrealized_pl_rate", "profit_loss_rate", "profitLossRate", "pl_ratio")
SYMBOL_KEYS = ("symbol", "ticker", "tickerSymbol", "ticker_symbol", "disSymbol", "dis_symbol", "instrument_symbol")


def _normalize_position(h: dict, account_id: str) -> dict:
    """
    生のポジションレスポンス1件を、表記ゆれを吸収した共通フォーマットに変換する。
    数量が直接取れない場合は 評価額÷現在値 / 取得総額÷平均取得単価 から逆算する。
    """
    qty = _to_float(_pick(h, *QTY_KEYS))
    unit_cost = _to_float(_pick(h, *UNIT_COST_KEYS))
    total_cost = _to_float(_pick(h, *TOTAL_COST_KEYS))
    last_price = _to_float(_pick(h, *LAST_PRICE_KEYS))
    market_value = _to_float(_pick(h, *MARKET_VALUE_KEYS))
    pl = _to_float(_pick(h, *PL_KEYS))
    pl_rate = _to_float(_pick(h, *PL_RATE_KEYS))

    # --- 数量の逆算(APIが数量を返さない/キー名が想定外だった場合の保険) ---
    if not qty:
        if market_value and last_price:
            qty = market_value / last_price
        elif total_cost and unit_cost:
            qty = total_cost / unit_cost
        if qty:
            # 端株でなければ整数に丸める(浮動小数点誤差で 2.9999 等になるのを防ぐ)
            if abs(qty - round(qty)) < 0.01:
                qty = float(round(qty))
            print(f"[info] {_pick(h, *SYMBOL_KEYS)}: 保有数量をAPIから直接取得できなかったため "
                  f"評価額/単価から {qty} と逆算しました")

    # --- 取得原価・評価額・含み損益の補完 ---
    if total_cost is None and qty and unit_cost:
        total_cost = qty * unit_cost
    if unit_cost is None and qty and total_cost:
        unit_cost = total_cost / qty
    if market_value is None and qty and last_price:
        market_value = qty * last_price
    if pl is None and market_value is not None and total_cost is not None:
        pl = market_value - total_cost
    if pl_rate is None and total_cost:
        pl_rate = (market_value - total_cost) / total_cost * 100 if market_value is not None else None

    return {
        "account_id": account_id,
        "symbol": _pick(h, *SYMBOL_KEYS),
        "instrument_id": _pick(h, "instrument_id", "instrumentId"),
        "currency": _pick(h, "currency"),
        "qty": qty,
        "unit_cost": unit_cost,
        "total_cost": total_cost,
        "last_price": last_price,
        "market_value": market_value,
        "unrealized_profit_loss": pl,
        "unrealized_profit_loss_rate": pl_rate,
        "holding_proportion": _to_float(_pick(h, "holding_proportion", "holdingProportion")),
        "raw_keys": sorted(h.keys()) if isinstance(h, dict) else [],
    }


def fetch_holdings() -> list[dict]:
    """
    Webull口座の保有株(ポジション)一覧を取得する。
    - 環境変数 WEBULL_ACCOUNT_ID が設定されていればその口座のみ、未設定なら
      get_account_list() で取得できる全口座を対象にする。
    - 注意: このサンドボックス環境ではTrading APIを実際に呼び出しての動作確認が
      できていません。公式ドキュメント(Account Positions)に基づき実装して
      いますが、実際のSDKバージョンによって関数の引数名やレスポンスの
      キー名(camelCase/snake_case等)が異なる可能性があります。エラーになる
      場合は、実際のレスポンス内容を確認の上でこの関数を調整してください。
    - 取得に失敗しても呼び出し側の処理を止めたくないため、例外は握りつぶし
      空リストを返す。
    """
    try:
        trade_client = build_webull_trade_client()

        account_ids = []
        forced_account_id = os.environ.get("WEBULL_ACCOUNT_ID")
        if forced_account_id:
            account_ids = [forced_account_id]
        else:
            acc_res = trade_client.account_v2.get_account_list()
            if acc_res.status_code != 200:
                print(f"[warn] 口座一覧の取得に失敗しました: {acc_res.status_code} {acc_res.text[:300]}")
                return []
            acc_json = acc_res.json()
            acc_list = acc_json.get("data") if isinstance(acc_json, dict) else acc_json
            for acc in (acc_list or []):
                aid = acc.get("account_id") or acc.get("accountId") or acc.get("id")
                if aid:
                    account_ids.append(aid)

        if not account_ids:
            print("[warn] 有効な口座IDが見つかりませんでした(保有株分析をスキップします)")
            return []

        holdings_all = []
        for account_id in account_ids:
            last_instrument_id = None
            page = 0
            while True:
                page += 1
                try:
                    if last_instrument_id:
                        pos_res = trade_client.account_v2.get_account_position(
                            account_id, page_size=100, last_instrument_id=last_instrument_id
                        )
                    else:
                        pos_res = trade_client.account_v2.get_account_position(account_id, page_size=100)
                except TypeError:
                    # SDKバージョンによりキーワード引数を受け付けない場合のフォールバック
                    pos_res = trade_client.account_v2.get_account_position(account_id)

                if pos_res.status_code != 200:
                    print(f"[warn] 保有株取得に失敗しました (account={account_id}): "
                          f"{pos_res.status_code} {pos_res.text[:300]}")
                    break

                pos_json = pos_res.json()
                # SDKバージョンによってレスポンス形式が
                # {"holdings": [...], "has_next": bool} や {"data": [...]} の
                # dict形式の場合と、[...] のようにholdingsの配列が直接返る
                # list形式の場合がある(実機確認により後者のケースを確認済み)。
                if isinstance(pos_json, list):
                    raw_holdings = pos_json
                    has_next = False  # list形式ではページング情報が無いため1ページのみ扱う
                elif isinstance(pos_json, dict):
                    raw_holdings = pos_json.get("holdings") or pos_json.get("data") or []
                    # "data"キーの中にさらに配列ではなくdictが入れ子になっているケースへの保険
                    if isinstance(raw_holdings, dict):
                        raw_holdings = raw_holdings.get("holdings") or raw_holdings.get("list") or []
                    has_next = bool(pos_json.get("has_next"))
                else:
                    raw_holdings = []
                    has_next = False

                for h in raw_holdings:
                    if not isinstance(h, dict):
                        continue
                    if page == 1 and not holdings_all:
                        # 実機レスポンスのキー名を1件だけログ出力しておく。
                        # 数量が取れない場合、ここを見ればどのキー名かが分かる。
                        print(f"[debug] ポジションのキー一覧: {sorted(h.keys())}")
                    norm = _normalize_position(h, account_id)
                    if norm.get("qty") in (None, 0):
                        print(f"[warn] {norm.get('symbol')}: 保有数量を特定できませんでした。"
                              f"レスポンスのキー: {norm.get('raw_keys')}")
                    holdings_all.append(norm)

                if not has_next or not raw_holdings or page > 20:
                    break
                last = raw_holdings[-1]
                last_instrument_id = last.get("instrument_id") if isinstance(last, dict) else None
                if not last_instrument_id:
                    break

        print(f"[info] 保有株取得: {len(holdings_all)}件 (口座数 {len(account_ids)})")
        return holdings_all
    except Exception as e:
        print(f"[warn] 保有株の取得処理に失敗しました: {e}")
        traceback.print_exc()
        return []


def analyze_holdings(data_client: DataClient, holdings: list[dict]) -> list[dict]:
    """
    保有株ごとに、スキャン本体と同じロジック(テクニカル指標・利益期待/リスク
    スコア・現状/見立て/タイミングの解説文・値動き予想)を計算し、保有株固有の
    情報(保有数量・平均取得単価・評価額・含み損益など)と合わせて返す。
    保有株は銘柄数が少ないため、ユニバース全体のバッチ処理とは別に個別取得する。
    """
    symbols = sorted({h["symbol"] for h in holdings if h.get("symbol")})
    if not symbols:
        return []
    print(f"[info] 保有株 {len(symbols)}銘柄の分析を開始します: {symbols}")

    try:
        history = download_history_batched(data_client, symbols)
    except Exception as e:
        print(f"[warn] 保有株の価格履歴取得に失敗しました: {e}")
        history = {}

    # 保有銘柄は「無条件で必ず調べる」対象なので、バッチ取得で欠落した銘柄は
    # 個別に複数回再試行してでも取得を試みる(ユニバースの絞り込みとは無関係)。
    missing_syms = [
        s for s in symbols
        if history.get(s) is None or (hasattr(history.get(s), "empty") and history[s].empty)
    ]
    if missing_syms:
        print(f"[info] 保有株 {len(missing_syms)}銘柄は価格履歴が未取得のため個別に再試行します: {missing_syms}")
        try:
            retried = download_history_individual_retry(data_client, missing_syms)
            history.update(retried)
            still_missing = [s for s in missing_syms if s not in retried]
            if still_missing:
                print(f"[warn] 個別再試行後もなお価格履歴が取得できなかった保有銘柄: {still_missing}")
        except Exception as e:
            print(f"[warn] 保有株の個別再取得処理に失敗しました: {e}")

    prefetch_fundamentals(symbols)

    by_symbol_holdings = {}
    for h in holdings:
        by_symbol_holdings.setdefault(h["symbol"], []).append(h)

    results = []
    for sym in symbols:
        # 同一銘柄が複数口座にまたがる場合は数量・評価額等を合算する
        hs = by_symbol_holdings[sym]
        qty_total = sum(h.get("qty") or 0 for h in hs)
        cost_total = sum(h.get("total_cost") or 0 for h in hs)
        mv_total = sum(h.get("market_value") or 0 for h in hs)
        pl_total = sum(h.get("unrealized_profit_loss") or 0 for h in hs)
        unit_cost = (cost_total / qty_total) if qty_total else (hs[0].get("unit_cost"))

        df = history.get(sym)
        row = compute_technical_row(sym, df) if df is not None else None
        has_tech = row is not None
        if row is None:
            row = {"symbol": sym, "price": hs[0].get("last_price")}

        # --- 数量・評価額の最終フォールバック ---
        # ここまでで数量が取れていない場合、現在値と評価額から逆算する。
        cur_price = row.get("price") or hs[0].get("last_price")
        if not qty_total and mv_total and cur_price:
            qty_total = mv_total / cur_price
            if abs(qty_total - round(qty_total)) < 0.01:
                qty_total = float(round(qty_total))
        if not mv_total and qty_total and cur_price:
            mv_total = qty_total * cur_price
        if not cost_total and qty_total and unit_cost:
            cost_total = qty_total * unit_cost
        if not pl_total and mv_total and cost_total:
            pl_total = mv_total - cost_total

        pl_rate = ((mv_total - cost_total) / cost_total * 100) if cost_total else hs[0].get("unrealized_profit_loss_rate")

        fund = fetch_fundamentals(sym)

        if has_tech:
            dt_opp, dt_risk = score_day_trade(row)
            lt_opp, lt_risk = score_long_term(row, fund)
            long_commentary = build_commentary(row, fund, "long")
            day_commentary = build_commentary(row, fund, "day")
            day_pred = predict_category(row, "day")
            long_pred = predict_category(row, "long")
        else:
            dt_opp = dt_risk = lt_opp = lt_risk = None
            note = "テクニカル指標を計算するための十分な価格履歴データが取得できませんでした。"
            long_commentary = {"situation": note, "outlook": "—", "timing": "—"}
            day_commentary = {"situation": note, "outlook": "—", "timing": "—"}
            day_pred = long_pred = None

        results.append({
            **row,
            **fund,
            "is_holding": True,
            "accounts": sorted({h.get("account_id") for h in hs if h.get("account_id")}),
            "qty": qty_total or None,
            "unit_cost": unit_cost,
            "total_cost": cost_total or None,
            "market_value": mv_total or None,
            "unrealized_pl": pl_total or None,
            "unrealized_pl_rate": pl_rate,
            "currency": hs[0].get("currency"),
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
            "day_prediction": day_pred,
            "long_prediction": long_pred,
        })

    # 評価額の大きい順に並べる(評価額が取れない銘柄は末尾に)
    results.sort(key=lambda r: (r.get("market_value") is None, -(r.get("market_value") or 0)))
    return results


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


HISTORY_BATCH_MAX_RETRIES = 3   # 通信エラー/一時的な失敗時に、バッチ全体を再試行する回数
HISTORY_BATCH_RETRY_SLEEP = 2.0  # 再試行前に待つ秒数(試行回数に応じて漸増)


def download_history_batched(data_client: DataClient, tickers: list[str]) -> dict[str, pd.DataFrame]:
    timespan = _resolve_daily_timespan()
    out: dict[str, pd.DataFrame] = {}
    n = len(tickers)
    invalid_symbols_seen = 0

    for i in range(0, n, BATCH_SIZE):
        original_chunk = tickers[i:i + BATCH_SIZE]
        if (i // BATCH_SIZE) % 20 == 0:
            print(f"[info] downloading history {i}-{i + len(original_chunk)} / {n} (Webull OpenAPI)")

        chunk_result: dict[str, pd.DataFrame] = {}
        parsed_any = False
        aborted = False

        # 通信エラー・一時的なHTTPエラーは、バッチ全体を最大 HISTORY_BATCH_MAX_RETRIES 回まで
        # 再試行する(「価格履歴データが取得できませんでした」という判定になる前に、
        # ネットワーク瞬断などの一時的な失敗をできるだけ吸収するため)。
        for batch_attempt in range(1, HISTORY_BATCH_MAX_RETRIES + 1):
            chunk = list(original_chunk)

            # 無効シンボル(Webull側に存在しない銘柄)を1件ずつ除外しながら再試行する。
            # NASDAQ公式リスト/GitHubミラーにはあるがWebullが未対応の銘柄
            # (一部ワラント・優先株・新規上場直後の銘柄など)が一定数混じるため必須。
            res = None
            transient_error = False
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
                    print(f"[warn] batch bars call failed for chunk starting {chunk[0]} "
                          f"(attempt {batch_attempt}/{HISTORY_BATCH_MAX_RETRIES}): {e}")
                    transient_error = True
                    break
                else:
                    break

            if not chunk:
                break  # 除外の結果、対象が0件になった(このバッチは何も取得しない)

            if transient_error:
                if batch_attempt < HISTORY_BATCH_MAX_RETRIES:
                    time.sleep(HISTORY_BATCH_RETRY_SLEEP * batch_attempt)
                    continue  # バッチ全体を再試行
                print(f"[warn] chunk starting {chunk[0]} は{HISTORY_BATCH_MAX_RETRIES}回再試行しましたが失敗しました")
                aborted = True
                break

            if res is None:
                break

            if res.status_code == 403:
                print("[error] 403: OpenAPIの市場データサブスクリプションが未契約の可能性があります。中断します。")
                aborted = True
                break
            if res.status_code != 200:
                print(f"[warn] batch bars HTTP {res.status_code} for chunk starting {chunk[0]} "
                      f"(attempt {batch_attempt}/{HISTORY_BATCH_MAX_RETRIES}): {res.text[:300]}")
                if batch_attempt < HISTORY_BATCH_MAX_RETRIES:
                    time.sleep(HISTORY_BATCH_RETRY_SLEEP * batch_attempt * 2)
                    continue  # バッチ全体を再試行
                print(f"[warn] chunk starting {chunk[0]} は{HISTORY_BATCH_MAX_RETRIES}回再試行しましたが失敗しました")
                break

            data = res.json()
            chunk_result, parsed_any = _parse_batch_bars_response(data, chunk)
            break  # 成功したのでリトライループを抜ける

        if aborted and res is not None and getattr(res, "status_code", None) == 403:
            break  # サブスクリプション未契約はリトライしても無駄なので全体を中断

        out.update(chunk_result)

        if not parsed_any and i == 0:
            # 最初のバッチだけ、解釈できなかった場合に生JSONの先頭を出す(デバッグ用)
            try:
                print(f"[debug] batch bars response (raw, first 500 chars): {str(res.json())[:500]}")
            except Exception:
                pass

        time.sleep(BATCH_SLEEP_SEC)

    if invalid_symbols_seen:
        print(f"[info] Webull非対応のため除外したシンボル数: {invalid_symbols_seen}")
    return out


def download_history_individual_retry(
    data_client: DataClient, symbols: list[str], max_retries: int = 4, base_sleep: float = 1.5,
) -> dict[str, pd.DataFrame]:
    """
    保有銘柄など「必ず調べたい」少数の銘柄向けに、1銘柄ずつ個別に価格履歴を
    取得する。バッチ取得で失敗/欠落した銘柄に対して、無条件で複数回再試行する
    ことで、一時的な通信エラーによる「データ取得できず」をできるだけ防ぐ。
    """
    if not symbols:
        return {}
    timespan = _resolve_daily_timespan()
    out: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        for attempt in range(1, max_retries + 1):
            try:
                res = data_client.market_data.get_batch_history_bar(
                    [sym], Category.US_STOCK.name, timespan.name
                )
                if res.status_code == 200:
                    data = res.json()
                    chunk_result, parsed_any = _parse_batch_bars_response(data, [sym])
                    if sym in chunk_result:
                        out[sym] = chunk_result[sym]
                        break
                    raise RuntimeError("empty response for symbol")
                elif res.status_code == 403:
                    print("[error] 403: OpenAPIの市場データサブスクリプションが未契約の可能性があります。")
                    return out
                else:
                    raise RuntimeError(f"HTTP {res.status_code}: {res.text[:200]}")
            except Exception as e:
                if attempt >= max_retries:
                    print(f"[warn] {sym}: 個別再取得を{max_retries}回試みましたが取得できませんでした: {e}")
                else:
                    print(f"[info] {sym}: 価格履歴の再取得を試みます ({attempt}/{max_retries}回目): {e}")
                    time.sleep(base_sleep * attempt)
            time.sleep(BATCH_SLEEP_SEC)
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

# yfinanceのリトライ/警告ログ("No earnings dates found, symbol may be delisted" など)は
# 大量の上場廃止・低流動性銘柄で延々と出力され、処理時間も体感を悪くするため抑制する。
try:
    import logging as _logging
    _logging.getLogger("yfinance").setLevel(_logging.CRITICAL)
except Exception:
    pass

# ファンダメンタルズのキャッシュ。
# PER・時価総額・セクター・決算日は1日のうちにほとんど変化しないため、
# 毎時のスキャンで毎回yfinanceを叩き直す必要がない。
FUNDAMENTALS_CACHE_PATH = os.path.join(DATA_DIR, "fundamentals_cache.json")
FUNDAMENTALS_CACHE_TTL_HOURS = float(os.environ.get("SCAN_FUNDAMENTALS_TTL_HOURS", "12"))
# 決算日が取得できなかった銘柄(上場廃止・ADR・ETF等)を記録しておき、
# しばらくの間は get_earnings_dates() を呼ばないようにする。ここが最大のボトルネック。
NO_EARNINGS_TTL_HOURS = float(os.environ.get("SCAN_NO_EARNINGS_TTL_HOURS", "168"))  # 既定7日
FUNDAMENTALS_WORKERS = int(os.environ.get("SCAN_FUNDAMENTALS_WORKERS", "8"))

_fund_cache: dict | None = None
_fund_cache_dirty = False


def _now_ts() -> float:
    return time.time()


def _load_fundamentals_cache() -> dict:
    global _fund_cache
    if _fund_cache is not None:
        return _fund_cache
    try:
        if os.path.exists(FUNDAMENTALS_CACHE_PATH) and os.path.getsize(FUNDAMENTALS_CACHE_PATH) > 0:
            with open(FUNDAMENTALS_CACHE_PATH, "r", encoding="utf-8") as f:
                _fund_cache = json.load(f)
        else:
            _fund_cache = {}
    except Exception as e:
        print(f"[warn] fundamentalsキャッシュの読み込みに失敗しました: {e}")
        _fund_cache = {}
    return _fund_cache


def save_fundamentals_cache() -> None:
    """スキャン終了時にキャッシュを書き出す(失敗しても処理は止めない)"""
    global _fund_cache_dirty
    if _fund_cache is None or not _fund_cache_dirty:
        return
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        # 古すぎるエントリは捨てる(ファイルの肥大化防止)
        cutoff = _now_ts() - max(FUNDAMENTALS_CACHE_TTL_HOURS, NO_EARNINGS_TTL_HOURS) * 3600 * 4
        pruned = {k: v for k, v in _fund_cache.items() if (v.get("fetched_at") or 0) > cutoff}
        _atomic_write_json(FUNDAMENTALS_CACHE_PATH, pruned)
        _fund_cache_dirty = False
        print(f"[info] fundamentalsキャッシュ保存: {len(pruned)}銘柄")
    except Exception as e:
        print(f"[warn] fundamentalsキャッシュの保存に失敗しました: {e}")


EMPTY_FUNDAMENTALS = {
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
    "news_sentiment_score": None,
    "news_volume_7d": None,
    "news_sentiment_conf": None,
}


def _earnings_from_info(info: dict) -> str | None:
    """
    info に含まれる決算日フィールドから次回決算日を取り出す。
    ここで取れれば、低速な get_earnings_dates() を呼ばずに済む。
    """
    today = datetime.now().date()
    candidates = []
    for key in ("earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd"):
        v = info.get(key)
        if v:
            try:
                candidates.append(datetime.fromtimestamp(float(v)).date())
            except (TypeError, ValueError, OSError):
                continue
    cal = info.get("earningsDate") or info.get("earnings_date")
    if isinstance(cal, (list, tuple)):
        for v in cal:
            try:
                candidates.append(pd.Timestamp(v).date())
            except Exception:
                continue
    future = sorted(d for d in candidates if d >= today)
    return future[0].strftime("%Y-%m-%d") if future else None


def _fetch_fundamentals_uncached(symbol: str, skip_earnings_lookup: bool) -> dict:
    result = dict(EMPTY_FUNDAMENTALS)
    info = {}
    try:
        tk = yf.Ticker(symbol)
        info = tk.info or {}
    except Exception as e:
        print(f"[warn] yfinance info fetch failed for {symbol}: {e}")
        tk = None

    result["target_mean"] = info.get("targetMeanPrice")
    result["market_cap"] = info.get("marketCap")
    result["recommendation"] = info.get("recommendationKey")
    result["sector"] = info.get("sector")
    # PERはtrailing優先、無ければforward。PBRはpriceToBook。
    result["per"] = info.get("trailingPE") or info.get("forwardPE")
    result["pbr"] = info.get("priceToBook")
    result["total_cash"] = info.get("totalCash")
    result["total_debt"] = info.get("totalDebt")

    # ネットキャッシュ比率 = (現金 - 有利子負債) / 時価総額
    if result["total_cash"] is not None and result["market_cap"]:
        net_cash = result["total_cash"] - (result["total_debt"] or 0)
        try:
            result["net_cash_ratio"] = float(net_cash) / float(result["market_cap"]) * 100
        except (TypeError, ZeroDivisionError):
            result["net_cash_ratio"] = None

    # まず info から決算日を拾う(追加のHTTPリクエスト不要)
    result["next_earnings"] = _earnings_from_info(info)

    # info から取れず、かつ「決算日が無い銘柄」として記録されていない場合のみ
    # 低速な get_earnings_dates() にフォールバックする。
    # 上場廃止・ETF・ADR等では毎回失敗して時間を浪費するため、
    # info自体が空(=実体が無い銘柄)ならここもスキップする。
    looks_delisted = not info.get("marketCap") and not info.get("sector") and not info.get("shortName")
    if result["next_earnings"] is None and not skip_earnings_lookup and not looks_delisted and tk is not None:
        try:
            cal = tk.get_earnings_dates(limit=4)
            if cal is not None and not cal.empty:
                future = cal[cal.index >= pd.Timestamp.now(tz=cal.index.tz)]
                if not future.empty:
                    result["next_earnings"] = future.index[0].strftime("%Y-%m-%d")
        except Exception:
            # 「No earnings dates found, symbol may be delisted」系はここに来る。
            # 件数が多くログが埋まるため、個別の警告は出さない。
            pass

    # ニュース見出しの辞書ベースセンチメント(追加のHTTPリクエストはyfinance内部で
    # 発生するが、tkは既に生成済みなのでTickerの再生成は不要)。
    if NEWS_SENTIMENT_AVAILABLE and tk is not None:
        try:
            news_result = ns.fetch_and_score_news(tk)
            result["news_sentiment_score"] = news_result["news_sentiment_score"]
            result["news_volume_7d"] = news_result["news_volume_7d"]
            result["news_sentiment_conf"] = news_result["news_sentiment_conf"]
        except Exception as e:
            print(f"[warn] news sentiment fetch failed for {symbol}: {e}")
            result["news_sentiment_score"] = 0.0
            result["news_volume_7d"] = 0
            result["news_sentiment_conf"] = 0.0
    else:
        result["news_sentiment_score"] = 0.0
        result["news_volume_7d"] = 0
        result["news_sentiment_conf"] = 0.0

    return result


def fetch_fundamentals(symbol: str, force: bool = False) -> dict:
    """
    アナリスト目標株価・時価総額・セクター・次回決算日・PER・PBR・
    現金/有利子負債(ネットキャッシュ比率算出用)を yfinance から取得する。

    高速化のため:
      - 取得結果を docs/data/fundamentals_cache.json にキャッシュ(既定12時間有効)
      - 決算日が取れなかった銘柄は一定期間(既定7日)、低速な
        get_earnings_dates() の呼び出しをスキップ
    """
    global _fund_cache_dirty
    cache = _load_fundamentals_cache()
    entry = cache.get(symbol)
    now = _now_ts()

    if entry and not force:
        age_h = (now - (entry.get("fetched_at") or 0)) / 3600
        if age_h < FUNDAMENTALS_CACHE_TTL_HOURS:
            return {k: entry.get("data", {}).get(k) for k in EMPTY_FUNDAMENTALS}

    skip_earnings = False
    if entry and entry.get("no_earnings_at"):
        if (now - entry["no_earnings_at"]) / 3600 < NO_EARNINGS_TTL_HOURS:
            skip_earnings = True

    data = _fetch_fundamentals_uncached(symbol, skip_earnings)

    new_entry = {"fetched_at": now, "data": data}
    if data.get("next_earnings") is None:
        # 決算日が取れなかったことを記録(次回以降しばらくは問い合わせない)
        new_entry["no_earnings_at"] = (entry or {}).get("no_earnings_at") or now
    cache[symbol] = new_entry
    _fund_cache_dirty = True
    return data


def prefetch_fundamentals(symbols: list[str], workers: int | None = None) -> None:
    """
    ファンダメンタルズを並列で先読みしてキャッシュに載せる。
    yfinanceの呼び出しはネットワーク待ちが大半のため、逐次実行だと
    銘柄数×数百msが丸ごと待ち時間になる。ここで並列化しておくと、
    後続の fetch_fundamentals() はキャッシュヒットで即座に返る。
    """
    targets = [s for s in dict.fromkeys(symbols) if s]
    if not targets:
        return
    workers = workers or FUNDAMENTALS_WORKERS
    t0 = time.time()
    done = 0
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(fetch_fundamentals, s): s for s in targets}
            for fut in as_completed(futures):
                done += 1
                try:
                    fut.result()
                except Exception:
                    pass
                if done % 50 == 0:
                    print(f"[info] fundamentals {done}/{len(targets)} 件取得 "
                          f"({time.time()-t0:.0f}秒経過)")
    except Exception as e:
        print(f"[warn] fundamentalsの並列取得に失敗したため逐次取得に切り替えます: {e}")
        for s in targets:
            fetch_fundamentals(s)
    print(f"[info] fundamentals取得完了: {len(targets)}銘柄 / {time.time()-t0:.0f}秒")


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
# 学習による予想の強め/弱めの補正で使う並び順(値下がり側→値上がり側)
CATEGORY_ORDER = ["大きく値下がり", "少し値下がり", "変動なし", "少し値上がり", "大きく値上がり"]


def _isnan(v):
    return v is None or (isinstance(v, float) and np.isnan(v))


def _signal_net(row: dict, kind: str) -> tuple[int, float, float]:
    """テクニカル指標から「値上がり/値下がりシグナルの差(net)」を計算する。
    predict_category と、学習用のパターン鍵生成(_pattern_signature)の両方から
    共通して使われるロジック本体。戻り値: (net, rsi14, atr_pct)"""
    rsi14 = row.get("rsi14") or 50
    above200 = row.get("above_sma200")
    above50 = row.get("above_sma50")
    above20 = row.get("above_sma20")
    mom1 = row.get("mom_1m")
    mom3 = row.get("mom_3m")
    atrp = row.get("atr_pct") or 0
    macd_up = row.get("macd_bullish_cross")
    macd_down = row.get("macd_bearish_cross")

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

    return net, rsi14, atrp


def _rsi_zone(rsi14) -> str:
    if _isnan(rsi14):
        return "na"
    if rsi14 < 35:
        return "low"
    if rsi14 > 65:
        return "high"
    return "mid"


def _atr_zone(atrp) -> str:
    if _isnan(atrp):
        return "na"
    if atrp < 2:
        return "lo"
    if atrp < 5:
        return "mid"
    return "hi"


def _trend_zone(above50, above200) -> str:
    a = "u" if above50 is True else ("d" if above50 is False else "n")
    b = "u" if above200 is True else ("d" if above200 is False else "n")
    return a + b


def prediction_pattern_key(row: dict, kind: str) -> str:
    """予想の「型(パターン)」を表す鍵。同じ型の予想がこれまでどれくらい
    的中してきたかを学習・集計するために使う(predictions_log に保存し、
    次回以降の predict_category での予想の強め/弱め補正に使う)。"""
    net, rsi14, atrp = _signal_net(row, kind)
    net_c = max(-4, min(4, int(round(net))))
    tz = _trend_zone(row.get("above_sma50"), row.get("above_sma200"))
    return f"{kind}|net{net_c}|rsi_{_rsi_zone(rsi14)}|atr_{_atr_zone(atrp)}|tr_{tz}"


_LEARNING_CACHE: dict | None = None


def load_learning_weights(force: bool = False) -> dict:
    """パターン別の的中率学習データ(data/learning_weights.json)を読み込む。
    プロセス内でキャッシュし、同一実行では1回だけディスクから読む。"""
    global _LEARNING_CACHE
    if _LEARNING_CACHE is not None and not force:
        return _LEARNING_CACHE
    data = {"patterns": {}}
    try:
        if os.path.exists(LEARNING_PATH) and os.path.getsize(LEARNING_PATH) > 0:
            with open(LEARNING_PATH, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict) and isinstance(loaded.get("patterns"), dict):
                data = loaded
    except Exception as e:
        print(f"[warn] learning_weights.json 読み込み失敗: {e}")
    _LEARNING_CACHE = data
    return data


def _apply_learning_adjustment(category: str, kind: str, pattern_key: str, learning: dict) -> str:
    """過去の的中率(勝ちパターン/負けパターン)に基づき、予想カテゴリを
    1段階だけ強める/弱める。サンプル数が十分(SIM_LEARN_MIN_SAMPLES以上)
    ある場合のみ反映し、サンプルが少ないうちは元の予想をそのまま使う。

    「変動なし」と予想したケースについては、単純に的中率だけを見ると
    (常に「変動なし」という予想が外れた=不的中、としかカウントされず)
    どちら方向に外れたのかが分からない。そこで別途「変動なし」と予想した
    ときの実際の値動き分布(neutral_miss_up / neutral_miss_down)を見て、
    その型のパターンが実際にはよく上昇/下落していたなら、見送り続けずに
    方向予想へ「昇格」させる(=逃した上昇/下落銘柄のパターンを学習する)。
    """
    stats = (learning.get("patterns") or {}).get(pattern_key)
    if not stats:
        return category

    if category == "変動なし":
        neutral_total = stats.get("neutral_total") or 0
        if neutral_total < SIM_LEARN_MIN_SAMPLES:
            return category
        outcome = stats.get("outcome") or {}
        up_rate = stats.get("neutral_miss_up_rate")
        down_rate = stats.get("neutral_miss_down_rate")
        # どちらの方向にも十分ズレているというデータが取れている場合は、
        # 見送りが優勢な(=判断がつかない)ケースとして「変動なし」のままにする
        if up_rate is not None and up_rate > SIM_LEARN_PROMOTE_RATE and (down_rate or 0) <= SIM_LEARN_DEMOTE_RATE:
            big = outcome.get("大きく値上がり", 0)
            small = outcome.get("少し値上がり", 0)
            return "大きく値上がり" if big >= small else "少し値上がり"
        if down_rate is not None and down_rate > SIM_LEARN_PROMOTE_RATE and (up_rate or 0) <= SIM_LEARN_DEMOTE_RATE:
            big = outcome.get("大きく値下がり", 0)
            small = outcome.get("少し値下がり", 0)
            return "大きく値下がり" if big >= small else "少し値下がり"
        return category

    total = stats.get("total") or 0
    rate = stats.get("rate")
    if total < SIM_LEARN_MIN_SAMPLES or rate is None:
        return category
    try:
        idx = CATEGORY_ORDER.index(category)
    except ValueError:
        return category
    center = CATEGORY_ORDER.index("変動なし")
    if rate < SIM_LEARN_DEMOTE_RATE:
        # 負けパターン: このパターンでの予想は外れが多い→「変動なし」寄りに弱める
        new_idx = idx + (1 if idx < center else -1)
        return CATEGORY_ORDER[new_idx]
    if rate > SIM_LEARN_PROMOTE_RATE and abs(idx - center) == 1:
        # 勝ちパターン: 弱めの予想(少し値上がり/値下がり)がよく当たる→強めの予想に格上げ
        new_idx = idx + (1 if idx > center else -1)
        return CATEGORY_ORDER[new_idx]
    return category


_RL_USAGE_COUNTS = {"model": 0, "explore": 0, "rule_fallback": 0}


def predict_category(row: dict, kind: str) -> str:
    net, rsi14, atrp = _signal_net(row, kind)
    mom3 = row.get("mom_3m")

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

    # 旧来のルールベース学習: 同じ型のパターンでこれまで的中率が低ければ弱め、
    # 高ければ強める(強化学習モデルが未成熟な間のフォールバック/事前分布として使う)
    pattern_key = prediction_pattern_key(row, kind)
    rule_category = _apply_learning_adjustment(category, kind, pattern_key, load_learning_weights())

    # 強化学習(文脈的バンディット): 特徴量から行動(カテゴリ)ごとの期待損益を
    # 予測し、最も期待値の高い行動を選ぶ(ε-greedyで一部は探索)。
    # 学習済みモデルが無い/データ不足の行動については rule_category にフォールバックする。
    rl_model = _get_rl_model()
    if rl_model is not None:
        feats = rl.featurize(row)
        category_final, meta = rl.choose_action(
            rl_model, kind, feats, rule_category,
            epsilon=SIM_RL_EPSILON, min_samples=SIM_RL_MIN_SAMPLES, rng=_RL_RNG,
        )
        _RL_USAGE_COUNTS[meta["source"]] = _RL_USAGE_COUNTS.get(meta["source"], 0) + 1
        return category_final

    _RL_USAGE_COUNTS["rule_fallback"] += 1
    return rule_category



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

    # ファンダメンタルズを並列で先読みしておく(逐次取得だとここが最も時間を食う)
    prefetch_fundamentals([r["symbol"] for _, r in shortlist.iterrows()])
    save_fundamentals_cache()   # 途中で落ちても次回に取得結果を再利用できるよう保存

    candidates = []
    for _, row in shortlist.iterrows():
        sym = row["symbol"]
        fund = fetch_fundamentals(sym)  # 先読み済みなのでキャッシュから即返る
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


def render_row_holding(r):
    pl = r.get("unrealized_pl")
    pl_color = "#188038" if (pl or 0) >= 0 else "#c5221f"
    pl_rate = r.get("unrealized_pl_rate")
    return f"""
    <tr>
      <td><b>{r['symbol']}</b></td>
      <td>{fmt_num(r.get('qty'), 2)}</td>
      <td>${fmt_num(r.get('unit_cost'))}</td>
      <td>${fmt_num(r.get('price') or r.get('last_price'))}</td>
      <td>${fmt_num(r.get('market_value'))}</td>
      <td style="color:{pl_color}"><b>${fmt_num(pl)} ({fmt_pct(pl_rate)})</b></td>
      <td><b>{r.get('long_prediction') or '—'}</b></td>
      <td>{r.get('next_earnings') or '不明'}</td>
      <td style="font-size:12px;">{r.get('long_situation','—')}</td>
      <td style="font-size:12px;">{r.get('long_outlook','—')}</td>
      <td style="font-size:12px;">{r.get('long_timing','—')}</td>
    </tr>"""


def render_email_html(day_list, long_list, major_list, universe_size, scanned_size, holdings_list=None):
    holdings_list = holdings_list or []
    now = datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M JST")
    day_rows = "".join(render_row_day(r) for r in day_list)
    long_rows = "".join(render_row_long(r) for r in long_list)
    major_rows = "".join(render_row_major(r) for r in major_list)
    holding_rows = "".join(render_row_holding(r) for r in holdings_list)

    if holdings_list:
        total_mv = sum(r.get("market_value") or 0 for r in holdings_list)
        total_pl = sum(r.get("unrealized_pl") or 0 for r in holdings_list)
        total_cost = sum(r.get("total_cost") or 0 for r in holdings_list)
        total_pl_rate = (total_pl / total_cost * 100) if total_cost else None
        pl_color = "#188038" if total_pl >= 0 else "#c5221f"
        holdings_section = f"""
    <h3 style="margin-top:24px;">保有株の状況</h3>
    <p style="font-size:12px;color:#666;">
      評価額合計: ${fmt_num(total_mv)} / 含み損益合計:
      <span style="color:{pl_color};"><b>${fmt_num(total_pl)} ({fmt_pct(total_pl_rate)})</b></span>
      (Webull口座のポジション情報に基づく。取得できたテクニカル指標がある銘柄のみ「予想」欄が表示されます)
    </p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px;">
      <tr style="background:#222;color:#fff;">
        <th>銘柄</th><th>保有数量</th><th>平均取得単価</th><th>現在値</th><th>評価額</th>
        <th>含み損益</th><th>予想(1〜3ヶ月)</th><th>次回決算</th>
        <th>現状</th><th>値動きの見立て</th><th>投資タイミングの目安</th>
      </tr>
      {holding_rows}
    </table>
    """
    else:
        holdings_section = """
    <h3 style="margin-top:24px;">保有株の状況</h3>
    <p style="font-size:12px;color:#666;">
      保有株情報を取得できませんでした(Webull口座が未接続、Trading API権限が
      無効、または保有株が無い可能性があります)。
    </p>
    """

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

    {holdings_section}

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
        # inf/-inf も JSON では Infinity という非標準トークンになるため落とす
        return v if math.isfinite(v) else None
    # ネストしたdict/listも再帰的に処理する。
    # (想定推移線 projection のような入れ子データにNaNが残ると、
    #  ブラウザの JSON.parse が "Unexpected token 'N'" で失敗する)
    if isinstance(v, dict):
        return {k: _json_safe(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_json_safe(x) for x in v]
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
    # allow_nan=False にして、NaN/Infinity が混ざったまま書き出されるのを防ぐ。
    # Pythonの既定では NaN がそのまま出力されるが、これは不正なJSONで
    # ブラウザ側の JSON.parse が失敗する。失敗した場合は再帰的に除去して書き直す。
    try:
        payload = json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False)
    except ValueError:
        payload = json.dumps(_json_safe(obj), ensure_ascii=False, indent=2, allow_nan=False)
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(payload)
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

SIMULATION_PATH = os.path.join(DATA_DIR, "simulation.json")
MAX_SIM_TRADES_KEPT = 3000


def _watched_symbols(day_list, long_list, major_list, holdings_list=None) -> set[str]:
    return {
        r.get("symbol")
        for r in (day_list + long_list + major_list + (holdings_list or []))
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


def update_intraday_prices(day_list, long_list, major_list, run_dt: datetime, holdings_list=None) -> None:
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
        for r in (day_list + long_list + major_list + (holdings_list or [])):
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


def append_predictions_log(day_list, long_list, major_list, run_dt: datetime, holdings_list=None) -> None:
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
                "pattern": prediction_pattern_key(r, kind),
                "features": rl.featurize(r) if RL_AVAILABLE else None,
            })

        for r in (day_list + major_list):
            _maybe_add(r, "day", "day_prediction")
        for r in (long_list + major_list):
            _maybe_add(r, "long", "long_prediction")
        for r in (holdings_list or []):
            _maybe_add(r, "day", "day_prediction")
            _maybe_add(r, "long", "long_prediction")

        log["entries"].extend(_clean_records(new_entries))
        if MAX_PREDICTIONS_LOG_KEPT:
            log["entries"] = log["entries"][-MAX_PREDICTIONS_LOG_KEPT:]
        _atomic_write_json(PREDICTIONS_LOG_PATH, log)
        print(f"[info] predictions_log 追記: {len(new_entries)}件")
    except Exception as e:
        print(f"[warn] predictions_log 更新に失敗しました: {e}")
        traceback.print_exc()


# --------------------------------------------------------------------------
# 売買シミュレーション(予想に従って仮想的に売買するペーパートレード)
# --------------------------------------------------------------------------
#
# ルール:
#   - 資金: 初期資金 SIM_INITIAL_CASH(既定$100、リセット時に変更可能)から
#     スタートする「現金口座」を持つ仮想シミュレーション。現金が尽きたら
#     (=稼いで現金を増やさない限り)それ以上は新規に買えない。
#   - 買い: 予想が強気(値上がり系)の銘柄を、1回の注文につき銘柄ごと最大$10
#     (ただし残り現金がそれ未満の場合は残り現金の範囲内)購入する。同じ銘柄を
#     すでに保有していても、強気予想が出るたびに追加で買い増す(現金が続く限り)。
#     残り現金が SIM_MIN_CASH_TO_TRADE 未満になったら新規購入は行わない。
#   - 売り: 予想が弱気(値下がり系)に転じても即座には売らず、
#     SIM_BEARISH_STREAK_TO_SELL 回連続で弱気予想が出るまでは「保持」を選べる
#     (=単発の弱気予想でうろたえて手放さない)。ただし含み損が
#     SIM_STOP_LOSS_PCT を超えて悪化した場合は、連続回数に関係なく
#     損切りとして全量売却する(セーフティネット)。
#   - 端株(単元未満株、$10で1株未満しか買えない/保有数量が1株未満)の売買のみ、
#     米国市場の通常取引時間中に限る。$10で1株以上買える、または1株以上保有して
#     いる場合は時間外でも売買する。
#   - リセット: 環境変数 SIM_RESET=1 を付けて実行すると、保有・取引履歴・
#     現金残高をすべて破棄し、SIM_INITIAL_CASH(既定$100、省略時は前回値か$100)
#     で仮想口座を作り直す。的中率の学習データ(learning_weights.json)は
#     SIM_RESET_LEARNING=1 を別途指定しない限り引き継がれる。
#   - 実際の資金は動かさない、あくまで仮想的なシミュレーション。

SIM_BULLISH = {"大きく値上がり", "少し値上がり"}
SIM_BEARISH = {"大きく値下がり", "少し値下がり"}
SIM_BUY_USD_PER_ORDER = 10.0

# 初期資金・現金運用まわりの設定(いずれも環境変数で上書き可能)
SIM_INITIAL_CASH_DEFAULT = float(os.environ.get("SIM_INITIAL_CASH", "100") or 100)
SIM_MIN_CASH_TO_TRADE = 1.0  # 残り現金がこれ未満なら新規購入しない
SIM_RESET_REQUESTED = os.environ.get("SIM_RESET", "").lower() in ("1", "true", "yes")
SIM_RESET_LEARNING_REQUESTED = os.environ.get("SIM_RESET_LEARNING", "").lower() in ("1", "true", "yes")

# 「弱気予想でも保持を選べる」ためのしきい値
SIM_BEARISH_STREAK_TO_SELL = int(os.environ.get("SIM_BEARISH_STREAK_TO_SELL", "2") or 2)
SIM_STOP_LOSS_PCT = float(os.environ.get("SIM_STOP_LOSS_PCT", "-20") or -20)  # 含み損率(%)。これを下回ったら強制損切り

# 日次の資産推移を残しておく上限(日数分。1日1エントリに集約するのでこれで十分長期間保持できる)
MAX_EQUITY_POINTS_KEPT = 400

# 予想の的中/不的中パターンを学習し、次回以降の予想に反映するための設定
LEARNING_PATH = os.path.join(DATA_DIR, "learning_weights.json")
SIM_LEARN_MIN_SAMPLES = int(os.environ.get("SIM_LEARN_MIN_SAMPLES", "8") or 8)   # このサンプル数未満のパターンは学習反映しない
SIM_LEARN_DEMOTE_RATE = float(os.environ.get("SIM_LEARN_DEMOTE_RATE", "35") or 35)  # 的中率がこれ未満→予想を弱める(負けパターン学習)
SIM_LEARN_PROMOTE_RATE = float(os.environ.get("SIM_LEARN_PROMOTE_RATE", "65") or 65)  # 的中率がこれ超→予想を強める(勝ちパターン学習)

# --------------------------------------------------------------------------
# 強化学習(文脈的バンディット)まわりの設定
# --------------------------------------------------------------------------
RL_MODEL_PATH = os.path.join(DATA_DIR, "rl_model.pkl")
RL_STATUS_PATH = os.path.join(DATA_DIR, "rl_status.json")
SIM_RL_EPSILON = float(os.environ.get("SIM_RL_EPSILON", "0.15") or 0.15)  # 探索確率(0〜1)
SIM_RL_MIN_SAMPLES = int(os.environ.get("SIM_RL_MIN_SAMPLES", "20") or 20)  # 行動ごとにこの件数未満は未学習扱い
SIM_RESET_RL_REQUESTED = os.environ.get("SIM_RESET_RL", "").lower() in ("1", "true", "yes")
_RL_RNG = random.Random(int(os.environ.get("SIM_RL_SEED", "0") or 0) or None)
_RL_MODEL_CACHE = None  # プロセス内で1回だけロードしてキャッシュ


def _get_rl_model():
    """学習済みの強化学習モデル(RewardModel)をロードする(プロセス内キャッシュ)。"""
    global _RL_MODEL_CACHE
    if not RL_AVAILABLE:
        return None
    if _RL_MODEL_CACHE is not None:
        return _RL_MODEL_CACHE
    _RL_MODEL_CACHE = rl.load_model(RL_MODEL_PATH)
    return _RL_MODEL_CACHE


def is_us_regular_market_hours(run_dt: datetime) -> bool:
    """米国市場の通常取引時間(9:30-16:00 America/New_York, 平日)内かどうか。
    祝日は考慮しない(簡易判定)。"""
    try:
        from zoneinfo import ZoneInfo
        et = run_dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        return False
    if et.weekday() >= 5:
        return False
    open_t = et.replace(hour=9, minute=30, second=0, microsecond=0)
    close_t = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_t <= et <= close_t


def _sim_signal(r: dict) -> tuple[str | None, str | None]:
    """行(銘柄)の予想から売買シグナルを判定する。戻り値: (action, kind)
    action は 'buy' / 'sell' / None、kind は 'day' / 'long'。
    day予想を優先し、なければlong予想・recommendationを見る。"""
    day_pred = r.get("day_prediction")
    long_pred = r.get("long_prediction")
    rec = r.get("recommendation")

    if day_pred in SIM_BULLISH:
        return "buy", "day"
    if long_pred in SIM_BULLISH:
        return "buy", "long"
    if rec == "buy":
        return "buy", "day" if day_pred else "long"

    if day_pred in SIM_BEARISH:
        return "sell", "day"
    if long_pred in SIM_BEARISH:
        return "sell", "long"
    if rec == "sell":
        return "sell", "day" if day_pred else "long"

    return None, None


def _sim_fresh_state(initial_cash: float) -> dict:
    """初期資金 initial_cash で仮想口座を作り直した状態(リセット後の状態)。"""
    return {
        "positions": {},
        "trades": [],
        "cash": round(initial_cash, 4),
        "initial_cash": round(initial_cash, 4),
        "equity_history": [],
    }


def _sim_load() -> dict:
    """
    内部の保有・取引状態(銘柄ごとの数量/簿価/連続弱気回数、現金残高、
    初期資金、日次資産推移)を読み込む。公開用JSON(simulation.json)は
    表示用に整形済みのため、内部状態は _positions_raw / _trades_raw /
    _cash_raw / _equity_raw フィールドから復元する。
    環境変数 SIM_RESET=1 が指定されている場合は、既存の状態を無視して
    (SIM_INITIAL_CASHで指定、省略時は既定$100の)まっさらな口座を返す。
    """
    if SIM_RESET_REQUESTED:
        # リセット時の初期資金: SIM_INITIAL_CASHの指定があればそれを優先、
        # なければ前回の初期資金(あれば)、それもなければ既定$100。
        prev_initial = None
        try:
            if os.path.exists(SIMULATION_PATH) and os.path.getsize(SIMULATION_PATH) > 0:
                with open(SIMULATION_PATH, "r", encoding="utf-8") as f:
                    prev_raw = json.load(f)
                prev_initial = prev_raw.get("_cash_raw", {}).get("initial_cash")
        except Exception:
            pass
        initial_cash = SIM_INITIAL_CASH_DEFAULT
        if "SIM_INITIAL_CASH" not in os.environ and prev_initial:
            initial_cash = prev_initial
        print(f"[info] simulation: SIM_RESET指定によりリセットします(初期資金 ${initial_cash:.2f})")
        return _sim_fresh_state(initial_cash)

    try:
        if os.path.exists(SIMULATION_PATH) and os.path.getsize(SIMULATION_PATH) > 0:
            with open(SIMULATION_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f)
            cash_raw = raw.get("_cash_raw") or {}
            state = {
                "positions": raw.get("_positions_raw") or {},
                "trades": raw.get("_trades_raw") or [],
                "cash": cash_raw.get("cash"),
                "initial_cash": cash_raw.get("initial_cash"),
                "equity_history": raw.get("_equity_raw") or [],
            }
            if state["cash"] is None or state["initial_cash"] is None:
                # 旧バージョンのsimulation.json(現金管理が無い状態)からの移行。
                # 既存の投資額を踏まえて、初期資金を使い切っていない体で現金を復元する。
                initial_cash = SIM_INITIAL_CASH_DEFAULT
                spent = sum(p.get("cost", 0.0) for p in state["positions"].values())
                state["initial_cash"] = initial_cash
                state["cash"] = max(0.0, initial_cash - spent)
            return state
    except Exception as e:
        print(f"[warn] simulation.json 読み込み失敗(新規作成します): {e}")
    return _sim_fresh_state(SIM_INITIAL_CASH_DEFAULT)


def _load_history_file(sym: str) -> list[dict]:
    path = os.path.join(HISTORY_DIR, f"{sym}.json")
    try:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []


def _judge_entry_hit(entry: dict, hist: list[dict], day_offset: int | None, today_str: str) -> bool | None:
    """
    予想エントリの的中判定。day_offset を指定すると「予想日から day_offset 日後
    までの範囲」で判定し(1/5/30日ごとの的中率用)、None なら horizon_end までの
    全期間で判定する(全期間の的中率用)。判定に必要なデータがまだ揃っていない
    場合は None を返す。
    """
    p0 = entry.get("price_at_prediction")
    made = entry.get("made_date")
    if p0 is None or not hist or not made:
        return None
    if day_offset is not None:
        try:
            made_dt = datetime.strptime(made, "%Y-%m-%d")
        except Exception:
            return None
        end_str = (made_dt + timedelta(days=day_offset)).strftime("%Y-%m-%d")
    else:
        end_str = entry.get("horizon_end")
    if not end_str or end_str > today_str:
        return None  # まだ判定期間が終わっていない

    pts = [h for h in hist if h.get("date") and made <= h["date"] <= end_str and h.get("close") is not None]
    if not pts:
        return None

    category = entry.get("category") or ""
    closes = [h["close"] for h in pts]
    if "値上がり" in category:
        return (max(closes) - p0) / p0 * 100 > 0
    if "値下がり" in category:
        return (min(closes) - p0) / p0 * 100 < 0
    return max(abs((c - p0) / p0 * 100) for c in closes) < 3


def _actual_outcome_category(entry: dict, hist: list[dict], today_str: str) -> str | None:
    """予想エントリについて、予想カテゴリに関係なく「実際にはどれくらい動いたか」
    を5段階(大きく値上がり/少し値上がり/変動なし/少し値下がり/大きく値下がり)で
    分類する。学習で「変動なし」と予想して逃した上昇/下落パターンを検出するために
    使う。判定期間がまだ終わっていない・データがない場合は None。"""
    p0 = entry.get("price_at_prediction")
    made = entry.get("made_date")
    end_str = entry.get("horizon_end")
    kind = entry.get("kind")
    if p0 is None or not hist or not made or not end_str or end_str > today_str:
        return None
    pts = [h for h in hist if h.get("date") and made <= h["date"] <= end_str and h.get("close") is not None]
    if not pts:
        return None
    pts_sorted = sorted(pts, key=lambda h: h["date"])
    p_end = pts_sorted[-1]["close"]
    if not p0:
        return None
    chg_pct = (p_end - p0) / p0 * 100
    big = 6.0 if kind == "day" else 15.0
    small = 1.5 if kind == "day" else 4.0
    if chg_pct >= big:
        return "大きく値上がり"
    if chg_pct >= small:
        return "少し値上がり"
    if chg_pct <= -big:
        return "大きく値下がり"
    if chg_pct <= -small:
        return "少し値下がり"
    return "変動なし"


def compute_prediction_accuracy(run_dt: datetime) -> dict:
    """予想ログ(predictions_log.json)の的中率を、全期間・1日後・5日後・30日後の
    それぞれの時間軸で集計する。シミュレーションタブの「予想的中率」表示に使う。"""
    out = {k: {"hits": 0, "total": 0, "rate": None} for k in ("all", "d1", "d5", "d30")}
    try:
        if not (os.path.exists(PREDICTIONS_LOG_PATH) and os.path.getsize(PREDICTIONS_LOG_PATH) > 0):
            return out
        with open(PREDICTIONS_LOG_PATH, "r", encoding="utf-8") as f:
            log = json.load(f)
        entries = log.get("entries", [])
        today_str = run_dt.strftime("%Y-%m-%d")
        hist_cache: dict[str, list[dict]] = {}

        def get_hist(sym):
            if sym not in hist_cache:
                hist_cache[sym] = _load_history_file(sym)
            return hist_cache[sym]

        windows = {"all": None, "d1": 1, "d5": 5, "d30": 30}
        for key, offset in windows.items():
            hits = total = 0
            for e in entries:
                sym = e.get("symbol")
                if not sym:
                    continue
                res = _judge_entry_hit(e, get_hist(sym), offset, today_str)
                if res is None:
                    continue
                total += 1
                hits += 1 if res else 0
            out[key] = {"hits": hits, "total": total, "rate": round(hits / total * 100, 1) if total else None}
    except Exception as e:
        print(f"[warn] 予想的中率の集計に失敗しました: {e}")
        traceback.print_exc()
    return out


def update_learning_state(run_dt: datetime) -> dict:
    """予想ログ(predictions_log.json)を「型(パターン)」ごとに集計し直し、
    パターンごとの的中率(勝ちパターン/負けパターン)を data/learning_weights.json
    に保存する。次回以降の predict_category はこの結果を読み込んで、
    的中率の低いパターンの予想は弱め、的中率の高いパターンの予想は強める
    (=学習して予想の精度を上げていく)。失敗してもスキャン本体は止めない。"""
    global _LEARNING_CACHE
    result = {"patterns": {}, "updated_at": run_dt.strftime("%Y-%m-%d %H:%M:%S JST")}
    try:
        if SIM_RESET_LEARNING_REQUESTED:
            print("[info] learning_weights: SIM_RESET_LEARNING指定によりリセットします")
            os.makedirs(DATA_DIR, exist_ok=True)
            _atomic_write_json(LEARNING_PATH, result)
            _LEARNING_CACHE = result
            return result

        if not (os.path.exists(PREDICTIONS_LOG_PATH) and os.path.getsize(PREDICTIONS_LOG_PATH) > 0):
            return result
        with open(PREDICTIONS_LOG_PATH, "r", encoding="utf-8") as f:
            log = json.load(f)
        entries = log.get("entries", [])
        today_str = run_dt.strftime("%Y-%m-%d")
        hist_cache: dict[str, list[dict]] = {}

        def get_hist(sym):
            if sym not in hist_cache:
                hist_cache[sym] = _load_history_file(sym)
            return hist_cache[sym]

        outcome_keys = ["大きく値上がり", "少し値上がり", "変動なし", "少し値下がり", "大きく値下がり"]
        buckets: dict[str, dict] = {}
        for e in entries:
            pattern = e.get("pattern")
            sym = e.get("symbol")
            if not pattern or not sym:
                continue
            hist = get_hist(sym)
            res = _judge_entry_hit(e, hist, None, today_str)
            actual = _actual_outcome_category(e, hist, today_str)
            if res is None and actual is None:
                continue
            b = buckets.setdefault(pattern, {
                "hits": 0, "total": 0,
                "outcome": {k: 0 for k in outcome_keys},
                "neutral_total": 0, "neutral_miss_up": 0, "neutral_miss_down": 0,
            })
            if res is not None:
                b["total"] += 1
                b["hits"] += 1 if res else 0
            if actual is not None:
                b["outcome"][actual] = b["outcome"].get(actual, 0) + 1
                # 「変動なし」と予想していたのに、実際は上昇/下落していた
                # ケースだけを別集計する(=見送りで逃した上昇/下落パターン)
                if e.get("category") == "変動なし":
                    b["neutral_total"] += 1
                    if "値上がり" in actual:
                        b["neutral_miss_up"] += 1
                    elif "値下がり" in actual:
                        b["neutral_miss_down"] += 1

        patterns_out = {}
        for pattern, b in buckets.items():
            rate = round(b["hits"] / b["total"] * 100, 1) if b["total"] else None
            neutral_total = b["neutral_total"]
            neutral_miss_up_rate = round(b["neutral_miss_up"] / neutral_total * 100, 1) if neutral_total else None
            neutral_miss_down_rate = round(b["neutral_miss_down"] / neutral_total * 100, 1) if neutral_total else None
            patterns_out[pattern] = {
                "hits": b["hits"], "total": b["total"], "rate": rate,
                "outcome": b["outcome"],
                "neutral_total": neutral_total,
                "neutral_miss_up": b["neutral_miss_up"],
                "neutral_miss_down": b["neutral_miss_down"],
                "neutral_miss_up_rate": neutral_miss_up_rate,
                "neutral_miss_down_rate": neutral_miss_down_rate,
            }

        result["patterns"] = patterns_out
        os.makedirs(DATA_DIR, exist_ok=True)
        _atomic_write_json(LEARNING_PATH, result)
        _LEARNING_CACHE = result

        losing = sum(1 for p in patterns_out.values() if p["total"] >= SIM_LEARN_MIN_SAMPLES and (p["rate"] or 0) < SIM_LEARN_DEMOTE_RATE)
        winning = sum(1 for p in patterns_out.values() if p["total"] >= SIM_LEARN_MIN_SAMPLES and (p["rate"] or 0) > SIM_LEARN_PROMOTE_RATE)
        missed_up = sum(1 for p in patterns_out.values() if p["neutral_total"] >= SIM_LEARN_MIN_SAMPLES and (p["neutral_miss_up_rate"] or 0) > SIM_LEARN_PROMOTE_RATE)
        missed_down = sum(1 for p in patterns_out.values() if p["neutral_total"] >= SIM_LEARN_MIN_SAMPLES and (p["neutral_miss_down_rate"] or 0) > SIM_LEARN_PROMOTE_RATE)
        print(f"[info] learning_weights更新: {len(patterns_out)}パターン "
              f"(負けパターン{losing}件を弱め / 勝ちパターン{winning}件を強め / "
              f"見送りで逃した上昇パターン{missed_up}件・下落パターン{missed_down}件を方向予想に昇格、次回予想に反映)")
    except Exception as e:
        print(f"[warn] learning_weights更新に失敗しました: {e}")
        traceback.print_exc()
    return result


def update_ml_learning_state(run_dt: datetime) -> None:
    """強化学習(文脈的バンディット)モデルを再学習して data/rl_model.pkl に保存する。
    predictions_log.json(特徴量つきの予想ログ)と simulation.json(実際の売買履歴)
    から (特徴量, 行動=予想カテゴリ, 報酬=$損益) の学習データを作り、行動ごとに
    RandomForestRegressorで期待報酬を回帰する。scikit-learnが無い環境や
    データがまだ少ない場合は失敗してもスキャン本体は止めない。"""
    global _RL_MODEL_CACHE
    if not RL_AVAILABLE:
        return
    try:
        os.makedirs(DATA_DIR, exist_ok=True)

        if SIM_RESET_RL_REQUESTED:
            print("[info] rl_model: SIM_RESET_RL指定によりリセットします")
            if os.path.exists(RL_MODEL_PATH):
                os.remove(RL_MODEL_PATH)
            _RL_MODEL_CACHE = None
            _atomic_write_json(RL_STATUS_PATH, {"updated_at": run_dt.strftime("%Y-%m-%d %H:%M:%S JST"),
                                                 "trained": False, "coverage": {}})
            return

        if not (os.path.exists(PREDICTIONS_LOG_PATH) and os.path.getsize(PREDICTIONS_LOG_PATH) > 0):
            return
        with open(PREDICTIONS_LOG_PATH, "r", encoding="utf-8") as f:
            predictions_log = json.load(f)

        sim_trades = []
        if os.path.exists(SIMULATION_PATH) and os.path.getsize(SIMULATION_PATH) > 0:
            with open(SIMULATION_PATH, "r", encoding="utf-8") as f:
                sim_raw = json.load(f)
            sim_trades = sim_raw.get("_trades_raw") or []

        today_str = run_dt.strftime("%Y-%m-%d")
        hist_cache: dict[str, list[dict]] = {}

        def get_hist(sym):
            if sym not in hist_cache:
                hist_cache[sym] = _load_history_file(sym)
            return hist_cache[sym]

        rows = rl.build_training_rows(predictions_log, sim_trades, get_hist, today_str, SIM_BUY_USD_PER_ORDER)
        model = rl.RewardModel()
        model.fit(rows, min_samples=SIM_RL_MIN_SAMPLES)
        rl.save_model(model, RL_MODEL_PATH)
        _RL_MODEL_CACHE = model

        coverage = {k: model.coverage(k) for k in ("day", "long")}
        trained_actions = sum(1 for k in coverage for c, n in coverage[k].items() if n >= SIM_RL_MIN_SAMPLES)
        _atomic_write_json(RL_STATUS_PATH, {
            "updated_at": model.trained_at,
            "trained": True,
            "training_rows": len(rows),
            "min_samples": SIM_RL_MIN_SAMPLES,
            "epsilon": SIM_RL_EPSILON,
            "coverage": coverage,
            "usage_this_run": dict(_RL_USAGE_COUNTS),
        })
        print(f"[info] rl_model更新: 学習サンプル{len(rows)}件 / "
              f"学習済み行動{trained_actions}/10(day5+long5) / "
              f"今回の予想内訳 model={_RL_USAGE_COUNTS.get('model',0)} "
              f"explore={_RL_USAGE_COUNTS.get('explore',0)} "
              f"rule_fallback={_RL_USAGE_COUNTS.get('rule_fallback',0)}")
    except Exception as e:
        print(f"[warn] rl_model更新に失敗しました: {e}")
        traceback.print_exc()


def _equity_ref_at_or_before(equity_history: list[dict], target_date_str: str) -> dict | None:
    candidates = [e for e in equity_history if e.get("date") and e["date"] <= target_date_str]
    return candidates[-1] if candidates else None


def _period_pl(equity_history: list[dict], run_dt: datetime, total_equity: float, days: int) -> dict:
    target_date = (run_dt - timedelta(days=days)).strftime("%Y-%m-%d")
    ref = _equity_ref_at_or_before(equity_history, target_date)
    if not ref or not ref.get("equity"):
        return {"pl": None, "pl_pct": None, "ref_date": None}
    ref_eq = ref["equity"]
    pl = total_equity - ref_eq
    pl_pct = (pl / ref_eq * 100) if ref_eq else None
    return {"pl": round(pl, 4), "pl_pct": round(pl_pct, 2) if pl_pct is not None else None, "ref_date": ref.get("date")}


def run_simulation(day_list, long_list, major_list, holdings_list, run_dt: datetime) -> None:
    """
    予想に従って仮想的に売買するシミュレーションを1ステップ進め、
    data/simulation.json (現金残高・保有ポジション・取引履歴・収支・期間損益・
    的中率) を更新する。失敗してもスキャン本体・メール送信は止めない。

    - 現金管理: 初期資金(既定$100)から始まる現金残高を持ち、残り現金の範囲
      でしか新規購入しない(稼いで現金を増やさない限りそれ以上は買えない)。
    - 弱気予想への対応: 単発の弱気予想では売らず、SIM_BEARISH_STREAK_TO_SELL
      回連続で弱気予想が出るまで「保持」を選べる。ただし含み損が
      SIM_STOP_LOSS_PCT を超えたら連続回数に関係なく損切りする。
    """
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        state = _sim_load()
        positions: dict = state.get("positions", {}) or {}
        trades: list = state.get("trades", []) or []
        cash: float = float(state.get("cash", SIM_INITIAL_CASH_DEFAULT) or 0.0)
        initial_cash: float = float(state.get("initial_cash", SIM_INITIAL_CASH_DEFAULT) or SIM_INITIAL_CASH_DEFAULT)
        equity_history: list = state.get("equity_history", []) or []

        regular_hours = is_us_regular_market_hours(run_dt)
        ts = run_dt.strftime("%Y-%m-%d %H:%M:%S JST")

        # 銘柄ごとに最新の行(価格・予想)を1件にまとめる(保有株の行を優先)
        by_symbol: dict[str, dict] = {}
        for r in (day_list or []) + (long_list or []) + (major_list or []):
            sym = r.get("symbol")
            if sym:
                by_symbol.setdefault(sym, r)
        for r in (holdings_list or []):
            sym = r.get("symbol")
            if sym:
                by_symbol[sym] = r

        if not regular_hours:
            print("[info] simulation: 通常取引時間外です(1株未満になる端株取引のみスキップします)")

        held_no_cash_skips = 0
        for sym, r in by_symbol.items():
            price = r.get("price")
            if not price or price <= 0:
                continue
            action, kind = _sim_signal(r)
            pos = positions.get(sym, {"qty": 0.0, "cost": 0.0, "bear_streak": 0})

            if action is None:
                # 中立予想: 弱気連続カウントはリセット(連続弱気のときだけカウントする)
                if pos.get("qty", 0) > 1e-9:
                    pos["bear_streak"] = 0
                    positions[sym] = pos
                continue

            if action == "buy":
                if pos.get("qty", 0) > 1e-9:
                    pos["bear_streak"] = 0  # 強気シグナルが戻ったので弱気カウントをリセット
                if cash < SIM_MIN_CASH_TO_TRADE:
                    held_no_cash_skips += 1
                    positions[sym] = pos
                    continue  # 現金不足: 稼いで現金を増やさない限り新規購入しない
                order_amount = min(SIM_BUY_USD_PER_ORDER, cash)
                qty = order_amount / price
                is_fractional = qty < 1.0  # 端株になる場合のみ通常取引時間の制限対象
                if is_fractional and not regular_hours:
                    positions[sym] = pos
                    continue  # 端株の新規売買は通常取引時間中のみ
                pos["qty"] = pos.get("qty", 0.0) + qty
                pos["cost"] = pos.get("cost", 0.0) + order_amount
                pos["bear_streak"] = 0
                cash -= order_amount
                positions[sym] = pos
                trades.append({
                    "time": ts, "symbol": sym, "side": "buy", "kind": kind,
                    "price": price, "qty": qty, "amount": order_amount,
                    "cash_after": round(cash, 4),
                    "prediction": r.get("day_prediction") if kind == "day" else r.get("long_prediction"),
                })

            elif action == "sell" and pos.get("qty", 0) > 1e-9:
                qty_held = pos["qty"]
                cost = pos.get("cost", 0.0)
                mv_now = qty_held * price
                pl_pct_now = ((mv_now - cost) / cost * 100) if cost else 0.0
                pos["bear_streak"] = pos.get("bear_streak", 0) + 1
                stop_loss_hit = pl_pct_now <= SIM_STOP_LOSS_PCT
                streak_hit = pos["bear_streak"] >= SIM_BEARISH_STREAK_TO_SELL

                if not (stop_loss_hit or streak_hit):
                    # 弱気予想が出たが、連続回数がまだ閾値未満かつ含み損も限度内
                    # →「保持」を選択して売らない(単発の弱気予想でうろたえない)
                    positions[sym] = pos
                    continue

                is_fractional = qty_held < 1.0  # 保有数量が1株未満(端株)の場合のみ制限対象
                if is_fractional and not regular_hours:
                    positions[sym] = pos
                    continue  # 端株の売却は通常取引時間中のみ(1株以上ならいつでも売却可)
                proceeds = qty_held * price
                trades.append({
                    "time": ts, "symbol": sym, "side": "sell", "kind": kind,
                    "price": price, "qty": qty_held, "amount": proceeds,
                    "realized_pl": proceeds - cost,
                    "cash_after": round(cash + proceeds, 4),
                    "sell_reason": "stop_loss" if stop_loss_hit else "bearish_streak",
                    "prediction": r.get("day_prediction") if kind == "day" else r.get("long_prediction"),
                })
                cash += proceeds
                positions[sym] = {"qty": 0.0, "cost": 0.0, "bear_streak": 0}

        # 数量0のポジションは掃除する
        positions = {s: p for s, p in positions.items() if (p.get("qty") or 0) > 1e-9}
        if MAX_SIM_TRADES_KEPT:
            trades = trades[-MAX_SIM_TRADES_KEPT:]

        # --- 現在の評価額・収支サマリー ---
        total_cost = 0.0
        total_mv = 0.0
        position_rows = []
        for sym, pos in positions.items():
            r = by_symbol.get(sym)
            price = r.get("price") if r else None
            qty = pos.get("qty", 0.0)
            cost = pos.get("cost", 0.0)
            mv = qty * price if price else None
            total_cost += cost
            if mv is not None:
                total_mv += mv
            position_rows.append({
                "symbol": sym, "qty": qty, "avg_cost": (cost / qty) if qty else None,
                "cost": cost, "price": price, "market_value": mv,
                "pl": (mv - cost) if mv is not None else None,
                "pl_pct": ((mv - cost) / cost * 100) if (mv is not None and cost) else None,
                "bear_streak": pos.get("bear_streak", 0),
                "sell_streak_threshold": SIM_BEARISH_STREAK_TO_SELL,
            })
        position_rows.sort(key=lambda x: -(x.get("market_value") or 0))

        total_bought = sum(t.get("amount", 0) for t in trades if t.get("side") == "buy")
        realized_pl_total = sum(t.get("realized_pl", 0) or 0 for t in trades if t.get("side") == "sell")
        unrealized_pl_total = total_mv - total_cost
        total_pl = realized_pl_total + unrealized_pl_total
        total_pl_pct = (total_pl / total_bought * 100) if total_bought else None

        total_equity = cash + total_mv
        equity_pl = total_equity - initial_cash
        equity_pl_pct = (equity_pl / initial_cash * 100) if initial_cash else None

        # --- 日次の資産推移を更新(同じ日は最新値で上書き)し、期間損益を算出 ---
        today_str = run_dt.strftime("%Y-%m-%d")
        equity_history = [e for e in equity_history if e.get("date") != today_str]
        equity_history.append({"date": today_str, "equity": round(total_equity, 4), "cash": round(cash, 4)})
        equity_history.sort(key=lambda e: e["date"])
        if MAX_EQUITY_POINTS_KEPT:
            equity_history = equity_history[-MAX_EQUITY_POINTS_KEPT:]

        period_pl = {
            "d1": _period_pl(equity_history, run_dt, total_equity, 1),
            "d7": _period_pl(equity_history, run_dt, total_equity, 7),
            "d30": _period_pl(equity_history, run_dt, total_equity, 30),
        }

        accuracy = compute_prediction_accuracy(run_dt)

        out = {
            "updated_at": ts,
            "regular_hours_last_run": regular_hours,
            "positions": position_rows,
            "trades": list(reversed(trades[-300:])),  # 新しい取引が先頭
            "summary": {
                "initial_cash": round(initial_cash, 4),
                "cash": round(cash, 4),
                "total_bought": round(total_bought, 4),
                "total_cost_basis": round(total_cost, 4),
                "total_market_value": round(total_mv, 4),
                "total_equity": round(total_equity, 4),
                "realized_pl": round(realized_pl_total, 4),
                "unrealized_pl": round(unrealized_pl_total, 4),
                "total_pl": round(total_pl, 4),
                "total_pl_pct": round(total_pl_pct, 2) if total_pl_pct is not None else None,
                "equity_pl": round(equity_pl, 4),
                "equity_pl_pct": round(equity_pl_pct, 2) if equity_pl_pct is not None else None,
            },
            "period_pl": period_pl,
            "policy": {
                "bearish_streak_to_sell": SIM_BEARISH_STREAK_TO_SELL,
                "stop_loss_pct": SIM_STOP_LOSS_PCT,
                "buy_per_order_usd": SIM_BUY_USD_PER_ORDER,
                "min_cash_to_trade": SIM_MIN_CASH_TO_TRADE,
            },
            "accuracy": accuracy,
        }
        # positions/trades/現金/資産推移はフルセットを別フィールドで保存(タブ側の再計算・追跡用)
        out["_positions_raw"] = positions
        out["_trades_raw"] = trades
        out["_cash_raw"] = {"cash": round(cash, 4), "initial_cash": round(initial_cash, 4)}
        out["_equity_raw"] = equity_history

        _atomic_write_json(SIMULATION_PATH, out)
        print(f"[info] simulation更新: 保有{len(position_rows)}銘柄 / 現金${cash:.2f} / "
              f"総資産${total_equity:.2f} / 総損益 ${total_pl:.2f}"
              + (f" ({equity_pl_pct:.1f}%)" if equity_pl_pct is not None else "")
              + (f" / 資金不足で見送り{held_no_cash_skips}件" if held_no_cash_skips else ""))
    except Exception as e:
        print(f"[warn] simulation更新に失敗しました: {e}")
        traceback.print_exc()


# --------------------------------------------------------------------------
# 期待リターンの推定と「おすすめ銘柄」の選定
# --------------------------------------------------------------------------
#
# 重要: 以下はスコア・モメンタム・アナリスト目標株価などから機械的に算出した
# 「目安」であり、将来の利益を保証するものではありません(投資助言ではありません)。

PREDICTION_BIAS = {
    "大きく値上がり": 1.0,
    "少し値上がり": 0.5,
    "変動なし": 0.0,
    "少し値下がり": -0.5,
    "大きく値下がり": -1.0,
}


def _num(v, default=None):
    """NaN/Inf/数値以外を弾いて float を返す(想定リターン計算にNaNを持ち込まないため)"""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    return f if math.isfinite(f) else default


def estimate_expected_return(row: dict, kind: str) -> float | None:
    """
    その銘柄の想定リターン(%)の目安を返す。
    short: 5営業日程度、long: 3ヶ月程度を想定。
      - 予想カテゴリの方向感 × 値幅(ATR/モメンタム)
      - 利益期待スコアとリスクスコアの差
      - 長期はアナリスト目標株価との乖離も加味
    """
    price = _num(row.get("price"))
    if not price:
        return None

    if kind == "day":
        bias = PREDICTION_BIAS.get(row.get("day_prediction"), 0.0)
        atrp = _num(row.get("atr_pct"), 2.0) or 2.0
        opp = _num(row.get("day_opportunity"), 50)
        risk = _num(row.get("day_risk"), 50)
        # 5営業日でATRの1.5倍程度を最大値幅として想定
        base = bias * min(atrp, 12) * 1.5
        edge = (opp - risk) / 100 * min(atrp, 12) * 0.8
        return _num(round(base + edge, 2))

    bias = PREDICTION_BIAS.get(row.get("long_prediction"), 0.0)
    opp = _num(row.get("long_opportunity"), 50)
    risk = _num(row.get("long_risk"), 50)
    mom3 = _num(row.get("mom_3m"), 0)
    base = bias * 8.0
    edge = (opp - risk) / 100 * 10.0
    trend = max(min(mom3, 40), -40) * 0.15
    upside = 0.0
    target = _num(row.get("target_mean"))
    if target and price:
        upside = max(min((target - price) / price * 100, 60), -30) * 0.25
    return _num(round(base + edge + trend + upside, 2))


def build_projection(price: float, exp_return_pct: float, points: int = 12) -> list[dict]:
    """
    期待リターンに向かって滑らかに推移する「想定利益の推移線」を生成する。
    step: 0(現在)〜points(期間終了時)。value は想定株価、pct は現在値からの騰落率。
    """
    price = _num(price)
    exp_return_pct = _num(exp_return_pct)
    if not price or exp_return_pct is None:
        return []
    out = []
    for i in range(points + 1):
        t = i / points
        # 直線ではなく、やや逓減するカーブ(初動が出て後半は緩む想定)
        shaped = t ** 0.85
        pct = exp_return_pct * shaped
        out.append({"step": i, "pct": round(pct, 3), "value": round(price * (1 + pct / 100), 4)})
    return out


def attach_expectations(rows: list[dict]) -> list[dict]:
    """各銘柄に想定リターンと想定推移線を付与する(閲覧ページの予測線表示用)"""
    for r in rows or []:
        try:
            ed = estimate_expected_return(r, "day")
            el = estimate_expected_return(r, "long")
            r["exp_return_day_pct"] = ed
            r["exp_return_long_pct"] = el
            r["projection_day"] = build_projection(r.get("price"), ed, points=10)
            r["projection_long"] = build_projection(r.get("price"), el, points=12)
        except Exception:
            continue
    return rows or []


def _rec_entry(r: dict, kind: str, reason: str) -> dict:
    exp = r.get("exp_return_day_pct") if kind == "day" else r.get("exp_return_long_pct")
    return {
        "symbol": r.get("symbol"),
        "price": r.get("price"),
        "sector": r.get("sector"),
        "kind": kind,
        "expected_return_pct": exp,
        "projection": r.get("projection_day") if kind == "day" else r.get("projection_long"),
        "opportunity": r.get("day_opportunity") if kind == "day" else r.get("long_opportunity"),
        "risk": r.get("day_risk") if kind == "day" else r.get("long_risk"),
        "prediction": r.get("day_prediction") if kind == "day" else r.get("long_prediction"),
        "next_earnings": r.get("next_earnings"),
        "reason": reason,
        "is_holding": bool(r.get("is_holding")),
        "qty": r.get("qty"),
    }


def build_recommendations(day_list, long_list, major_list, holdings_list=None, top_n: int = 5) -> list[dict]:
    """
    複数の投資戦略ごとに「おすすめ銘柄」を選定して返す。
    各戦略は {strategy, label, horizon, description, picks:[...]} の形。
    """
    day_list = day_list or []
    long_list = long_list or []
    major_list = major_list or []
    holdings_list = holdings_list or []

    def _srt(rows, key):
        return sorted([r for r in rows if key(r) is not None], key=key, reverse=True)

    strategies = []

    # 1) 短期(デイトレ〜数日)
    cand = [r for r in day_list if (r.get("exp_return_day_pct") or -99) > 0]
    picks = _srt(cand, lambda r: r.get("exp_return_day_pct"))[:top_n]
    strategies.append({
        "strategy": "short_term",
        "label": "短期(1〜5営業日)",
        "horizon": "1〜5営業日",
        "description": "値幅(ATR)と出来高が大きく、直近の方向感が上向きの銘柄。短時間で動く代わりに振れ幅も大きいため、ポジションは小さめに。",
        "picks": [_rec_entry(r, "day", "値幅・出来高が大きく、短期の指標が上向き") for r in picks],
    })

    # 2) スイング(数週間): 短期の勢い × 長期の健全性
    def _swing_score(r):
        ed, el = r.get("exp_return_day_pct"), r.get("exp_return_long_pct")
        if ed is None or el is None:
            return None
        return ed * 0.4 + el * 0.6 - (r.get("day_risk") or 50) * 0.05
    pool = {r.get("symbol"): r for r in (day_list + long_list)}.values()
    picks = _srt([r for r in pool if r.get("above_sma50")], _swing_score)[:top_n]
    strategies.append({
        "strategy": "swing",
        "label": "スイング(数週間)",
        "horizon": "2〜6週間",
        "description": "中期のトレンド(50日線の上)を維持しつつ、短期の勢いも出ている銘柄。押し目を待って分割で入る前提。",
        "picks": [_rec_entry(r, "long", "50日線の上でトレンド継続、短期の勢いも良好") for r in picks],
    })

    # 3) 長期・成長
    picks = _srt([r for r in long_list if r.get("above_sma200") is not False],
                 lambda r: r.get("exp_return_long_pct"))[:top_n]
    strategies.append({
        "strategy": "long_growth",
        "label": "長期・成長期待",
        "horizon": "3ヶ月〜1年",
        "description": "モメンタムとアナリスト目標株価の乖離から、中長期の上値余地が見込める銘柄。決算をまたぐ前提でポジションを取る想定。",
        "picks": [_rec_entry(r, "long", "中長期トレンドが良好で目標株価との乖離も大きい") for r in picks],
    })

    # 4) 積み立て(低リスク・主要企業)
    def _accum_score(r):
        opp = r.get("long_opportunity")
        risk = r.get("long_risk")
        if opp is None or risk is None:
            return None
        mc = r.get("market_cap") or 0
        if mc < 10_000_000_000:
            return None  # コア資産は大型株に限定する
        size_bonus = 10 if mc > 50_000_000_000 else 5
        stable = 10 if r.get("above_sma200") else 0
        return opp - risk * 1.2 + size_bonus + stable
    picks = _srt(major_list + long_list, _accum_score)[:top_n]
    strategies.append({
        "strategy": "accumulate",
        "label": "長期積み立て(コア)",
        "horizon": "1年以上・毎月積み立て",
        "description": "時価総額が大きくリスクスコアが低い、値動きが比較的安定した銘柄。毎月一定額を買い付けるコア資産向け。",
        "picks": [_rec_entry(r, "long", "大型でリスクが低く、長期の積み立てに向く") for r in picks],
    })

    # 5) 逆張り(売られすぎ)
    def _dip_score(r):
        rsi = r.get("rsi14")
        if rsi is None or rsi > 40:
            return None
        return (40 - rsi) + (r.get("long_opportunity") or 0) * 0.3
    picks = _srt(day_list + long_list + major_list, _dip_score)[:top_n]
    strategies.append({
        "strategy": "contrarian",
        "label": "逆張り(売られすぎ)",
        "horizon": "2週間〜3ヶ月",
        "description": "RSIが低く短期的に売られすぎの水準にある銘柄。下降トレンドが続くリスクもあるため、反転の兆し(出来高増・MACD)を確認してから。",
        "picks": [_rec_entry(r, "long", "RSIが低く売られすぎ水準からの反発余地") for r in picks],
    })

    # 6) 保有株のアクション(買い増し/保持/利確・撤退の目安)
    hold_picks = []
    for r in sorted(holdings_list, key=lambda x: -(x.get("market_value") or 0)):
        el = r.get("exp_return_long_pct")
        plr = r.get("unrealized_pl_rate")
        if el is None:
            action = "様子見"
        elif el >= 6:
            action = "買い増し検討"
        elif el <= -6:
            action = "利確・縮小検討"
        else:
            action = "保持"
        reason = f"想定リターン {el if el is not None else '—'}% / 含み損益 {round(plr,1) if plr is not None else '—'}%"
        e = _rec_entry(r, "long", reason)
        e["action"] = action
        hold_picks.append(e)
    strategies.append({
        "strategy": "holdings_action",
        "label": "保有株のアクション目安",
        "horizon": "保有中",
        "description": "現在保有している銘柄について、スコアと想定リターンから買い増し/保持/縮小の目安を示します。",
        "picks": hold_picks,
    })

    return strategies


def save_json_snapshot(day_list, long_list, major_list, universe_size, scanned_size, holdings_list=None) -> str | None:
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

        # 想定リターン・想定推移線を各銘柄に付与し、戦略別おすすめを組み立てる
        day_list = attach_expectations(day_list)
        long_list = attach_expectations(long_list)
        major_list = attach_expectations(major_list)
        holdings_list = attach_expectations(holdings_list or [])
        recommendations = build_recommendations(day_list, long_list, major_list, holdings_list)

        snapshot = {
            "run_id": run_id,
            "generated_at_jst": now.strftime("%Y-%m-%d %H:%M:%S JST"),
            "universe_size": universe_size,
            "scanned_size": scanned_size,
            "day_trade": _clean_records(day_list),
            "long_term": _clean_records(long_list),
            "major": _clean_records(major_list),
            "holdings": _clean_records(holdings_list or []),
            "recommendations": recommendations,
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

        # 保有株(Webull口座のポジション)分析。失敗してもスキャン本体は止めない。
        # 保有株はユニバースの絞り込み結果に関係なく、必ず全銘柄を個別に分析する。
        holdings_list = []
        if os.environ.get("SCAN_SKIP_HOLDINGS", "").lower() not in ("1", "true", "yes"):
            try:
                holdings_raw = fetch_holdings()
                if holdings_raw:
                    holdings_data_client = build_webull_client()
                    holdings_list = analyze_holdings(holdings_data_client, holdings_raw)
                    missing_qty = [h.get("symbol") for h in holdings_list if not h.get("qty")]
                    if missing_qty:
                        print(f"[warn] 保有数量が取得できなかった銘柄: {missing_qty}")
                    print(f"[info] 保有株 {len(holdings_list)}銘柄を個別に分析しました "
                          f"(ユニバースの絞り込みとは独立)")
                else:
                    print("[info] 保有株は0件でした")
            except Exception as e:
                print(f"[warn] 保有株分析に失敗しました: {e}")
                traceback.print_exc()

        html = render_email_html(day_list, long_list, major_list, len(universe), scanned_size, holdings_list)

        # Web閲覧ページ(GitHub Pages)用にJSONスナップショットを保存
        save_json_snapshot(day_list, long_list, major_list, len(universe), scanned_size, holdings_list)

        # 分析タブ用: 株価履歴・予想ログを更新(失敗してもメール送信は止めない)
        run_dt = datetime.now(timezone(timedelta(hours=9)))
        watched = _watched_symbols(day_list, long_list, major_list, holdings_list)
        update_price_histories(history, watched, run_dt)
        update_intraday_prices(day_list, long_list, major_list, run_dt, holdings_list)
        append_predictions_log(day_list, long_list, major_list, run_dt, holdings_list)

        # 予想パターンごとの的中率を学習データに反映(次回スキャンの predict_category に使われる)
        update_learning_state(run_dt)

        # 売買シミュレーション(予想に従った仮想売買)を1ステップ進める
        run_simulation(day_list, long_list, major_list, holdings_list, run_dt)

        # 強化学習モデルの再学習(実際の売買損益を報酬として、次回予想の精度向上に反映)
        update_ml_learning_state(run_dt)

        save_fundamentals_cache()

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

#$env:SIM_RESET="1"; $env:SIM_INITIAL_CASH="300"; python "c:\Users\81803\Downloads\kabuserver-main - コピー\kabuserver-main\us_stock_scanner_webull_openapi.py"
