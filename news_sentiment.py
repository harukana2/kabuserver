# -*- coding: utf-8 -*-
"""
news_sentiment.py
==================
yfinance の `Ticker.news`(無料・見出しのみ)を使い、外部の有料センチメントAPI
(Alpha Vantage / Finnhub 等)に頼らず、**自前の辞書ベース(lexicon-based)**で
ニュースの強気/弱気度をスコア化するモジュール。

なぜ辞書ベースか
----------------
- ネットワーク越しの学習済みモデル(BERT系など)を都度ダウンロードするのは
  GitHub Actions 実行のたびに重い/失敗しうる。
- 金融ニュースの見出しは短く定型的な語彙(beat/miss/downgrade/guidance等)が
  多いため、Loughran-McDonald(金融テキスト向け極性辞書)の考え方をベースに
  した簡易辞書でも実用上十分なシグナルになる。
- 完全にオフラインで動作し、追加の課金・レート制限が発生しない。

制限事項(利用時に必ず認識してください)
----------------------------------------
- 皮肉・複雑な構文・複合文の否定などは正しく拾えない(ルールベースの限界)。
- yfinance の news エンドポイントは非公式ラッパーであり、フィールド構造が
  バージョンによって変わることがある(下記 `_extract_headline_and_time` で
  複数パターンに対応)。
- あくまで「今日のニュースの雰囲気」を表す補助特徴量であり、単独で
  売買判断してはいけない。
"""
from __future__ import annotations

import math
import re
from datetime import datetime, timezone, timedelta

# --------------------------------------------------------------------------
# 金融特化型 簡易極性辞書
# (Loughran & McDonald の金融テキスト用ポジティブ/ネガティブ語彙リストの
#  考え方を踏まえた自前の簡易版。一般的な英単語ではなく決算・株式ニュースの
#  見出しで頻出する語に絞ってある)
# --------------------------------------------------------------------------
POSITIVE_WORDS = {
    "beat", "beats", "beating", "surge", "surges", "surged", "soar", "soars",
    "soared", "rally", "rallies", "rallied", "upgrade", "upgraded", "upgrades",
    "outperform", "outperforms", "record", "growth", "grows", "grew",
    "profit", "profits", "profitable", "strong", "stronger", "strength",
    "gain", "gains", "gained", "jump", "jumps", "jumped", "bullish",
    "buyback", "buybacks", "dividend", "raise", "raised", "raises",
    "exceeds", "exceeded", "exceeding", "optimistic", "optimism", "boost",
    "boosts", "boosted", "expansion", "expand", "expands", "win", "wins",
    "won", "approval", "approved", "breakthrough", "innovation", "innovative",
    "partnership", "deal", "acquire", "acquires", "acquisition", "milestone",
    "top", "tops", "topping", "positive", "recovery", "recovers", "rebound",
    "rebounds", "resilient", "robust", "accelerate", "accelerates",
    "upbeat", "momentum", "all-time high", "buy rating", "overweight",
}

NEGATIVE_WORDS = {
    "miss", "misses", "missed", "missing", "plunge", "plunges", "plunged",
    "slump", "slumps", "slumped", "downgrade", "downgraded", "downgrades",
    "underperform", "underperforms", "loss", "losses", "weak", "weaker",
    "weakness", "decline", "declines", "declined", "drop", "drops",
    "dropped", "fall", "falls", "fell", "bearish", "layoff", "layoffs",
    "lawsuit", "sues", "sued", "investigation", "probe", "recall",
    "recalls", "recalled", "fraud", "scandal", "warning", "warns",
    "warned", "cut", "cuts", "cutting", "bankruptcy", "bankrupt",
    "default", "delisting", "delist", "resign", "resigns", "resigned",
    "resignation", "fired", "fires", "concern", "concerns", "concerned",
    "risk", "risks", "risky", "volatile", "volatility", "sell-off",
    "selloff", "crash", "crashes", "crashed", "shortfall", "disappoint",
    "disappoints", "disappointing", "disappointed", "guidance cut",
    "downturn", "recession", "tariff", "tariffs", "ban", "banned",
    "halt", "halted", "suspend", "suspended", "sec charges", "subpoena",
    "underweight", "sell rating", "slowdown", "slows", "slowed",
}

# 否定語(直前1〜2語にあると極性を反転させる。簡易的な対応)
NEGATIONS = {"not", "no", "never", "without", "fails", "failed", "failing"}

_WORD_RE = re.compile(r"[a-z][a-z\-']*")


def _tokenize(text: str) -> list:
    return _WORD_RE.findall((text or "").lower())


def score_headline(text: str) -> float:
    """1つの見出し(英文)を -1.0(弱気)〜 +1.0(強気) でスコア化する。

    シンプルな bag-of-words + 直前語の否定反転のみ。複雑な構文解析はしない。
    """
    tokens = _tokenize(text)
    if not tokens:
        return 0.0

    score = 0.0
    hits = 0
    for i, tok in enumerate(tokens):
        polarity = 0
        if tok in POSITIVE_WORDS:
            polarity = 1
        elif tok in NEGATIVE_WORDS:
            polarity = -1
        else:
            continue

        # 直前2語以内に否定語があれば反転する(例: "not profitable" → 弱気)
        window = tokens[max(0, i - 2):i]
        if any(w in NEGATIONS for w in window):
            polarity *= -1

        score += polarity
        hits += 1

    if hits == 0:
        return 0.0
    # 語数で正規化しつつ、-1〜1にクリップ
    return max(-1.0, min(1.0, score / hits))


def _extract_headline_and_time(item: dict):
    """yfinance の news 1件分の辞書から (見出しテキスト, 公開日時UTC) を取り出す。

    yfinanceのバージョンによって構造が変わりうるため("content"配下にネストする
    版と、トップレベルに"title"がある版の両方を確認)、両対応にしてある。
    """
    content = item.get("content") if isinstance(item.get("content"), dict) else None
    title = None
    pub_time = None

    if content:
        title = content.get("title") or content.get("summary")
        pub_str = content.get("pubDate") or content.get("displayTime")
        if pub_str:
            try:
                pub_time = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
            except Exception:
                pub_time = None
    if title is None:
        title = item.get("title")
    if pub_time is None:
        ts = item.get("providerPublishTime")
        if ts:
            try:
                pub_time = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            except Exception:
                pub_time = None

    return title, pub_time


def fetch_and_score_news(yf_ticker, now_utc: datetime = None, lookback_days: int = 7,
                          halflife_hours: float = 36.0, max_items: int = 20) -> dict:
    """yfinance の Ticker オブジェクトからニュース見出しを取得し、
    直近ニュースの加重平均センチメントを計算する。

    Parameters
    ----------
    yf_ticker : yfinance.Ticker
        呼び出し側で既に生成済みの Ticker インスタンス(fundamentalsと使い回す想定)
    now_utc : datetime
        基準時刻(省略時は現在時刻UTC)。テストや再現性のために外から渡せるようにしてある。
    lookback_days : int
        何日前までのニュースを対象にするか
    halflife_hours : float
        新しいニュースほど重く扱うための半減期(時間)。36時間で重みが半分になる。
    max_items : int
        yfinanceから取得するニュース件数の上限

    Returns
    -------
    dict:
        news_sentiment_score   : float  -1.0〜1.0 (加重平均、ニュース無ければ0.0)
        news_volume_7d         : int    直近lookback_days日以内の件数
        news_sentiment_conf    : float  0.0〜1.0 (件数が多いほど1に近づく信頼度)
        news_headlines_sample  : list[str] デバッグ・目視確認用に採用した見出し(最大5件)
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=lookback_days)

    out = {
        "news_sentiment_score": 0.0,
        "news_volume_7d": 0,
        "news_sentiment_conf": 0.0,
        "news_headlines_sample": [],
    }

    try:
        raw_news = yf_ticker.news
    except Exception:
        raw_news = None

    if not raw_news:
        return out

    weighted_sum = 0.0
    weight_total = 0.0
    count = 0
    sample = []

    for item in raw_news[:max_items]:
        if not isinstance(item, dict):
            continue
        title, pub_time = _extract_headline_and_time(item)
        if not title or not pub_time:
            continue
        if pub_time < cutoff:
            continue

        age_hours = max(0.0, (now_utc - pub_time).total_seconds() / 3600.0)
        # 指数減衰: weight = 0.5 ^ (age_hours / halflife_hours)
        weight = math.pow(0.5, age_hours / halflife_hours)

        s = score_headline(title)
        weighted_sum += s * weight
        weight_total += weight
        count += 1
        if len(sample) < 5:
            sample.append(title)

    if count > 0 and weight_total > 0:
        out["news_sentiment_score"] = round(weighted_sum / weight_total, 4)
    out["news_volume_7d"] = count
    # 信頼度: 0件=0.0, 5件以上でほぼ1.0に近づく(単純な飽和関数)
    out["news_sentiment_conf"] = round(1.0 - math.exp(-count / 3.0), 4)
    out["news_headlines_sample"] = sample

    return out
