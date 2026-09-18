# -*- coding: utf-8 -*-
"""
scikit-learn を使った「文脈的バンディット」型の強化学習モジュール。

考え方:
  - 状態(context) = 銘柄のテクニカル指標から作った特徴量ベクトル
  - 行動(action)  = 予想カテゴリ(大きく値上がり/少し値上がり/変動なし/
                     少し値下がり/大きく値下がり の5択)
  - 報酬(reward)  = その予想が実際にシミュレーション上の売買につながって
                     いれば実現損益($)。売買されなかった予想については、
                     SIM_BUY_USD_PER_ORDER 相当を「紙上で」その方向に
                     建てたと仮定した場合の概算損益($)で代用する
                     (そうしないと「変動なし」や不的中で買われなかった
                     予想には一切の学習信号が付かず、モデルが育たないため)。

行動(カテゴリ)ごとに独立した回帰モデル(RandomForestRegressor)を学習し、
「この特徴量でこの行動を取ったら、期待報酬はいくらか」を予測する。
推論時は5つの行動の期待報酬を比較して最大のものを選ぶが、
一定確率(epsilon)でランダムな行動を選び、データの薄い行動についても
探索的にサンプルを集める(= ε-greedy方策)。

学習は本体スクリプトの実行のたびに、蓄積された predictions_log.json と
simulation.json の取引履歴からゼロから再学習する(GitHub Actions上の
ステートレスな実行を前提としており、増分学習ではなくバッチ再学習)。
"""
from __future__ import annotations

import json
import math
import os
import pickle
import random
from datetime import datetime, timedelta

CATEGORY_ORDER = ["大きく値下がり", "少し値下がり", "変動なし", "少し値上がり", "大きく値上がり"]

FEATURE_NAMES = [
    "rsi14", "above_sma200", "above_sma50", "above_sma20",
    "mom_1m", "mom_3m", "atr_pct",
    "macd_bullish_cross", "macd_bearish_cross",
]

try:
    import numpy as np
    from sklearn.ensemble import RandomForestRegressor
    SKLEARN_AVAILABLE = True
except Exception:  # scikit-learn / numpy が入っていない環境でも本体は動かす
    SKLEARN_AVAILABLE = False


def _isnan(v) -> bool:
    return v is None or (isinstance(v, float) and math.isnan(v))


def _b(v) -> float:
    if v is True:
        return 1.0
    if v is False:
        return -1.0
    return 0.0


def _f(v, default: float = 0.0) -> float:
    try:
        if _isnan(v):
            return default
        return float(v)
    except Exception:
        return default


def featurize(row: dict) -> list:
    """テクニカル指標の行(row)から、モデル入力用の特徴量ベクトルを作る。"""
    return [
        _f(row.get("rsi14"), 50.0),
        _b(row.get("above_sma200")),
        _b(row.get("above_sma50")),
        _b(row.get("above_sma20")),
        _f(row.get("mom_1m"), 0.0),
        _f(row.get("mom_3m"), 0.0),
        _f(row.get("atr_pct"), 0.0),
        1.0 if row.get("macd_bullish_cross") else 0.0,
        1.0 if row.get("macd_bearish_cross") else 0.0,
    ]


def _paper_reward(category: str, pct_chg: float, order_usd: float) -> float:
    """実際には売買されなかった予想について、$order_usdを紙上でその方向に
    建てたと仮定した場合の概算損益($)を計算する(報酬の代用値)。"""
    if "値上がり" in category:
        return order_usd * (pct_chg / 100.0)
    if "値下がり" in category:
        # 「下がる」と予想して実際に下がった/上がったかで正負が決まる
        # (空売りではなく、あくまで「買わずに避けた」ことの価値として符号を反転)
        return order_usd * (-pct_chg / 100.0)
    # 「変動なし」: 実際に動いた分だけ、機会損失/無駄な静観として小さく減点
    return -order_usd * (abs(pct_chg) / 100.0) * 0.3


def _actual_pct_change(entry: dict, hist: list, today_str: str):
    p0 = entry.get("price_at_prediction")
    made = entry.get("made_date")
    end_str = entry.get("horizon_end")
    if p0 is None or not hist or not made or not end_str or end_str > today_str:
        return None
    pts = sorted(
        [h for h in hist if h.get("date") and made <= h["date"] <= end_str and h.get("close") is not None],
        key=lambda h: h["date"],
    )
    if not pts or not p0:
        return None
    return (pts[-1]["close"] - p0) / p0 * 100.0


def build_training_rows(predictions_log: dict, sim_trades: list, load_hist_fn, today_str: str,
                         order_usd: float) -> list:
    """(features, kind, action, reward) のタプルのリストを作る。

    - sim_trades: simulation.json の _trades_raw(実際の売買履歴)
    - load_hist_fn: symbol -> [{"date":..., "close":...}, ...] を返す関数
    """
    # symbol+kind ごとに、時系列順の「売り」トレード(実現損益つき)を集める
    sells_by_key: dict = {}
    for t in sim_trades or []:
        if t.get("side") != "sell" or t.get("realized_pl") is None:
            continue
        key = (t.get("symbol"), t.get("kind"))
        sells_by_key.setdefault(key, []).append(t)
    for lst in sells_by_key.values():
        lst.sort(key=lambda t: t.get("time") or "")

    rows = []
    entries = (predictions_log or {}).get("entries", [])
    for e in entries:
        feats = e.get("features")
        sym = e.get("symbol")
        kind = e.get("kind")
        category = e.get("category")
        made = e.get("made_date")
        if not feats or not sym or not kind or not category or not made:
            continue

        reward = None
        # ① 実際にこの銘柄・この予想種別で「売り」が成立していれば、
        #    made_date以降で最初に来た売りトレードの実現損益を報酬に使う
        for t in sells_by_key.get((sym, kind), []):
            t_time = (t.get("time") or "")[:10]
            if t_time >= made:
                reward = float(t.get("realized_pl") or 0.0)
                break

        # ② 実売買がなければ、価格データから紙上の概算損益で代用する
        if reward is None:
            hist = load_hist_fn(sym)
            pct_chg = _actual_pct_change(e, hist, today_str)
            if pct_chg is None:
                continue  # まだ判定期間が終わっていない
            reward = _paper_reward(category, pct_chg, order_usd)

        rows.append((feats, kind, category, reward))
    return rows


class RewardModel:
    """kind("day"/"long") ごと・行動(カテゴリ)ごとに独立した期待報酬の回帰モデル。"""

    def __init__(self):
        self.models: dict = {}  # (kind, category) -> RandomForestRegressor
        self.sample_counts: dict = {}  # (kind, category) -> int
        self.trained_at: str | None = None

    def fit(self, rows: list, min_samples: int):
        if not SKLEARN_AVAILABLE:
            return
        by_key: dict = {}
        for feats, kind, category, reward in rows:
            by_key.setdefault((kind, category), []).append((feats, reward))
        self.models = {}
        self.sample_counts = {}
        for key, samples in by_key.items():
            self.sample_counts[key] = len(samples)
            if len(samples) < min_samples:
                continue
            X = np.array([s[0] for s in samples], dtype=float)
            y = np.array([s[1] for s in samples], dtype=float)
            model = RandomForestRegressor(n_estimators=80, max_depth=6, min_samples_leaf=3,
                                           random_state=42, n_jobs=-1)
            model.fit(X, y)
            self.models[key] = model
        self.trained_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    def predict_rewards(self, kind: str, feats: list) -> dict:
        """行動(カテゴリ)ごとの期待報酬を予測する。学習済みモデルがない
        カテゴリは None(=判断材料なし)を返す。"""
        out = {}
        for category in CATEGORY_ORDER:
            model = self.models.get((kind, category))
            if model is None:
                out[category] = None
                continue
            out[category] = float(model.predict(np.array([feats], dtype=float))[0])
        return out

    def coverage(self, kind: str) -> dict:
        return {c: self.sample_counts.get((kind, c), 0) for c in CATEGORY_ORDER}


def save_model(model: RewardModel, path: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(model, f)
    os.replace(tmp, path)


def load_model(path: str) -> RewardModel | None:
    try:
        if os.path.exists(path) and os.path.getsize(path) > 0:
            with open(path, "rb") as f:
                obj = pickle.load(f)
            if isinstance(obj, RewardModel):
                return obj
    except Exception:
        pass
    return None


def choose_action(model: RewardModel | None, kind: str, feats: list, fallback_category: str,
                   epsilon: float, min_samples: int, rng: random.Random) -> tuple:
    """ε-greedy方策で予想カテゴリを選ぶ。
    戻り値: (category, meta) meta には選択理由・探索有無・各行動の期待報酬を含む。
    モデルが無い/十分なデータがない場合はルールベースの fallback_category を使う。
    """
    meta = {"source": "rule_fallback", "explored": False, "expected_rewards": None}
    if model is None or not SKLEARN_AVAILABLE:
        return fallback_category, meta

    expected = model.predict_rewards(kind, feats)
    meta["expected_rewards"] = expected
    usable = {c: r for c, r in expected.items() if r is not None}
    if not usable:
        return fallback_category, meta

    if rng.random() < epsilon:
        # 探索: 学習済みの行動の中からランダムに選ぶ(データの薄い行動にも
        # あえて予想を出させて、次回以降の学習データを増やす)
        category = rng.choice(list(usable.keys()))
        meta["source"] = "explore"
        meta["explored"] = True
        return category, meta

    # 活用: 期待報酬が最大の行動を選ぶ
    category = max(usable.items(), key=lambda kv: kv[1])[0]
    meta["source"] = "model"
    return category, meta
