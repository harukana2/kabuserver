# -*- coding: utf-8 -*-
"""
scikit-learn を使った「文脈的バンディット」型の強化学習モジュール(改善版)。

考え方:
  - 状態(context) = 銘柄のテクニカル指標+ニュース感情から作った特徴量ベクトル
  - 行動(action)  = 予想カテゴリ(大きく値下がり/少し値下がり/変動なし/
                     少し値上がり/大きく値上がり の5択)
  - 報酬(reward)  = その行動を取った場合の概算損益($)。株価の実績から計算する。

旧版からの主な変更点
--------------------
1. 【フルインフォメーション学習】報酬は実績の騰落率から「全5行動分」を計算できる
   ので、1件の予想ログから5行(各行動の反事後報酬)の学習データを作る。
   旧版は「実際に選んだ1行動」分しか使わず、全行動が20件に達するまで学習が
   始まらなかった(rl_status.json で training_rows=8 / 学習済み行動0 だった原因)。
2. 【報酬の一貫性】実売買の実現損益(銘柄・種別と日付だけの粗い対応づけで、
   別予想との重複や「値下がり」予想へのロング損益の混入があった)は使わず、
   全行動で同一の定義(価格ベースの紙上損益)に統一。
   「大きく/少し」はポジションサイズ(1.0/0.5)の違いとして報酬に反映し、
   自信が無いときは小さい行動、確信があるときは大きい行動が選ばれるようにした。
3. 【未学習ログの救済】
   - 特徴量が旧バージョン(次元数が少ない)のログは、足りない末尾を中立値で
     埋めて利用する(旧版は次元不一致で再学習全体が失敗し得た)。
   - 判定期限前でも一定割合(MIN_PROVISIONAL_FRAC)以上経過していれば、
     その時点までの値動きを「暫定ラベル」として低い重みで利用する。
   - 履歴が期限まで届いていない場合も同様に暫定扱い(旧版は黙って途中までの
     損益を完全なラベルとして使っていた)。
   - 使えなかったログは理由別に件数を LAST_BUILD_STATS に記録する。
4. 【検証の改善】学習/検証の分割を「日付単位」にして、同日の銘柄が学習側と
   検証側にまたがる漏れを防止。検証には確定ラベルのみ使用し、MAE がベース
   ラインに勝つことに加えて符号一致率 >= 0.5 も要求する。
5. 【活用時の安全弁】検証合格の行動の中で最大の期待報酬が 0 以下なら、
   「どの行動も儲からない」と判断して「変動なし」(ノーポジション)を選ぶ。

学習は本体スクリプトの実行のたびに、蓄積された predictions_log.json と
価格履歴からゼロから再学習する(GitHub Actions上のステートレスな実行を前提とした
バッチ再学習)。
"""
from __future__ import annotations

import math
import os
import pickle
import random
from datetime import datetime

CATEGORY_ORDER = ["大きく値下がり", "少し値下がり", "変動なし", "少し値上がり", "大きく値上がり"]

FEATURE_NAMES = [
    "rsi14", "above_sma200", "above_sma50", "above_sma20",
    "mom_1m", "mom_3m", "atr_pct",
    "macd_bullish_cross", "macd_bearish_cross",
    "news_sentiment_score", "news_sentiment_conf",
]
# 旧バージョンの短い特徴量を末尾パディングするときの中立値(featurize の既定値と同じ)
FEATURE_DEFAULTS = [50.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

# 「大きく」「少し」をポジションサイズの違いとして報酬に反映する
SIZE_MULT = {
    "大きく値上がり": 1.0, "少し値上がり": 0.5, "変動なし": 0.0,
    "少し値下がり": 0.5, "大きく値下がり": 1.0,
}

# 判定期限に対して履歴がこの割合以上カバーしていれば「確定ラベル」とみなす
COMPLETE_FRAC = 0.8
# 期限前でも、この割合以上経過していれば「暫定ラベル」として学習に使う
MIN_PROVISIONAL_FRAC = 0.4
# 暫定ラベルの基本重み(実際の重み = PROVISIONAL_WEIGHT * 経過割合)
PROVISIONAL_WEIGHT = 0.5

# 【安定志向】買い(値上がり系)で損した場合の損失を何倍に数えるか。
# 1.0超にすると「儲かる/損するが五分五分」の行動より「見送り」が選ばれやすくなり、
# 一発狙いの過熱銘柄の追っかけ買いが学習で抑えられる。
LOSS_AVERSION = 1.5
# 「変動なし(見送り)」で実際に動いた分を機会損失として減点する係数
ABSTAIN_OPP_COST = 0.1
# 活用(greedy)時に「買う/売る」を選ぶのに必要な最低期待報酬($)。これ以下は見送り。
MIN_EDGE_USD = 0.02
# 検証でモデルが「買う/売る」と判断した件のうち、最低これだけの件数が無いと信用しない
MIN_PICKS_FOR_TRUST = 5
# 検証データの最低件数(日付をまたいだ確定ラベル)
MIN_VAL_SAMPLES = 15
# 検証期間の直前、この日数分は学習に使わない(判定期限が検証期間に食い込む漏れの防止)
EMBARGO_DAYS = 2  # kind 不明時のフォールバック
# 【修正2】エンバーゴは判定期限(day=7日/long=95日)以上にする。
# 短いと、学習サンプルのラベルが検証期間の値動きを含み、検証成績が甘く出る。
EMBARGO_DAYS_BY_KIND = {"day": 7, "long": 95}
# 【修正3】検証に使う日付数の下限。同日の銘柄は同じ地合いに連動するため、
# 件数ではなく「日数」で最低ラインを決める。
MIN_VAL_DATES = 5
# 【修正4】騰落率のクリップ(%)。株式分割などで基準がずれた偽の騰落率が報酬を汚さないように。
RETURN_CLIP_PCT = {"day": 30.0, "long": 100.0}
# 【修正4】暫定ラベルに必要な最低の価格点数(週末またぎで実質1営業日だけのラベルを除外)
MIN_HIST_POINTS = 3

# 直近の build_training_rows の集計(使えた/使えなかった理由別の件数)。
# 本体スクリプトが rl_status.json に書き出して原因調査に使う。
LAST_BUILD_STATS: dict = {}

try:
    import numpy as np
    from sklearn.ensemble import RandomForestRegressor
    SKLEARN_AVAILABLE = True
except Exception:  # scikit-learn / numpy が入っていない環境でも本体は動かす
    SKLEARN_AVAILABLE = False


def _isnan(v) -> bool:
    if v is None:
        return True
    if isinstance(v, bool):
        return False
    try:
        return math.isnan(float(v))
    except Exception:
        return False


def _b(v) -> float:
    # numpy.bool_ も受け付ける(`v is True` だと np.bool_ が 0.0 に化けるため)
    if v is None or _isnan(v):
        return 0.0
    if isinstance(v, bool) or getattr(getattr(v, "dtype", None), "kind", "") == "b":
        return 1.0 if bool(v) else -1.0
    return 0.0


def _f(v, default: float = 0.0) -> float:
    try:
        if _isnan(v):
            return default
        return float(v)
    except Exception:
        return default


def featurize(row: dict) -> list:
    """テクニカル指標の行(row)から、モデル入力用の特徴量ベクトルを作る。

    news_sentiment_score / news_sentiment_conf は yfinance の news 見出しを
    news_sentiment.py で辞書ベース(自前・無料)にスコア化したもの。
    ニュースが取得できなかった銘柄は 0.0(中立・信頼度なし)として扱われる。
    """
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
        _f(row.get("news_sentiment_score"), 0.0),
        _f(row.get("news_sentiment_conf"), 0.0),
    ]


def _normalize_feats(feats):
    """ログ中の特徴量を現行の次元数にそろえる。
    戻り値: (ベクトル or None, 状態) 状態は "ok" / "padded" / 失敗理由。"""
    if not isinstance(feats, (list, tuple)) or len(feats) == 0:
        return None, "no_features"
    n = len(FEATURE_NAMES)
    if len(feats) > n:
        return None, "bad_feature_len"
    vec = [_f(v, FEATURE_DEFAULTS[i]) for i, v in enumerate(feats)]
    if len(vec) < n:
        vec += FEATURE_DEFAULTS[len(vec):]
        return vec, "padded"
    return vec, "ok"


def _paper_reward(category: str, pct_chg: float, order_usd: float) -> float:
    """行動(カテゴリ)を取った場合の概算損益($)。全行動で同一の定義。

    - 値上がり系: order_usd × サイズ倍率 × 騰落率(買った場合の損益)。損失はLOSS_AVERSION倍
    - 値下がり系: 下がると予想して避けた/空売りした価値として符号を反転(損失は同じく加重)
    - 変動なし  : ノーポジション。実際に動いた分だけ機会損失として小さく減点
    """
    mult = SIZE_MULT.get(category, 1.0)
    if "値上がり" in category:
        pnl = order_usd * mult * (pct_chg / 100.0)
        return pnl * LOSS_AVERSION if pnl < 0 else pnl
    if "値下がり" in category:
        pnl = order_usd * mult * (-pct_chg / 100.0)
        return pnl * LOSS_AVERSION if pnl < 0 else pnl
    return -order_usd * (abs(pct_chg) / 100.0) * ABSTAIN_OPP_COST


def _parse_date(s):
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d")
    except Exception:
        return None


def _actual_return(entry: dict, hist: list, today_str: str):
    """予想日から(判定期限 or 今日の早い方)までの騰落率を返す。
    戻り値: (pct, frac, reason)。frac は期限に対する履歴のカバー率(0〜1)。"""
    p0 = entry.get("price_at_prediction")
    made = entry.get("made_date")
    end_str = entry.get("horizon_end")
    if not p0 or not made or not end_str:
        return None, 0.0, "no_price_or_dates"
    if not hist:
        return None, 0.0, "no_history"
    upper = min(end_str, today_str)
    pts = sorted(
        [h for h in hist if h.get("date") and made <= h["date"] <= upper and h.get("close") is not None],
        key=lambda h: h["date"],
    )
    if not pts:
        return None, 0.0, "no_history"
    if len(pts) < MIN_HIST_POINTS:
        return None, 0.0, "history_short"
    d0, dl, de = _parse_date(made), _parse_date(pts[-1]["date"]), _parse_date(end_str)
    if not d0 or not dl or not de:
        return None, 0.0, "bad_date"
    horizon_days = max(1, (de - d0).days)
    frac = max(0.0, min(1.0, (dl - d0).days / horizon_days))
    pct = (float(pts[-1]["close"]) - float(p0)) / float(p0) * 100.0
    return pct, frac, None


def build_training_rows(predictions_log: dict, sim_trades: list, load_hist_fn, today_str: str,
                         order_usd: float) -> list:
    """(features, kind, action, reward, made_date, weight) のタプルのリストを作る。

    1件の予想ログにつき、全5行動分の行を作る(フルインフォメーション学習)。
    sim_trades は互換性のために引数として残しているが、報酬の一貫性のため使わない
    (実売買の実現損益は予想との対応が曖昧で、行動ごとに意味が変わってしまうため)。
    """
    global LAST_BUILD_STATS
    stats = {
        "entries_total": 0, "entries_used": 0, "rows": 0,
        "complete_labels": 0, "provisional_labels": 0, "padded_features": 0,
        "skipped": {},
    }

    def skip(reason):
        stats["skipped"][reason] = stats["skipped"].get(reason, 0) + 1

    rows = []
    entries = (predictions_log or {}).get("entries", [])
    for e in entries:
        stats["entries_total"] += 1
        sym, kind, category, made = e.get("symbol"), e.get("kind"), e.get("category"), e.get("made_date")
        if not sym or not kind or not made:
            skip("missing_fields")
            continue

        feats, fstate = _normalize_feats(e.get("features"))
        if feats is None:
            skip(fstate)
            continue

        pct, frac, reason = _actual_return(e, load_hist_fn(sym), today_str)
        if reason:
            skip(reason)
            continue
        clip = RETURN_CLIP_PCT.get(kind, 100.0)
        if abs(pct) > clip:
            stats["clipped_returns"] = stats.get("clipped_returns", 0) + 1
            pct = max(-clip, min(clip, pct))

        end_str = e.get("horizon_end") or ""
        if end_str <= today_str and frac >= COMPLETE_FRAC:
            weight, label = 1.0, "complete"
        elif frac >= MIN_PROVISIONAL_FRAC:
            weight, label = PROVISIONAL_WEIGHT * frac, "provisional"
        else:
            skip("too_early" if end_str > today_str else "history_short")
            continue

        for cat in CATEGORY_ORDER:
            rows.append((feats, kind, cat, _paper_reward(cat, pct, order_usd), made, weight))

        stats["entries_used"] += 1
        stats["complete_labels" if label == "complete" else "provisional_labels"] += 1
        if fstate == "padded":
            stats["padded_features"] += 1

    stats["rows"] = len(rows)
    LAST_BUILD_STATS = stats
    return rows


def _edge_threshold(category: str) -> float:
    """行動ごとの最低期待報酬($)。サイズ倍率に比例させ、「少し」と「大きく」で
    必要な期待リターン(%)が同じになるようにする。"""
    return MIN_EDGE_USD * SIZE_MULT.get(category, 1.0)


def _recency_weight(made_date: str, today_str: str, halflife_days: float) -> float:
    """予想が行われた日(made_date)が新しいほど大きい重みを返す(指数減衰)。
    パース失敗時は中立(1.0)を返す。"""
    d0, d1 = _parse_date(made_date), _parse_date(today_str)
    if not d0 or not d1 or halflife_days <= 0:
        return 1.0
    age_days = max(0.0, (d1 - d0).days)
    return math.pow(0.5, age_days / halflife_days)


def _new_forest():
    return RandomForestRegressor(n_estimators=80, max_depth=6, min_samples_leaf=3,
                                  random_state=42, n_jobs=-1)


class RewardModel:
    """kind("day"/"long") ごと・行動(カテゴリ)ごとに独立した期待報酬の回帰モデル。

    時系列(日付単位)で学習/検証に分割し、「何も学習していないベースライン
    (=訓練期間の平均報酬で常に予測する)」とのMAE比較と符号一致率で検証する。
    合格しない行動は validated=False/beats_baseline=False となり、
    choose_action の活用(greedy)には使われず探索(explore)のみに回る。
    """

    def __init__(self):
        self.models: dict = {}  # (kind, category) -> RandomForestRegressor
        self.sample_counts: dict = {}  # (kind, category) -> int
        self.metrics: dict = {}  # (kind, category) -> {...}
        self.trained_at: str | None = None

    def fit(self, rows: list, min_samples: int, today_str: str | None = None,
            recency_halflife_days: float = 60.0, val_frac: float = 0.25):
        if not SKLEARN_AVAILABLE:
            return
        today_str = today_str or datetime.utcnow().strftime("%Y-%m-%d")

        by_key: dict = {}
        for row in rows:
            feats, kind, category, reward, made = row[:5]
            label_w = row[5] if len(row) > 5 else 1.0
            by_key.setdefault((kind, category), []).append((feats, reward, made or today_str, label_w))

        self.models = {}
        self.sample_counts = {}
        self.metrics = {}

        def weights(samples):
            return np.array([_recency_weight(s[2], today_str, recency_halflife_days) * s[3] for s in samples],
                            dtype=float)

        for key, samples in by_key.items():
            self.sample_counts[key] = len(samples)
            if len(samples) < min_samples:
                continue

            samples_sorted = sorted(samples, key=lambda s: s[2])

            # 日付単位の時系列分割。
            # 旧版は「直近の日付」を検証に回していたが、直近は判定期限前の暫定ラベルばかりで
            # 確定ラベルが0件 → 検証が一度も走らず全行動が未検証(=モデルが一度も使われない)
            # になっていた。今回は「確定ラベルがある日付」の直近を検証に使い、
            # それより前(+禁止期間EMBARGO_DAYS)だけで学習する。
            dates = sorted({s[2] for s in samples_sorted})
            complete_dates = sorted({s[2] for s in samples_sorted if s[3] >= 0.999})
            val_dates = set()
            cutoff_dt = None
            embargo = EMBARGO_DAYS_BY_KIND.get(key[0], EMBARGO_DAYS)
            if len(complete_dates) >= MIN_VAL_DATES + 2:
                n_vd = max(MIN_VAL_DATES, int(round(len(complete_dates) * val_frac)))
                val_dates = set(complete_dates[-n_vd:])
                cutoff_dt = _parse_date(min(val_dates))
            train_s, val_s = [], []
            for smp in samples_sorted:
                d = _parse_date(smp[2])
                if cutoff_dt is not None and d is not None:
                    if smp[2] in val_dates and smp[3] >= 0.999:
                        val_s.append(smp)
                    elif (cutoff_dt - d).days > embargo:
                        train_s.append(smp)

            if (len(val_dates) >= MIN_VAL_DATES and len(val_s) >= MIN_VAL_SAMPLES
                    and len(train_s) >= min_samples):
                X_tr = np.array([s[0] for s in train_s], dtype=float)
                y_tr = np.array([s[1] for s in train_s], dtype=float)
                val_model = _new_forest()
                val_model.fit(X_tr, y_tr, sample_weight=weights(train_s))

                X_val = np.array([s[0] for s in val_s], dtype=float)
                y_val = np.array([s[1] for s in val_s], dtype=float)
                pred_val = val_model.predict(X_val)

                baseline_pred = float(np.mean(y_tr))
                mae_model = float(np.mean(np.abs(pred_val - y_val)))
                mae_baseline = float(np.mean(np.abs(baseline_pred - y_val)))
                hit_rate = float(np.mean(np.sign(pred_val) == np.sign(y_val)))

                # 実運用と同じ使い方での成績: モデルが「期待報酬 > MIN_EDGE_USD」と
                # 判断した件を実際に取った場合の平均損益($)。これがプラスでなければ信用しない。
                picked = pred_val > _edge_threshold(category)
                pick_n = int(picked.sum())
                pick_avg = float(np.mean(y_val[picked])) if pick_n else 0.0

                beats = (mae_model < mae_baseline) and (hit_rate >= 0.5)
                if category != "変動なし":
                    beats = beats and pick_n >= MIN_PICKS_FOR_TRUST and pick_avg > 0.0

                self.metrics[key] = {
                    "validated": True,
                    "n_train": len(train_s),
                    "n_val": len(val_s),
                    "n_dates": len(dates),
                    "mae_model": round(mae_model, 4),
                    "mae_baseline": round(mae_baseline, 4),
                    "beats_baseline": bool(beats),
                    "hit_rate": round(hit_rate, 4),
                    "pick_n": pick_n,
                    "pick_avg_reward": round(pick_avg, 4),
                }
            else:
                self.metrics[key] = {"validated": False, "n_train": len(samples_sorted),
                                     "n_val": len(val_s), "n_dates": len(dates),
                                     "n_complete_dates": len(complete_dates),
                                     "embargo_days": embargo,
                                     "n_train_after_embargo": len(train_s)}

            # 本番用モデルは全データ(検証データも含む)で、直近ほど・確定ラベルほど
            # 重く学習する。検証は「信頼できるか」の判定専用。
            X_all = np.array([s[0] for s in samples_sorted], dtype=float)
            y_all = np.array([s[1] for s in samples_sorted], dtype=float)
            model = _new_forest()
            model.fit(X_all, y_all, sample_weight=weights(samples_sorted))
            self.models[key] = model

        self.trained_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    def predict_rewards(self, kind: str, feats: list) -> dict:
        """行動(カテゴリ)ごとの期待報酬を予測する。学習済みモデルがない
        カテゴリは None(=判断材料なし)を返す。"""
        out = {}
        x = np.array([feats], dtype=float)
        for category in CATEGORY_ORDER:
            model = self.models.get((kind, category))
            out[category] = None if model is None else float(model.predict(x)[0])
        return out

    def is_validated_and_better(self, kind: str, category: str) -> bool:
        """このカテゴリのモデルが、検証で「何も学習しないベースライン」より
        明確に優れていると確認できているか。未検証なら False(慎重側)。"""
        m = self.metrics.get((kind, category))
        return bool(m and m.get("validated") and m.get("beats_baseline"))

    def coverage(self, kind: str) -> dict:
        return {c: self.sample_counts.get((kind, c), 0) for c in CATEGORY_ORDER}

    def metrics_for(self, kind: str) -> dict:
        return {c: self.metrics.get((kind, c)) for c in CATEGORY_ORDER}


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

    # 特徴量の次元が現行と違う場合は推論しない(モデル不整合の保険)
    feats, fstate = _normalize_feats(feats)
    if feats is None:
        meta["reason"] = fstate
        return fallback_category, meta

    expected = model.predict_rewards(kind, feats)
    meta["expected_rewards"] = expected
    usable = {c: r for c, r in expected.items() if r is not None}
    if not usable:
        return fallback_category, meta

    if rng.random() < epsilon:
        category = rng.choice(list(usable.keys()))
        meta["source"] = "explore"
        meta["explored"] = True
        return category, meta

    trustworthy = {c: r for c, r in usable.items() if model.is_validated_and_better(kind, c)}
    if not trustworthy:
        meta["reason"] = "no_validated_action_beats_baseline"
        return fallback_category, meta

    # 【修正1】「変動なし」は報酬が常に0以下で検証に通りやすく、これだけが合格した場合に
    # 全銘柄が見送りになっていた。判断は売買系(値上がり/値下がり)の合格行動だけで行い、
    # 売買系が1つも合格していなければルール予想にフォールバックする。
    directional = {c: r for c, r in trustworthy.items() if c != "変動なし"}
    if not directional:
        meta["reason"] = "no_validated_directional_action"
        return fallback_category, meta

    best_cat, best_reward = max(directional.items(), key=lambda kv: kv[1])
    if best_reward <= _edge_threshold(best_cat):
        # 売買系の合格行動がどれも期待値不足 → ノーポジション(変動なし)。
        meta["source"] = "model_abstain"
        meta["reason"] = "best_expected_reward_below_min_edge"
        return "変動なし", meta

    meta["source"] = "model"
    meta["trustworthy_actions"] = list(directional.keys())
    return best_cat, meta