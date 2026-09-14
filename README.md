# Web閲覧ページ(GitHub Pages、無料)セットアップ手順

## 全体構成

```
リポジトリ/
├── us_stock_scanner_webull_openapi.py   ← 修正版(JSON出力機能を追加済み)
├── data/                                 ← スキャン結果のJSON(自動生成・自動コミット)
│   ├── index.json
│   ├── scan_20260914_073000.json
│   └── ...
├── docs/
│   └── index.html                        ← 閲覧ページ本体(GitHub Pagesで公開)
└── .github/workflows/scan.yml            ← 定期実行 + 自動コミット
```

## 手順

1. **リポジトリ作成**
   GitHubで新規リポジトリを作成し、上記のファイル一式(このチャットで生成した
   `us_stock_scanner_webull_openapi.py` / `docs/index.html` /
   `.github/workflows/scan.yml`)をpushしてください。

2. **Secretsの登録**
   リポジトリの Settings → Secrets and variables → Actions で以下を登録:
   - `WEBULL_APP_KEY`
   - `WEBULL_APP_SECRET`
   - `GMAIL_USER` / `GMAIL_APP_PASSWORD` / `GMAIL_TO`(メール送信を使う場合。
     使わない場合は登録不要 — `send_email`が失敗してもJSON保存とワークフローは継続します)

3. **GitHub Pagesを有効化**
   Settings → Pages → Source を「Deploy from a branch」、
   Branch を `main` / フォルダを `/docs` に設定。
   数分後に `https://harukana2.github.io/kabuserver/` で閲覧ページが公開されます。

4. **動作確認**
   Actions タブから `US Stock Scan` ワークフローを「Run workflow」で手動実行し、
   - `data/` にJSONが追加されること
   - 閲覧ページで日付選択・ソートができること
   を確認してください。以降は `scan.yml` のcron設定に従って自動実行されます
   (デフォルトは平日 日本時間7:30。`cron`の値を編集すれば変更可能)。

## 閲覧ページでできること

- 上部のドロップダウンで過去の実行日時を選択 → 過去のスキャン結果を表示
- タブで「デイトレード候補」「長期・成長期待候補」「主要企業」を切替
- 列見出しをクリックでソート(利益期待スコア順・PER順など)
- 銘柄フィルタ欄でシンボル絞り込み

## 運用上の注意

- `MAX_SNAPSHOTS_KEPT`(スクリプト冒頭、デフォルト90件)を超えた古いJSONは
  自動的に削除されます。増やすとリポジトリ容量が増えるので、必要に応じて調整してください。
- GitHub Pagesは静的ファイルのみなので、認証やアクセス制限はできません
  (URLを知っていれば誰でも閲覧可能)。非公開にしたい場合はリポジトリを
  Privateにした上でGitHub Pagesの代わりにNetlify/Vercelの認証機能付き無料プランを
  検討してください。
