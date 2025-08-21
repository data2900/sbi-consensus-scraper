SBI Financial Data Scraper

SBI証券の「四季報・財務情報」ページから成長率・利益率・ROE/ROA・自己資本比率・配当性向などの主要指標を自動収集する Python スクリプトです。

Scrapyベースのリンク収集 (consensus_url) を利用し、ログイン後に Selenium + Playwright を組み合わせて安定的に情報を取得・SQLiteに保存します。

⚠️ 本スクリプトは教育・研究目的で公開しています。
金融機関へのログインを伴う処理は、利用規約に抵触する可能性があります。必ず自己責任のもとで、個人利用に限定してください。

⸻

🔧 機能概要
	•	Selenium (GUI)
	•	ログインフォーム自動入力
	•	デバイス認証などは人手で実施
	•	Enter 押下で自動終了し、Cookie を Playwright に引き継ぎ
	•	Playwright (ヘッドレス)
	•	認証済み Cookie を使用し高速スクレイピング
	•	四季報タブを自動クリック
	•	成長率・利益率・ROE/ROA・自己資本比率・配当性向などを抽出
	•	QPS 制御・小並列での polite 収集
	•	SQLite 保存
	•	sbi_reports テーブルに以下を格納
	•	sales_growth, op_profit_growth, op_margin, roe, roa, equity_ratio, dividend_payout
	•	consensus_url テーブルを参照し、全件／未取得のみを選択可能

⸻

🧩 使用技術
	•	Python 3.9 以上推奨
	•	Selenium（ログイン操作）
	•	Playwright（非同期スクレイピング）
	•	SQLite3（データ保存）
	•	asyncio / TokenBucket（並列制御とQPS管理）

⸻

⚠️ 注意事項・免責
	•	本スクリプトは SBI証券ログインを伴います。
認証情報は環境変数や .env ファイルで安全に管理してください。
	•	ID・パスワードをソースコードに直書きしないこと。
	•	対象サイトの利用規約・robots.txt を必ず遵守してください。
	•	本コードは学習・検証目的に限って利用し、商用利用・自動運用は禁止します。
	•	作者は本スクリプト使用により生じたいかなる損害にも責任を負いません。

⸻

🗓 更新履歴
 	•	2025/07/17 - 初期版（コンセンサス情報リンク収集）
  •	2025/07/31 - SQLite 対応（CSV管理から移行）
	•	2025/08/21 - Selenium+Playwright 組み合わせによる本格収集版を実装
