import scrapy
import sqlite3
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ユーザID・パスワード（必要に応じて .env に移すべき）
USER_ID = ID"
PASSWORD = "PASSWORD"
LOGIN_URL = "https://www.sbisec.co.jp/ETGate/login"

# データベースパス
DB_PATH = "/market_data.db"

# XPath対応マップ
XPATH_MAP = {
    "sales_growth": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[1]/td[2]/p/span',
    "op_profit_growth": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[2]/td[2]/p/span',
    "op_margin": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[3]/td[2]/p',
    "roe": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[4]/td[2]/p',
    "roa": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[5]/td[2]/p',
    "equity_ratio": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[6]/td[2]/p',
    "dividend_payout": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[7]/td[2]/p'
}

class SBISpider(scrapy.Spider):
    name = "sbi_scraper"

    def __init__(self, target_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not target_date:
            raise ValueError("実行時に -a target_date=YYYYMMDD の形式で日付を指定してください")
        self.target_date = target_date
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()
        self.init_db()
        self.setup_driver()

    def init_db(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS sbi_reports (
                target_date TEXT,
                code TEXT,
                sales_growth TEXT,
                op_profit_growth TEXT,
                op_margin TEXT,
                roe TEXT,
                roa TEXT,
                equity_ratio TEXT,
                dividend_payout TEXT,
                PRIMARY KEY (target_date, code)
            )
        """)
        self.conn.commit()

    def setup_driver(self):
        chrome_options = Options()
        # chrome_options.add_argument("--headless")  # 必要なら有効化
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
        self.driver = webdriver.Chrome(service=Service(), options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)
        self.short_wait = WebDriverWait(self.driver, 2)

    def start_requests(self):
        # URL取得
        self.cursor.execute("""
            SELECT code, sbiurl FROM consensus_url WHERE target_date = ?
        """, (self.target_date,))
        rows = self.cursor.fetchall()

        if not rows:
            print(f"❌ 対象日 {self.target_date} のURLが見つかりません")
            return

        # ログイン処理
        self.login()

        for i, (code, url) in enumerate(rows):
            yield scrapy.Request(
                url=url,
                callback=self.parse_sbi,
                meta={'code': code, 'url': url, 'index': i + 1, 'total': len(rows)}
            )

    def login(self):
        self.driver.get(LOGIN_URL)
        self.wait.until(EC.presence_of_element_located((By.ID, "imedisable"))).send_keys(USER_ID)
        self.wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="password_input"]/input'))).send_keys(PASSWORD)
        self.wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="login"]/ul/li[1]/p[2]/button'))).click()

        print("🔐 デバイス認証待機中 (30秒)...")
        time.sleep(30)

    def parse_sbi(self, response):
        code = response.meta['code']
        url = response.meta['url']
        index = response.meta['index']
        total = response.meta['total']

        print(f"[{index}/{total}] アクセス中: {url}")
        self.driver.get(url)
        time.sleep(random.uniform(2, 4))

        # 初回だけ四季報クリック
        if index == 1:
            try:
                shikiho_btn = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="clmSubArea"]/div[2]/div/div[1]/ul/li[2]/a')))
                shikiho_btn.click()
                time.sleep(1)
            except Exception as e:
                print("⚠️ 四季報ボタン失敗:", e)

        data = {
            "target_date": self.target_date,
            "code": code
        }

        for col, xpath in XPATH_MAP.items():
            try:
                element = self.short_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                val = element.text.strip()
                if not val.endswith("%"):
                    val += "%"
                data[col] = val
            except:
                data[col] = "N/A"
                print(f"⚠️ {col} 取得失敗")

        self.cursor.execute("""
            INSERT OR REPLACE INTO sbi_reports (
                target_date, code, sales_growth, op_profit_growth,
                op_margin, roe, roa, equity_ratio, dividend_payout
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["target_date"], data["code"], data["sales_growth"], data["op_profit_growth"],
            data["op_margin"], data["roe"], data["roa"], data["equity_ratio"], data["dividend_payout"]
        ))

        self.conn.commit()

    def closed(self, reason):
        print("🔚 ブラウザとDBを閉じます")
        self.driver.quit()
        self.conn.close()
