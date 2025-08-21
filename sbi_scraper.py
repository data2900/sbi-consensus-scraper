# sbi_scraper.py
# Selenium(GUI)でログイン→人手でデバイス認証→Enterで続行＆Selenium自動終了→Playwrightで丁寧＆高速収集
# - ブラウザは必ず立ち上げ（Selenium: headless不使用, detach=False）
# - ログインは自動入力＆送信、その後は人手でデバイス認証（ワンタイム等）
# - 認証完了後に Enter 押下 → Seleniumは自動で閉じる → Playwrightで収集開始
# - 四季報タブがあれば毎回クリックして開く
# - 礼儀: 小並列(既定2) + 全体QPS制御(既定0.7) + 画像/フォント遮断 + リトライ + WAL + バッチコミット
# - 1本で all / missing 対応

import os
import sys
import time
import random
import sqlite3
import argparse
import datetime
from typing import Dict, Any, List, Tuple, Optional
from contextlib import asynccontextmanager

# --- Selenium (GUI) for login ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Playwright for fast scraping ---
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ====== 認証（直書き：取り扱いに注意）======
USER_ID = "個別のID"
PASSWORD = "個別のパスワード"

# ====== 定数 ======
DB_PATH = "/market_data.db"

DEFAULT_CONCURRENCY = 2      # 小並列（2〜3推奨）
DEFAULT_QPS = 0.7            # 全体QPS（0.6〜0.9推奨）
DEFAULT_BATCH = 100          # DBコミット間隔
NAV_TIMEOUT_MS = 25000       # Playwright ナビゲーション/待機
SEL_NAV_TIMEOUT = 25         # Selenium ページロードタイムアウト(秒)
RETRIES = 3                  # 最大リトライ
BASE_DELAY = 0.8             # バックオフ初期
DEFAULT_LOGIN_WAIT = 60      # 自動ログイン後、認証の目安秒（任意）

LOGIN_URL = ("https://www.sbisec.co.jp/login")

# 四季報タブ（あれば開く）
XPATH_SHIKIHO_TAB = '//*[@id="clmSubArea"]/div[2]/div/div[1]/ul/li[2]/a'

# 取得XPaths（SBI 四季報エリア）
XPATH_MAP: Dict[str, str] = {
    "sales_growth":      '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[1]/td[2]/p/span',
    "op_profit_growth":  '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[2]/td[2]/p/span',
    "op_margin":         '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[3]/td[2]/p',
    "roe":               '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[4]/td[2]/p',
    "roa":               '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[5]/td[2]/p',
    "equity_ratio":      '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[6]/td[2]/p',
    "dividend_payout":   '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[7]/td[2]/p',
}

# ========= ユーティリティ =========
def pct(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "N/A"
    return s if s.endswith("%") else (s + "%")

def polite_sleep_for_qps(qps: float):
    min_interval = 1.0 / max(qps, 0.01)
    time.sleep(min_interval * (1.0 + random.uniform(-0.15, 0.15)))

# ========= DB =========
def ensure_tables(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    cur.execute("""
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
    conn.commit()

def resolve_target_date(conn: sqlite3.Connection, explicit: Optional[str]) -> Optional[str]:
    if explicit:
        datetime.datetime.strptime(explicit, "%Y%m%d")
        return explicit
    cur = conn.cursor()
    cur.execute("SELECT MAX(target_date) FROM consensus_url")
    row = cur.fetchone()
    td = row[0] if row else None
    if td:
        datetime.datetime.strptime(td, "%Y%m%d")
        return td
    return None

def load_targets(conn: sqlite3.Connection, target_date: str, mode: str) -> List[Tuple[str, str]]:
    cur = conn.cursor()
    if mode == "all":
        cur.execute("SELECT code, sbiurl FROM consensus_url WHERE target_date = ?", (target_date,))
        return [(c, u) for c, u in cur.fetchall() if u]
    # missing: nikkei を母集合、sbi_reports 未保存のみ
    cur.execute("""
        SELECT code FROM nikkei_reports WHERE target_date = ?
        EXCEPT
        SELECT code FROM sbi_reports WHERE target_date = ?
    """, (target_date, target_date))
    codes = [r[0] for r in cur.fetchall()]
    if not codes:
        return []
    ph = ",".join(["?"] * len(codes))
    cur.execute(f"""
        SELECT code, sbiurl FROM consensus_url
        WHERE target_date = ? AND code IN ({ph})
    """, [target_date] + codes)
    return [(c, u) for c, u in cur.fetchall() if u]

# ========= Selenium (GUI) for login =========
def build_selenium() -> Tuple[webdriver.Chrome, str]:
    opts = Options()
    # GUI強制（headless不使用）
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_experimental_option("detach", False)  # ← Enter後に自動で閉じられるよう False
    # 自動化痕跡を軽減（完全ではない）
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    # DOMContentLoaded で復帰
    opts.page_load_strategy = "eager"
    ua = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"user-agent={ua}")
    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_page_load_timeout(SEL_NAV_TIMEOUT)
    return driver, ua

def sbi_login_auto(driver: webdriver.Chrome, wait_seconds: int):
    driver.get(LOGIN_URL)
    try:
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "imedisable"))).clear()
        driver.find_element(By.ID, "imedisable").send_keys(USER_ID)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, '//*[@id="password_input"]/input'))).clear()
        driver.find_element(By.XPATH, '//*[@id="password_input"]/input').send_keys(PASSWORD)
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="login"]/ul/li[1]/p[2]/button'))
        ).click()
        print("🔐 ログイン送信済み。デバイス認証を完了してください。")
    except Exception as e:
        print(f"❌ ログイン操作失敗: {e}")
        return

    if wait_seconds > 0:
        time.sleep(wait_seconds)

def export_cookies_for_playwright(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    raw = driver.get_cookies()
    cookies: List[Dict[str, Any]] = []
    for c in raw:
        cookies.append({
            "name": c.get("name"),
            "value": c.get("value"),
            "domain": c.get("domain") or ".sbisec.co.jp",
            "path": c.get("path", "/"),
            "expires": c.get("expiry", -1),
            "httpOnly": bool(c.get("httpOnly", False)),
            "secure": bool(c.get("secure", True)),
            "sameSite": "Lax",
        })
    return cookies

# ========= Playwright scraping =========
class TokenBucket:
    def __init__(self, qps: float):
        self.interval = 1.0 / max(qps, 0.0001)
        self._lock = asyncio.Lock()
        self._next = time.monotonic()
    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self.interval

@asynccontextmanager
async def playwright_context(play, user_agent: str, seed_cookies: List[Dict[str, Any]]):
    browser = await play.chromium.launch(
        headless=True,  # 収集はヘッドレスで軽量化（Selenium側はGUIで起動済み）
        args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"]
    )
    context = await browser.new_context(
        user_agent=user_agent,
        java_script_enabled=True,
        bypass_csp=True,
        viewport={"width": 1366, "height": 768}
    )
    # 軽量化: 画像/フォント/メディア遮断
    async def route_handler(route, request):
        if request.resource_type in ("image", "media", "font"):
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", route_handler)

    if seed_cookies:
        try:
            await context.add_cookies(seed_cookies)
        except Exception:
            sbisec = [c for c in seed_cookies if "sbisec" in (c.get("domain") or "")]
            if sbisec:
                await context.add_cookies(sbisec)

    try:
        yield context
    finally:
        await context.close()
        await browser.close()

async def fetch_one_pw(page, code: str, url: str) -> Tuple[str, Dict[str, Any]]:
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
    # 四季報タブ：存在すればクリック（失敗は無視）
    try:
        btn = await page.wait_for_selector(f"xpath={XPATH_SHIKIHO_TAB}", timeout=3000)
        if btn:
            await btn.click()
            await asyncio.sleep(0.5)
    except Exception:
        pass

    data = await page.evaluate(
        """(xps) => {
            const get = (xp) => {
              try { return document.evaluate(xp, document, null, XPathResult.STRING_TYPE, null).stringValue.trim(); }
              catch(e){ return ""; }
            };
            const out = {};
            for (const [k, xp] of Object.entries(xps)) out[k] = get(xp);
            return out;
        }""",
        XPATH_MAP
    )
    for k in list(data.keys()):
        data[k] = pct(data.get(k, ""))
    return code, data

async def worker(ctx, jobs: asyncio.Queue, bucket: TokenBucket, results: asyncio.Queue):
    page = await ctx.new_page()
    try:
        while True:
            item = await jobs.get()
            if item is None:
                break
            code, url = item
            await bucket.acquire()
            delay = BASE_DELAY
            last_err = None
            for attempt in range(RETRIES):
                try:
                    c, d = await fetch_one_pw(page, code, url)
                    await results.put((c, d, None))
                    break
                except (PwTimeout, Exception) as e:
                    last_err = e
                    if attempt < RETRIES - 1:
                        await asyncio.sleep(delay); delay *= 1.8
                    else:
                        await results.put((code, None, last_err))
            jobs.task_done()
    finally:
        await page.close()

async def run_playwright_scrape(targets: List[Tuple[str,str]], ua: str, cookies: List[Dict[str, Any]],
                                target_date: str, qps: float, concurrency: int, batch: int):
    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    cur = conn.cursor()

    jobs: asyncio.Queue = asyncio.Queue()
    results: asyncio.Queue = asyncio.Queue()
    for t in targets:
        await jobs.put(t)
    total = len(targets)
    done = ok = ng = 0
    buf: List[Tuple] = []
    bucket = TokenBucket(qps)

    async with async_playwright() as play:
        async with playwright_context(play, ua, cookies) as ctx:
            workers = [asyncio.create_task(worker(ctx, jobs, bucket, results))
                       for _ in range(max(1, min(concurrency, 6)))]

            async def stop_workers():
                for _ in workers:
                    await jobs.put(None)

            try:
                while done < total:
                    code, data, err = await results.get()
                    done += 1
                    if err is None and data:
                        row = (
                            target_date, code,
                            data.get("sales_growth",""), data.get("op_profit_growth",""),
                            data.get("op_margin",""), data.get("roe",""), data.get("roa",""),
                            data.get("equity_ratio",""), data.get("dividend_payout","")
                        )
                        buf.append(row); ok += 1
                        if len(buf) >= batch:
                            cur.executemany("""
                                INSERT OR REPLACE INTO sbi_reports (
                                    target_date, code, sales_growth, op_profit_growth,
                                    op_margin, roe, roa, equity_ratio, dividend_payout
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, buf)
                            conn.commit(); buf.clear()
                    else:
                        ng += 1
                    if done % 50 == 0 or done == total:
                        print(f"✅ {done}/{total} / OK:{ok} NG:{ng}")
            finally:
                if buf:
                    cur.executemany("""
                        INSERT OR REPLACE INTO sbi_reports (
                            target_date, code, sales_growth, op_profit_growth,
                            op_margin, roe, roa, equity_ratio, dividend_payout
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, buf)
                    conn.commit()
                await stop_workers()
                await asyncio.gather(*workers, return_exceptions=True)
                conn.close()
                print(f"🏁 完了 / OK:{ok} NG:{ng} / 対象:{total}")

# ========= メイン（Seleniumでログイン→Enterで閉じる→Playwrightで収集）=========
def main():
    p = argparse.ArgumentParser()
    p.add_argument("-a", "--target_date", help="YYYYMMDD（未指定なら consensus_url の最新日付）")
    p.add_argument("--mode", choices=["all", "missing"], default="missing",
                   help="all: consensus_url 全件 / missing: nikkei基準で未取得のみ")
    p.add_argument("--qps", type=float, default=DEFAULT_QPS, help="全体QPS上限（0.6〜0.9推奨）")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Playwright並列（2〜3推奨）")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="DBコミット間隔")
    p.add_argument("--login-wait", type=int, default=DEFAULT_LOGIN_WAIT, help="自動ログイン後に待つ秒数（認証目安）")
    args = p.parse_args()

    conn = sqlite3.connect(DB_PATH)
    ensure_tables(conn)
    target_date = resolve_target_date(conn, args.target_date)
    if not target_date:
        print("❌ target_date を決定できません（-a YYYYMMDD を指定するか、consensus_url にデータが必要）")
        conn.close(); sys.exit(1)

    targets = load_targets(conn, target_date, args.mode)
    if not targets:
        if args.mode == "missing":
            print(f"✅ {target_date} 未取得はありません（mode=missing）")
        else:
            print(f"⚠️ {target_date} 対象URLが見つかりません（mode=all）")
        conn.close(); return
    conn.close()

    # 1) Selenium(GUI) でログイン→デバイス認証（Enterで続行＆自動クローズ）
    print("🌐 Chrome(GUI) を起動してログインします。")
    driver, ua = build_selenium()
    try:
        sbi_login_auto(driver, wait_seconds=args.login_wait)
        input("⏸ 認証が完了し会員ページが開ける状態になったら Enter を押してください… ")

        # 認証後の Cookie を Playwright へ移植
        cookies = export_cookies_for_playwright(driver)
    finally:
        # ← Enterの後は**必ず**Seleniumブラウザを自動で閉じる
        try:
            driver.quit()
        except Exception:
            pass

    if not cookies:
        print("⚠️ Cookie を取得できませんでした。認証が完了していない可能性があります。続行は可能ですが失敗する場合があります。")

    # 2) Playwright で丁寧に高速取得（小並列＋QPS制御）
    print(f"▶ 取得開始: mode={args.mode} date={target_date} / concurrency={args.concurrency} qps={args.qps}")
    asyncio.run(run_playwright_scrape(
        targets=targets,
        ua=ua,
        cookies=cookies,
        target_date=target_date,
        qps=args.qps,
        concurrency=args.concurrency,
        batch=args.batch
    ))

if __name__ == "__main__":
    main()
