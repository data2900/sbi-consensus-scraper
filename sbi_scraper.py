# sbi_scraper.py

import pandas as pd
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- 外部設定ファイルから読み込み ---
try:
    from config import SBI_USER_ID, SBI_PASSWORD, LOGIN_URL, CSV_PATH
except ImportError:
    raise ImportError("❗ config.py が見つかりません。必要な認証情報とパスを記載してください。")

# --- 出力ファイル名 ---
now_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
OUTPUT_BASENAME = f"data_{now_str}"
CSV_OUTPUT_NAME = OUTPUT_BASENAME + ".csv"
CSV_PARTIAL_NAME = OUTPUT_BASENAME + "_partial.csv"

# --- Chromeオプション ---
options = Options()
# options.add_argument("--headless")  # ヘッドレスモードで実行する場合は有効化
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service(), options=options)
wait = WebDriverWait(driver, 20)
short_wait = WebDriverWait(driver, 1)

# --- XPath定義 ---
xpath_map = {
    "増収率": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[1]/td[2]/p/span',
    "経常増益率": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[2]/td[2]/p/span',
    "売上高経常利益率": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[3]/td[2]/p',
    "ROE": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[4]/td[2]/p',
    "ROA": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[5]/td[2]/p',
    "株主資本比率": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[6]/td[2]/p',
    "配当性向": '//*[@id="clmMainArea"]/div[12]/table/tbody/tr[7]/td[2]/p'
}

# --- CSV読み込み ---
df = pd.read_csv(CSV_PATH)
df = df.dropna(subset=[df.columns[4]])  # URL列が空でない行のみ対象
codes = df.iloc[:, 0].tolist()
urls = df.iloc[:, 4].tolist()

# --- 途中結果格納用リスト ---
results = []

# --- ログイン処理 ---
try:
    driver.get(LOGIN_URL)
    wait.until(EC.presence_of_element_located((By.ID, "imedisable"))).send_keys(SBI_USER_ID)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="password_input"]/input'))).send_keys(SBI_PASSWORD)
    wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="login"]/ul/li[1]/p[2]/button'))).click()

    print("🔐 デバイス認証待ち：30秒")
    time.sleep(30)

    for i, (code, url) in enumerate(zip(codes, urls)):
        print(f"[{i+1}/{len(urls)}] アクセス中：{url}")
        driver.get(url)
        time.sleep(random.uniform(2, 4))

        if i == 0:
            try:
                print("📘 初回：四季報ボタンをクリック")
                shikiho_btn = wait.until(EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="clmSubArea"]/div[2]/div/div[1]/ul/li[2]/a')))
                shikiho_btn.click()
                time.sleep(1)
            except Exception as e:
                print(f"⚠️ 四季報ボタンクリック失敗: {e}")

        # --- データ取得 ---
        data = {"証券コード": code}
        for label, xpath in xpath_map.items():
            try:
                element = short_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                data[label] = element.text.strip()
            except Exception:
                data[label] = "N/A"
                print(f"⚠️ {label} の取得失敗")

        results.append(data)

    # --- 成功時保存 ---
    result_df = pd.DataFrame(results)
    result_df = result_df[["証券コード"] + list(xpath_map.keys())]
    result_df.to_csv(CSV_OUTPUT_NAME, index=False)
    print(f"✅ データ抽出完了: {CSV_OUTPUT_NAME}")

except Exception as e:
    print(f"❌ エラー発生: {e}")

finally:
    # --- 中断時も保存 ---
    if results:
        result_df = pd.DataFrame(results)
        result_df = result_df[["証券コード"] + list(xpath_map.keys())]
        result_df.to_csv(CSV_PARTIAL_NAME, index=False)
        print(f"💾 中断時データ保存: {CSV_PARTIAL_NAME}")
    driver.quit()