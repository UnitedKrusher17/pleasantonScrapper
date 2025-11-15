from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("web-scrapper-478302-acbf7cccf033.json", scope)
client = gspread.authorize(creds)
sheet = client.open("Pleasanton Business Directory").sheet1

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
driver.get("https://business.pleasanton.org/list")
wait = WebDriverWait(driver, 20)
time.sleep(3)

category_links = list({a.get_attribute("href") for a in driver.find_elements(By.TAG_NAME, "a") if a.get_attribute("href") and "ql/" in a.get_attribute("href")})
business_links = set()
for link in category_links:
    driver.get(link)
    time.sleep(2)
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    business_links.update({a.get_attribute("href") for a in driver.find_elements(By.TAG_NAME, "a") if a.get_attribute("href") and "/member/" in a.get_attribute("href")})
driver.quit()

def scrape_business(link):
    try:
        local_driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        local_wait = WebDriverWait(local_driver, 10)
        local_driver.get(link)
        time.sleep(1)
        name = local_wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1"))).text
        try:
            industry = local_driver.find_element(By.XPATH, "//p[contains(text(),'Industry') or contains(text(),'Category')]").text
        except:
            industry = ""
        try:
            address = local_driver.find_element(By.XPATH, "//p[contains(@class,'address')]").text
        except:
            address = ""
        try:
            phone = local_driver.find_element(By.XPATH, "//a[starts-with(@href,'tel:')]").text
        except:
            phone = ""
        try:
            website = local_driver.find_element(By.XPATH, "//a[starts-with(@href,'http') and not(starts-with(@href,'tel:')) and not(starts-with(@href,'mailto:'))]").get_attribute("href")
        except:
            website = ""
        try:
            email = local_driver.find_element(By.XPATH, "//a[starts-with(@href,'mailto:')]").get_attribute("href").replace("mailto:", "")
        except:
            email = ""
        try:
            description = local_driver.find_element(By.XPATH, "//p[contains(@class,'description')]").text
        except:
            description = ""
        local_driver.quit()
        return [name, industry, address, website, phone, email, description]
    except:
        return ["", "", "", "", "", "", ""]

data = []
with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [executor.submit(scrape_business, link) for link in business_links]
    for result in tqdm(futures, desc="Scraping businesses", leave=True):
        data.append(result.result())

headers = ["Name", "Industry", "Address", "Website", "Phone", "Email", "Description"]
sheet.clear()
sheet.update(values=[headers] + data, range_name="A1")
print(f"{len(data)} businesses exported to Google Sheets")
