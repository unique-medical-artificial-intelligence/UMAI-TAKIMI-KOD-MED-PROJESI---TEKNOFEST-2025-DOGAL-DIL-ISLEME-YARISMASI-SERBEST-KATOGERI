# Selenium ve ilgili paketleri kuran kod
!pip install selenium

# Chrome sürücüsünü ve tarayıcısını kurmak için gerekli diğer araçlar
!apt-get update
!apt install -y chromium-chromedriver
!cp /usr/lib/chromium-browser/chromedriver /usr/bin

# Colab'da Selenium'un sorunsuz çalışması için gerekli ayarlar
import sys
sys.path.insert(0,'/usr/lib/chromium-browser/chromedriver')
from selenium import webdriver
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless') # Tarayıcının arayüzünü göstermeden arka planda çalıştırır
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

print("Selenium ve Chrome Driver kurulumu tamamlandı.")
