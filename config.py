"""إعدادات بوت نور الفرقان"""
import os

BOT_TOKEN = "8028513728:AAFkh1k6RxiMUm1gPqqr-m8DOzh2lnajKHk"
ADMIN_IDS = [8147097184, 6380638265]
ADMIN_ID = 8147097184

# Firebase configuration is handled via serviceAccountKey.json
# Place your serviceAccountKey.json in the project root directory.

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

BOT_USERNAME_DISPLAY = "نور الفرقان"

# API URLs
ALADHAN_API = "https://api.aladhan.com/v1/timingsByCity"
QURAN_API = "https://api.quran.com/api/v4"
MP3QURAN_API = "https://mp3quran.net/api/v3"