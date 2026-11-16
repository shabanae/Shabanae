# Shabanae - برنامج تحميل الملفات
import os
import re
import sys
import time
import shutil
import hashlib
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, unquote

import requests
import yt_dlp

try:
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QGridLayout, QLineEdit,
        QPushButton, QLabel, QComboBox, QCheckBox, QSlider, QSpinBox,
        QProgressBar, QTextEdit, QFileDialog, QMessageBox, QGroupBox)
    from PyQt6.QtCore import QThread, pyqtSignal, Qt
    QT_MAJOR = 6
except ImportError:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QGridLayout, QLineEdit,
        QPushButton, QLabel, QComboBox, QCheckBox, QSlider, QSpinBox,
        QProgressBar, QTextEdit, QFileDialog, QMessageBox, QGroupBox)
    from PyQt5.QtCore import QThread, pyqtSignal, Qt
    QT_MAJOR = 5


APP_NAME = "Shabanae"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
CHUNK = 64 * 1024
RETRY_STATUS = {429, 500, 502, 503, 504}
BACKOFF = [1, 2, 4, 8]
MAX_RETRIES = len(BACKOFF)
MIN_SEGMENTED_SIZE = 2 * 1024 * 1024

MODES = [
    ("تحميل مباشر من رابط ملف", "direct"),
    ("تحميل من موقع فيديو أو صوت", "media"),
    ("تقسيم ملف فيديو محلي", "split"),
    ("تحويل ملف فيديو محلي إلى صوت", "audio"),
]


class Cancelled(Exception):
    pass


class FatalError(Exception):
    pass


# ---------- بيئة التشغيل ----------

def windows_name():
    if sys.platform != "win32":
        return sys.platform
    v = sys.getwindowsversion()
    if (v.major, v.minor) == (6, 1):
        return "Windows 7"
    if v.major == 6 and v.minor in (2, 3):
        return "Windows 8/8.1"
    return f"Windows {v.major}"


def base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def find_ffmpeg():
    for name in ("ffmpeg.exe", "ffmpeg"):
        local = os.path.join(base_dir(), name)
        if os.path.exists(local):
            return local
    return shutil.which("ffmpeg")


def hidden_flags():
    return subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0


# ---------- أدوات مساعدة ----------

def sanitize(name):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(" .")
    return name or "download.bin"


def safe_filename(folder, name):
    """يضيف رقماً إذا كان الملف موجوداً حتى لا يتم الكتابة فوقه."""
    base, ext = os.path.splitext(sanitize(name))
    path = os.path.join(folder, base + ext)
    n = 1
    while os.path.exists(path):
        path = os.path.join(folder, f"{base} ({n}){ext}")
        n += 1
    return path


def parse_time(text):
    total = 0.0
    for part in text.strip().split(":"):
        total = total * 60 + float(part)
    return total


def parse_sections(text):
    """يحول نص مثل '0-300, 00:05:00-00:10:00' إلى قائمة أزواج بالثواني."""
    sections = []
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        start, end = item.split("-", 1)
        sections.append((parse_time(start), parse_time(end)))
    return sections


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


class RateLimiter:
    """خوارزمية Token Bucket لحد سرعة مشترك بين كل الاتصالات. rate بالبايت في الثانية، 0 يعني بدون حد."""

    def __init__(self, rate):
        self.lock = threading.Lock()
        self.set_rate(rate)
        self.tokens = self.capacity
        self.updated = time.monotonic()

    def set_rate(self, rate):
        with getattr(self, "lock", threading.Lock()):
            self.rate = rate
            self.capacity = max(rate, 1)

    def acquire(self, n):
        if self.rate <= 0:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                need = min(n, self.capacity)
                if self.tokens >= need:
                    self.tokens -= need
                    return
                wait = (need - self.tokens) / self.rate
            time.sleep(min(wait, 0.5))


class Tracker:
    """يحسب نسبة التقدم ويرسلها إلى الواجهة."""

    def __init__(self, total, callback):
        self.total = total
        self.done = 0
        self.callback = callback
        self.last = -1
        self.lock = threading.Lock()

    def add(self, n):
        with self.lock:
            self.done += n
            if self.total:
                pct = min(100, int(self.done * 100 / self.total))
                if pct != self.last:
                    self.last = pct
                    self.callback(pct)


def make_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "*/*"})
    return s


def check_status(r):
    if r.status
