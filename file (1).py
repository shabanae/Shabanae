"""Shabanae - file, media and video downloader.
Windows 10/11: PyQt6 (Python 3.12+). Windows 7/8/8.1: PyQt5 (Python 3.8).
"""
import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import requests

APP_NAME = "Shabanae"


# ---------- Qt selection based on Windows version ----------
def windows_major():
    if sys.platform != "win32":
        return 10
    try:
        return sys.getwindowsversion().major
    except AttributeError:
        return 10


USE_QT6 = windows_major() >= 10
try:
    if USE_QT6:
        from PyQt6.QtCore import Qt, QThread, pyqtSignal
        from PyQt6.QtWidgets import (QApplication, QComboBox, QFileDialog, QHBoxLayout,
                                     QLabel, QLineEdit, QMainWindow, QMessageBox,
                                     QProgressBar, QPushButton, QSlider, QTextEdit,
                                     QVBoxLayout, QWidget)
    else:
        raise ImportError
except ImportError:
    from PyQt5.QtCore import Qt, QThread, pyqtSignal
    from PyQt5.QtWidgets import (QApplication, QComboBox, QFileDialog, QHBoxLayout,
                                 QLabel, QLineEdit, QMainWindow, QMessageBox,
                                 QProgressBar, QPushButton, QSlider, QTextEdit,
                                 QVBoxLayout, QWidget)


# ---------- Shared speed limiter (token bucket) ----------
class RateLimiter:
    def __init__(self, bytes_per_sec=0):
        self.rate = bytes_per_sec
        self.tokens = float(bytes_per_sec)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def set_rate(self, bytes_per_sec):
        with self.lock:
            self.rate = bytes_per_sec
            self.tokens = float(bytes_per_sec)

    def consume(self, n):
        if self.rate <= 0:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                wait = (n - self.tokens) / self.rate
            time.sleep(min(wait, 0.5))


# ---------- Retry helper ----------
def get_with_retry(session, method, url, **kwargs):
    delays = [1, 2, 4, 8]
    for attempt in range(len(delays) + 1):
        r = session.request(method, url, timeout=30, **kwargs)
        if r.status_code == 429 or r.status_code >= 500:
            if attempt < len(delays):
                time.sleep(delays[attempt])
                continue
        r.raise_for_status()
        return r
    raise RuntimeError("retries exhausted")


# ---------- Direct downloads (parallel ranges) ----------
class DirectDownloader:
    def __init__(self, url, dest, connections=8, limiter=None, expected_sha256=None,
                 progress=None, log=None, cancel_event=None):
        self.url = url
        self.dest = dest
        self.connections = max(1, connections)
        self.limiter = limiter or RateLimiter(0)
        self.expected_sha256 = expected_sha256
        self.progress = progress or (lambda done, total: None)
        self.log = log or (lambda msg: None)
        self.cancel = cancel_event or threading.Event()
        self.session = requests.Session()
        self.done = 0
        self.lock = threading.Lock()

    def run(self):
        head = get_with_retry(self.session, "HEAD", self.url, allow_redirects=True)
        size = int(head.headers.get("Content-Length", 0))
        ranges_ok = head.headers.get("Accept-Ranges", "").lower() == "bytes" and size > 0
        if not ranges_ok or self.connections == 1:
            self.log("التحميل بخيط واحد (الخادم لا يدعم التجزئة)")
            self._single()
        else:
            self.log(f"التحميل بـ {self.connections} اتصالات، الحجم {size} بايت")
            self._parallel(size)
        if self.cancel.is_set():
            raise RuntimeError("أُلغي التحميل")
        if self.expected_sha256:
            self.log("جارٍ التحقق من SHA256...")
            if self.sha256() != self.expected_sha256.lower():
                raise RuntimeError("فشل التحقق من SHA256")
            self.log("التحقق من SHA256 ناجح")
        self.log(f"اكتمل: {self.dest}")

    def _write_stream(self, resp, f, start_offset=0):
        for chunk in resp.iter_content(64 * 1024):
            if self.cancel.is_set():
                return
            self.limiter.consume(len(chunk))
            f.seek(start_offset)
            f.write(chunk)
            start_offset += len(chunk)
            with self.lock:
                self.done += len(chunk)
                self.progress(self.done, self._total)

    def _single(self):
        resp = get_with_retry(self.session, "GET", self.url, stream=True)
        self._total = int(resp.headers.get("Content-Length", 0))
        with open(self.dest, "wb") as f:
            for chunk in resp.iter_content(64 * 1024):
                if self.cancel.is_set():
                    return
                self.limiter.consume(len(chunk))
                f.write(chunk)
                self.done += len(chunk)
                self.progress(self.done, self._total)

    def _parallel(self, size):
        self._total = size
        part = size // self.connections
        ranges = []
        for i in range(self.connections):
            start = i * part
            end = size - 1 if i == self.connections - 1 else start + part - 1
            ranges.append((start, end))
        with open(self.dest, "wb") as f:
            f.truncate(size)
        with ThreadPoolExecutor(max_workers=self.connections) as ex:
            futures = [ex.submit(self._fetch_part, s, e) for s, e in ranges]
            for fut in futures:
                fut.result()

    def _fetch_part(self, start, end):
        headers = {"Range": f"bytes={start}-{end}"}
        resp = get_with_retry(self.session, "GET", self.url, headers=headers, stream=True)
        with open(self.dest, "r+b") as f:
            f.seek(start)
            for chunk in resp.iter_content(64 * 1024):
                if self.cancel.is_set():
                    return
                self.limiter.consume(len(chunk))
                f.write(chunk)
                with self.lock:
                    self.done += len(chunk)
                    self.progress(self.done, self._total)

    def sha256(self):
        h = hashlib.sha256()
        with open(self.dest, "rb") as f:
            for block in iter(lambda: f.read(1024 * 1024), b""):
                h.update(block)
        return h.hexdigest()


# ---------- FFmpeg helpers ----------
def find_ffmpeg():
    local = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ffmpeg.exe")
    if os.path.exists(local):
        return local
    return shutil.which("ffmpeg")


def run_ffmpeg(args, log):
    ffmpeg = find_ffmpeg()
    if not ffmpeg:
        raise RuntimeError("لم يُعثر على ffmpeg. ضعه بجوار البرنامج أو أضفه إلى PATH")
    cmd = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error"] + args
    creation = 0x08000000 if sys.platform == "win32" else 0  # no console window
    result = subprocess.run(cmd, capture_output=True, text=True, creationflags=creation)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "فشل ffmpeg")
    log("ffmpeg: اكتملت العملية")


def split_local_video(src, seconds_per_part, log):
    base, ext = os.path.splitext(src)
    out_pattern = f"{base}_part%03d{ext}"
    run_ffmpeg(["-i", src, "-c", "copy", "-map", "0", "-f", "segment",
                "-segment_time", str(seconds_per_part), "-reset_timestamps", "1",
                out_pattern], log)


def convert_local_to_audio(src, fmt, log):
    codecs = {"mp3": ["-codec:a", "libmp3lame", "-q:a", "2"],
              "m4a": ["-codec:a", "aac", "-b:a", "192k"],
              "wav": ["-codec:a", "pcm_s16le"]}
    base = os.path.splitext(src)[0]
    out = f"{base}.{fmt}"
    run_ffmpeg(["-i", src, "-vn"] + codecs[fmt] + [out], log)
    return out


def parse_time(text):
    """Accepts seconds or HH:MM:SS / MM:SS."""
    parts = [float(p) for p in text.strip().split(":")]
    total = 0.0
    for p in parts:
        total = total * 60 + p
    return total


# ---------- Media downloads with yt-dlp ----------
def download_media(url, dest_dir, audio_only, audio_fmt, time_range, rate_bytes, log, progress):
    import yt_dlp
    from yt_dlp.utils import download_range_func

    opts = {
        "outtmpl": os.path.join(dest_dir, "%(title).150s.%(ext)s"),
        "noplaylist": True,
        "ratelimit": rate_bytes or None,
        "progress_hooks": [lambda d: progress(d)],
        "quiet": True,
        "no_warnings": True,
    }
    if audio_only:
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [{"key": "FFmpegExtractAudio",
                                   "preferredcodec": audio_fmt,
                                   "preferredquality": "192"}]
    else:
        opts["format"] = "bestvideo+bestaudio/best"
        opts["merge_output_format"] = "mp4"
    if time_range:
        start, end = time_range
        opts["download_ranges"] = download_range_func(None, [(start, end)])
        opts["force_keyframes_at_cuts"] = True
    log("بدء تحميل الوسائط...")
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])
    log("اكتمل تحميل الوسائط")


# ---------- Background worker ----------
class Worker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    done_signal = pyqtSignal(bool, str)

    def __init__(self, task, params, limiter):
        super().__init__()
        self.task = task
        self.params = params
        self.limiter = limiter
        self.cancel = threading.Event()

    def log(self, msg):
        self.log_signal.emit(msg)

    def run(self):
        try:
            p = self.params
            if self.task == "direct":
                def prog(done, total):
                    if total:
                        self.progress_signal.emit(int(done * 100 / total))
                d = DirectDownloader(p["url"], p["dest"], p["connections"], self.limiter,
                                     p.get("sha256"), prog, self.log, self.cancel)
                d.run()
            elif self.task == "media":
                def hook(d):
                    if d.get("status") == "downloading" and d.get("total_bytes"):
                        self.progress_signal.emit(int(d["downloaded_bytes"] * 100 / d["total_bytes"]))
                rate = int(self.limiter.rate) if self.limiter.rate else 0
                download_media(p["url"], p["dest_dir"], p["audio_only"], p["audio_fmt"],
                               p.get("time_range"), rate, self.log, hook)
            elif self.task == "split":
                split_local_video(p["path"], p["seconds"], self.log)
            elif self.task == "audio":
                out = convert_local_to_audio(p["path"], p["fmt"], self.log)
                self.log(f"الملف الناتج: {out}")
            self.progress_signal.emit(100)
            self.done_signal.emit(True, "تمت العملية بنجاح")
        except Exception as e:
            self.done_signal.emit(False, str(e))


# ---------- GUI ----------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} - Downloader")
        self.resize(720, 560)
        self.limiter = RateLimiter(0)
        self.worker = None

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("الرابط (مباشر أو فيديو من موقع مدعوم)")
        layout.addWidget(QLabel("الرابط:"))
        layout.addWidget(self.url_input)

        row = QHBoxLayout()
        self.dest_input = QLineEdit(os.path.expanduser("~/Downloads"))
        browse = QPushButton("اختيار المجلد")
        browse.clicked.connect(self.choose_folder)
        row.addWidget(self.dest_input)
        row.addWidget(browse)
        layout.addWidget(QLabel("مجلد الحفظ:"))
        layout.addLayout(row)

        row = QHBoxLayout()
        self.mode = QComboBox()
        self.mode.addItems(["ملف مباشر", "فيديو", "صوت فقط"])
        self.audio_fmt = QComboBox()
        self.audio_fmt.addItems(["mp3", "m4a", "wav"])
        self.conn = QComboBox()
        self.conn.addItems(["1", "4", "8", "16"])
        self.conn.setCurrentText("8")
        row.addWidget(QLabel("النوع:"))
        row.addWidget(self.mode)
        row.addWidget(QLabel("الصيغة الصوتية:"))
        row.addWidget(self.audio_fmt)
        row.addWidget(QLabel("الاتصالات:"))
        row.addWidget(self.conn)
        layout.addLayout(row)

        row = QHBoxLayout()
        self.range_input = QLineEdit()
        self.range_input.setPlaceholderText("مثال: 00:01:00-00:03:30 (اختياري)")
        self.sha_input = QLineEdit()
        self.sha_input.setPlaceholderText("SHA256 المتوقع (اختياري)")
        row.addWidget(self.range_input)
        row.addWidget(self.sha_input)
        layout.addLayout(row)

        row = QHBoxLayout()
        self.speed_label = QLabel("السرعة: غير محدودة")
        self.speed = QSlider(Qt.Orientation.Horizontal)
        self.speed.setRange(0, 100)
        self.speed.setValue(0)
        self.speed.valueChanged.connect(self.speed_changed)
        row.addWidget(self.speed_label)
        row.addWidget(self.speed)
        layout.addLayout(row)

        self.bar = QProgressBar()
        self.bar.setRange(0, 100)
        layout.addWidget(self.bar)

        btns = QHBoxLayout()
        self.start_btn = QPushButton("بدء التحميل")
        self.start_btn.clicked.connect(self.start_download)
        self.cancel_btn = QPushButton("إلغاء")
        self.cancel_btn.clicked.connect(self.cancel_task)
        split_btn = QPushButton("تقسيم ملف محلي")
        split_btn.clicked.connect(self.split_file)
        audio_btn = QPushButton("تحويل ملف محلي إلى صوت")
        audio_btn.clicked.connect(self.audio_file)
        for b in (self.start_btn, self.cancel_btn, split_btn, audio_btn):
            btns.addWidget(b)
        layout.addLayout(btns)

        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        layout.addWidget(self.log_box)

    # --- helpers ---
    def log(self, msg):
        self.log_box.append(msg)

    def choose_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "اختر المجلد", self.dest_input.text())
        if folder:
            self.dest_input.setText(folder)

    def speed_changed(self, value):
        if value == 0:
            self.limiter.set_rate(0)
            self.speed_label.setText("السرعة: غير محدودة")
        else:
            bps = value * 128 * 1024  # 1% = 128 KB/s
            self.limiter.set_rate(bps)
            self.speed_label.setText(f"السرعة: {bps // 1024} KB/s")

    def busy(self):
        return self.worker is not None and self.worker.isRunning()

    def launch(self, task, params):
        if self.busy():
            QMessageBox.warning(self, APP_NAME, "هناك عملية قيد التنفيذ")
            return
        self.bar.setValue(0)
        self.worker = Worker(task, params, self.limiter)
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.bar.setValue)
        self.worker.done_signal.connect(self.finished)
        self.worker.start()

    def finished(self, ok, msg):
        self.log(("✔ " if ok else "✘ ") + msg)
        if not ok:
            QMessageBox.critical(self, APP_NAME, msg)

    def cancel_task(self):
        if self.busy():
            self.worker.cancel.set()
            self.log("جارٍ الإلغاء...")

    # --- actions ---
    def start_download(self):
        url = self.url_input.text().strip()
        dest_dir = self.dest_input.text().strip()
        if not url or not urlparse(url).scheme:
            QMessageBox.warning(self, APP_NAME, "أدخل رابطاً صحيحاً")
            return
        if not os.path.isdir(dest_dir):
            QMessageBox.warning(self, APP_NAME, "مجلد الحفظ غير موجود")
            return
        time_range = None
        if self.range_input.text().strip():
            try:
                a, b = self.range_input.text().split("-", 1)
                time_range = (parse_time(a), parse_time(b))
            except ValueError:
                QMessageBox.warning(self, APP_NAME, "صيغة المدى الزمني غير صحيحة")
                return
        mode = self.mode.currentText()
        if mode == "ملف مباشر":
            name = os.path.basename(urlparse(url).path) or "download.bin"
            params = {"url": url, "dest": os.path.join(dest_dir, name),
                      "connections": int(self.conn.currentText()),
                      "sha256": self.sha_input.text().strip() or None}
            self.launch("direct", params)
        else:
            params = {"url": url, "dest_dir": dest_dir,
                      "audio_only": mode == "صوت فقط",
                      "audio_fmt": self.audio_fmt.currentText(),
                      "time_range": time_range}
            self.launch("media", params)

    def pick_local(self, title):
        path, _ = QFileDialog.getOpenFileName(self, title)
        return path

    def split_file(self):
        path = self.pick_local("اختر الفيديو للتقسيم")
        if not path:
            return
        minutes = self.range_input.text().strip() or "10"
        try:
            seconds = parse_time(minutes) if ":" in minutes else float(minutes) * 60
        except ValueError:
            QMessageBox.warning(self, APP_NAME, "أدخل مدة الجزء بالدقائق في حقل المدى، مثل 10")
            return
        self.launch("split", {"path": path, "seconds": seconds})

    def audio_file(self):
        path = self.pick_local("اختر الفيديو للتحويل")
        if path:
            self.launch("audio", {"path": path, "fmt": self.audio_fmt.currentText()})


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    run = getattr(app, "exec", None) or app.exec_
    sys.exit(run())


if __name__ == "__main__":
    main()
