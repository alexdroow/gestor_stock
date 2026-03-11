import base64
import os
import re
import socket
import threading
import time
from urllib.parse import quote, unquote, urlparse


def _opencv_import_message(exc):
    detail = f"{exc.__class__.__name__}: {exc}"
    low = detail.lower()
    if "more than once per process" in low or "mas de una vez por proceso" in low:
        return "Conflicto DLL OpenCV (modulo cargado mas de una vez por proceso)"
    if "openh264" in low:
        return "OpenH264 faltante (instala codec 64-bit o usa substream alternativo)"
    if "dll load failed" in low or "no module named cv2" in low:
        return "OpenCV no disponible (instala Visual C++ 2015-2022 x64)"
    return f"OpenCV no disponible ({detail})"[:180]


def _to_int(value, default, min_value=None, max_value=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def _status_jpeg(text):
    try:
        import cv2
        import numpy as np
    except Exception:
        return b""

    canvas = np.zeros((240, 426, 3), dtype=np.uint8)
    canvas[:] = (28, 32, 45)
    cv2.putText(canvas, "Camara sin senal", (22, 94), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (255, 255, 255), 2, cv2.LINE_AA)
    cv2.putText(canvas, str(text or "RTSP no disponible")[:34], (22, 138), cv2.FONT_HERSHEY_SIMPLEX, 0.62, (189, 201, 219), 1, cv2.LINE_AA)
    ok, encoded = cv2.imencode(".jpg", canvas, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
    if not ok:
        return b""
    return encoded.tobytes()


def _mjpeg_frame_bytes(jpeg_bytes):
    size = len(jpeg_bytes or b"")
    return (
        b"--frame\r\n"
        b"Content-Type: image/jpeg\r\n"
        b"Cache-Control: no-cache\r\n"
        + f"Content-Length: {size}\r\n\r\n".encode("ascii")
        + (jpeg_bytes or b"")
        + b"\r\n"
    )


def _rtsp_describe_status(host, port, path, user="", password="", timeout=1.6):
    host = str(host or "").strip()
    if not host:
        return None
    path = str(path or "").strip()
    if not path:
        return None
    if not path.startswith("/"):
        path = "/" + path
    try:
        port = int(port or 554)
    except Exception:
        port = 554

    auth = ""
    if user or password:
        token = base64.b64encode(f"{user}:{password}".encode("utf-8", errors="ignore")).decode("ascii", errors="ignore")
        auth = f"Authorization: Basic {token}\r\n"

    req = (
        f"DESCRIBE rtsp://{host}:{port}{path} RTSP/1.0\r\n"
        f"CSeq: 2\r\n"
        f"Accept: application/sdp\r\n"
        f"{auth}"
        f"User-Agent: SucreeStock/4.3\r\n\r\n"
    )
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall(req.encode("utf-8", errors="ignore"))
            data = s.recv(2048).decode("utf-8", errors="ignore")
        if not data:
            return None
        first = data.splitlines()[0] if data.splitlines() else ""
        m = re.search(r"RTSP/\d+\.\d+\s+(\d{3})", first)
        if not m:
            return None
        return int(m.group(1))
    except Exception:
        return None


def _extract_rtsp_params(rtsp_url):
    url = str(rtsp_url or "").strip()
    if not url:
        return None
    try:
        parsed = urlparse(url)
    except Exception:
        return None
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("rtsp", "rtsps"):
        return None
    host = parsed.hostname or ""
    if not host:
        return None
    port = int(parsed.port or 554)
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    path = parsed.path or ""
    query = parsed.query or ""

    channel = None
    stream = None

    m = re.search(r"/avstream/channel=(\d+)/stream=(\d+)\.sdp", path, flags=re.IGNORECASE)
    if m:
        channel = _to_int(m.group(1), 1, min_value=0, max_value=64)
        stream = _to_int(m.group(2), 1, min_value=0, max_value=1)

    if channel is None:
        m_channel = re.search(r"(?:^|[?&])channel=(\d+)(?:&|$)", query, flags=re.IGNORECASE)
        m_subtype = re.search(r"(?:^|[?&])subtype=(\d+)(?:&|$)", query, flags=re.IGNORECASE)
        if m_channel:
            channel = _to_int(m_channel.group(1), 1, min_value=0, max_value=64)
        if m_subtype:
            stream = _to_int(m_subtype.group(1), 1, min_value=0, max_value=1)

    if channel is None:
        return None
    if stream is None:
        stream = 1

    return {
        "scheme": scheme,
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "channel": channel,
        "stream": stream,
    }


def _build_rtsp_url(host, channel, port=554, stream=1, user="", password="", scheme="rtsp"):
    host = str(host or "").strip()
    if not host:
        return ""
    auth = ""
    if user and password:
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    elif user:
        auth = f"{quote(user, safe='')}@"
    return f"{str(scheme or 'rtsp').lower()}://{auth}{host}:{int(port)}/avstream/channel={int(channel)}/stream={int(stream)}.sdp"


def build_rtsp_candidates(rtsp_url):
    url = str(rtsp_url or "").strip()
    params = _extract_rtsp_params(url)
    if not params:
        return [url] if url else []

    host = params["host"]
    port = params["port"]
    user = params["user"]
    password = params["password"]
    scheme = params["scheme"]
    channel = params["channel"]
    stream = params["stream"]
    alt_stream = 0 if stream == 1 else 1

    candidates = []

    def add(v):
        v = str(v or "").strip()
        if v and v not in candidates:
            candidates.append(v)

    add(url)

    # XVR AVStream variants
    for ch in (channel, channel - 1, channel + 1):
        if ch < 0:
            continue
        add(_build_rtsp_url(host, ch, port=port, stream=stream, user=user, password=password, scheme=scheme))
        add(_build_rtsp_url(host, ch, port=port, stream=alt_stream, user=user, password=password, scheme=scheme))

        # XMEye-like variants
        user_q = quote(user or "admin", safe="")
        pass_q = quote(password or "", safe="")
        add(f"{scheme}://{host}:{int(port)}/user={user_q}_password={pass_q}_channel={ch}_stream={stream}.sdp?real_stream")
        add(f"{scheme}://{host}:{int(port)}/user={user_q}_password={pass_q}_channel={ch}_stream={alt_stream}.sdp?real_stream")

    return candidates[:10]


def filter_reachable_candidates(rtsp_url, candidates):
    params = _extract_rtsp_params(rtsp_url)
    if not params:
        return candidates

    host = params["host"]
    port = params["port"]
    user = params["user"]
    password = params["password"]

    valid = []
    for url in candidates or []:
        try:
            parsed = urlparse(str(url or "").strip())
            path = parsed.path or "/"
            if parsed.query:
                path = f"{path}?{parsed.query}"
            code = _rtsp_describe_status(host, port, path, user=user, password=password, timeout=1.2)
            if code in (200, 401):
                valid.append(url)
        except Exception:
            continue
    return valid if valid else candidates


class CameraWorker:
    def __init__(self, cam_id, rtsp_url, target_fps=12, jpeg_quality=84):
        self.cam_id = int(cam_id)
        self.rtsp_url = str(rtsp_url or "").strip()
        self.target_fps = _to_int(target_fps, 12, min_value=4, max_value=24)
        self.jpeg_quality = _to_int(jpeg_quality, 84, min_value=60, max_value=92)
        self.frame_sleep = 1.0 / float(max(1, self.target_fps))
        self.quality_args = [1, int(self.jpeg_quality)]  # cv2.IMWRITE_JPEG_QUALITY set in run

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._latest_jpeg = _status_jpeg("Iniciando...")
        self._online = False
        self._last_error = ""
        self._active_url = self.rtsp_url
        self._active_idx = 0
        self._frame_count = 0
        self._last_frame_ts = 0.0
        self._candidates = build_rtsp_candidates(self.rtsp_url)
        if not self._candidates and self.rtsp_url:
            self._candidates = [self.rtsp_url]
        # Evitar bloqueos en navegación: la validación de rutas RTSP no debe
        # ejecutarse en el hilo HTTP principal.

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=f"cam-worker-{self.cam_id}", daemon=True)
        self._thread.start()

    def stop(self, timeout=1.5):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def get_jpeg(self):
        with self._lock:
            return self._latest_jpeg

    def status(self):
        with self._lock:
            return {
                "cam_id": self.cam_id,
                "online": bool(self._online),
                "last_error": self._last_error,
                "active_url": self._active_url,
                "active_idx": int(self._active_idx),
                "candidate_count": len(self._candidates),
                "frame_count": int(self._frame_count),
                "last_frame_ts": float(self._last_frame_ts or 0),
            }

    def _set_status(self, online=None, error=None, jpeg=None, active_url=None, active_idx=None):
        with self._lock:
            if online is not None:
                self._online = bool(online)
            if error is not None:
                self._last_error = str(error or "")
            if jpeg is not None:
                self._latest_jpeg = jpeg
            if active_url is not None:
                self._active_url = str(active_url or "")
            if active_idx is not None:
                self._active_idx = int(active_idx or 0)
            if jpeg is not None and online:
                self._frame_count += 1
                self._last_frame_ts = time.time()

    def _run(self):
        try:
            import cv2
        except Exception as exc:
            msg = _opencv_import_message(exc)
            self._set_status(online=False, error=msg, jpeg=_status_jpeg(msg))
            while not self._stop.wait(1.2):
                pass
            return

        self.quality_args = [int(cv2.IMWRITE_JPEG_QUALITY), int(self.jpeg_quality)]

        if not self._candidates:
            self._set_status(online=False, error="Sin RTSP", jpeg=_status_jpeg("Sin URL RTSP"))
            while not self._stop.wait(1.2):
                pass
            return

        os.environ["OPENCV_VIDEOIO_DEBUG"] = "0"
        os.environ["OPENCV_LOG_LEVEL"] = "SILENT"
        os.environ["OPENCV_FFMPEG_LOGLEVEL"] = "-8"
        os.environ["FFMPEG_LOG_LEVEL"] = "quiet"
        os.environ["AV_LOG_LEVEL"] = "quiet"
        try:
            if hasattr(cv2, "setLogLevel"):
                cv2.setLogLevel(0)
            elif hasattr(cv2, "utils") and hasattr(cv2.utils, "logging"):
                cv2.utils.logging.setLogLevel(cv2.utils.logging.LOG_LEVEL_SILENT)
        except Exception:
            pass

        cap = None
        fail_reads = 0
        idx = 0
        last_publish_ts = 0.0

        while not self._stop.is_set():
            if cap is None or not cap.isOpened():
                url = self._candidates[idx % len(self._candidates)]
                idx = idx % len(self._candidates)
                self._set_status(
                    online=False,
                    error=f"Conectando ruta {idx+1}/{len(self._candidates)}",
                    jpeg=_status_jpeg("Conectando..."),
                    active_url=url,
                    active_idx=idx,
                )
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
                    "rtsp_transport;tcp|fflags;discardcorrupt|err_detect;ignore_err|loglevel;quiet|"
                    "stimeout;5000000|rw_timeout;5000000|max_delay;900000"
                )
                ffmpeg_backend = getattr(cv2, "CAP_FFMPEG", 0)
                cap = cv2.VideoCapture(url, ffmpeg_backend) if ffmpeg_backend else cv2.VideoCapture(url)
                if not cap.isOpened():
                    cap = cv2.VideoCapture(url)
                if hasattr(cv2, "CAP_PROP_BUFFERSIZE"):
                    try:
                        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                    except Exception:
                        pass
                if hasattr(cv2, "CAP_PROP_FPS"):
                    try:
                        cap.set(cv2.CAP_PROP_FPS, float(self.target_fps))
                    except Exception:
                        pass
                if not cap.isOpened():
                    self._set_status(online=False, error="No conecta RTSP", jpeg=_status_jpeg("No conecta RTSP"))
                    try:
                        cap.release()
                    except Exception:
                        pass
                    cap = None
                    idx = (idx + 1) % len(self._candidates)
                    self._stop.wait(0.7)
                    continue

            ok, frame = cap.read()
            if not ok or frame is None:
                fail_reads += 1
                if fail_reads >= 18:
                    try:
                        cap.release()
                    except Exception:
                        pass
                    cap = None
                    idx = (idx + 1) % len(self._candidates)
                    fail_reads = 0
                    self._set_status(online=False, error="Reconectando...", jpeg=_status_jpeg("Reconectando..."))
                    self._stop.wait(0.35)
                else:
                    self._stop.wait(0.05)
                continue

            fail_reads = 0
            now_ts = time.time()
            if (now_ts - last_publish_ts) < self.frame_sleep:
                continue

            ok_enc, enc = cv2.imencode(".jpg", frame, self.quality_args)
            if not ok_enc:
                self._stop.wait(0.01)
                continue

            self._set_status(online=True, error="", jpeg=enc.tobytes())
            last_publish_ts = now_ts
            self._stop.wait(0.001)

        if cap is not None:
            try:
                cap.release()
            except Exception:
                pass


class CameraHub:
    def __init__(self):
        self._lock = threading.RLock()
        self._workers = {}
        self._last_sync = 0.0

    def sync_paneles(self, paneles):
        with self._lock:
            desired = {}
            for p in paneles or []:
                try:
                    cam_id = int(p.get("id") or 0)
                except Exception:
                    continue
                if cam_id <= 0:
                    continue
                if not bool(p.get("activa", True)):
                    continue
                rtsp = str(p.get("rtsp_url") or "").strip()
                if not rtsp:
                    continue
                desired[cam_id] = rtsp

            # stop removed/changed
            for cam_id, worker in list(self._workers.items()):
                target = desired.get(cam_id)
                if not target or str(worker.rtsp_url).strip() != str(target).strip():
                    worker.stop()
                    self._workers.pop(cam_id, None)

            # start missing
            for cam_id, rtsp in desired.items():
                if cam_id in self._workers:
                    continue
                worker = CameraWorker(cam_id, rtsp)
                worker.start()
                self._workers[cam_id] = worker

            self._last_sync = time.time()

    def get_statuses(self):
        with self._lock:
            out = {}
            for cam_id, worker in self._workers.items():
                out[cam_id] = worker.status()
            return out

    def get_jpeg(self, cam_id):
        with self._lock:
            worker = self._workers.get(int(cam_id))
        if not worker:
            return _status_jpeg("Camara no configurada")
        return worker.get_jpeg()

    def mjpeg_generator(self, cam_id, fps=10):
        fps = _to_int(fps, 10, min_value=3, max_value=20)
        delay = 1.0 / float(max(1, fps))
        while True:
            jpeg = self.get_jpeg(cam_id)
            yield _mjpeg_frame_bytes(jpeg)
            time.sleep(delay)

    def stop_all(self):
        with self._lock:
            for worker in self._workers.values():
                worker.stop()
            self._workers.clear()

