# core/qr.py
import cv2
try:
    from pyzbar import pyzbar
    PYZBAR = True
except ImportError:
    PYZBAR = False

def scan_qr_from_frame(frame):
    if frame is None:
        return None, None
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    if gray.mean() < 10:
        return None, None

    data = None
    source = None

    if PYZBAR:
        decoded = pyzbar.decode(gray)
        if decoded:
            raw = decoded[0].data
            data = raw.decode('utf-8', errors='ignore').strip('\x00')
            source = "Pyzbar"

    if not data:
        detector = cv2.QRCodeDetector()
        retval, _, _ = detector.detectAndDecode(gray)
        if retval:
            data = retval
            source = "CV2"

    return (data.strip() if data else None), source