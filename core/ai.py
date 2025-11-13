# core/ai.py
import cv2
import numpy as np
from typing import Tuple, Optional
import logging

YOLO_AVAILABLE = False
DEEPSORT_AVAILABLE = False

try:
    from ultralytics import YOLO
    YOLO_AVAILABLE = True
except ImportError:
    pass

try:
    from deep_sort_realtime.deepsort_tracker import DeepSort
    DEEPSORT_AVAILABLE = True
except ImportError:
    pass

class AIDetector:
    def __init__(self, model_path: str, ai_config: dict):
        self.model = None
        self.tracker = None
        self.min_conf = ai_config.get('min_confidence', 0.6)
        self.lane_map = {}
        self.enabled = ai_config.get('enable_ai', False) and YOLO_AVAILABLE

        if not self.enabled:
            return

        try:
            self.model = YOLO(model_path)
            self._init_tracker(ai_config)
            logging.info(f"[AI] Loaded {model_path}")
        except Exception as e:
            logging.error(f"[AI] Load failed: {e}")
            self.enabled = False

    def _init_tracker(self, ai_config: dict):
        if not DEEPSORT_AVAILABLE or not ai_config.get('enable_deepsort', False):
            return
        try:
            self.tracker = DeepSort(
                max_age=ai_config.get('deepsort_max_age', 30),
                n_init=ai_config.get('deepsort_n_init', 3),
                max_iou_distance=ai_config.get('deepsort_max_iou_distance', 0.7),
            )
        except Exception as e:
            logging.error(f"[DEEPSORT] Init failed: {e}")

    def detect(self, frame: np.ndarray) -> Tuple[int, Optional[str], Optional[int]]:
        if not self.enabled or self.model is None:
            return -1, None, None

        results = self.model.predict(frame, conf=self.min_conf, verbose=False)
        result = results[0]
        if len(result.boxes) == 0:
            return -1, None, None

        detections = []
        for box in result.boxes:
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            conf = float(box.conf[0])
            cls = int(box.cls[0])
            detections.append(([x1, y1, x2-x1, y2-y1], conf, cls))

        track_id = None
        if self.tracker and detections:
            tracks = self.tracker.update_tracks(detections, frame=frame)
            best = max((t for t in tracks if t.is_confirmed()), key=lambda t: t.get_det_conf() or 0, default=None)
            if best:
                track_id = best.track_id

        high_conf = result.boxes.conf > self.min_conf
        if not high_conf.any():
            return -1, None, None
        best_idx = result.boxes.conf[high_conf].argmax()
        cls_id = int(result.boxes.cls[high_conf][best_idx])
        class_name = result.names[cls_id].upper()

        lane_idx = self.lane_map.get(class_name, -1)
        return lane_idx, class_name, track_id