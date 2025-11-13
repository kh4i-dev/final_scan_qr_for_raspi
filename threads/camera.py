# pi/threads/camera.py
import cv2
import time
import logging

def start_camera_thread(system):
    """Luồng chụp camera (Lấy từ app_god.py)"""
    global latest_frame, fps_value # Vẫn dùng global để tương thích code cũ, nhưng gán vào system

    frame_count = 0
    start_time = time.time()
    
    cam_settings = {}
    with system.state_lock:
        cam_settings = system.system_state.get('camera_settings', {})
    
    # Lấy CAMERA_INDEX từ config (nếu có), nếu không dùng 0
    camera_index = cam_settings.get('camera_index', 0) 
    
    camera = cv2.VideoCapture(camera_index)
    camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640); 
    camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
    camera.set(cv2.CAP_PROP_BUFFERSIZE, 1) 
    
    try:
        auto_exposure_cfg = cam_settings.get('auto_exposure', True)
        auto_exposure_val = 1 if auto_exposure_cfg else 0
        camera.set(cv2.CAP_PROP_AUTO_EXPOSURE, auto_exposure_val)
        logging.info(f"[CAMERA] Đã đặt Auto Exposure: {'BẬT' if auto_exposure_val == 1 else 'TẮT'}.")

        if auto_exposure_val == 0:
            brightness_val = int(cam_settings.get('brightness', 128))
            contrast_val = int(cam_settings.get('contrast', 32))
            camera.set(cv2.CAP_PROP_BRIGHTNESS, brightness_val)
            camera.set(cv2.CAP_PROP_CONTRAST, contrast_val)
            logging.info(f"[CAMERA] Đã đặt Brightness thủ công: {brightness_val}")
            logging.info(f"[CAMERA] Đã đặt Contrast thủ công: {contrast_val}")
    except Exception as cam_e:
        logging.error(f"[CAMERA] Lỗi khi cài đặt thông số camera: {cam_e}")
    
    if not camera.isOpened():
        logging.error(f"[ERROR] Không mở được camera index {camera_index}.")
        system.error_manager.trigger_maintenance(f"Không thể mở camera (index {camera_index}).")
        return
    
    logging.info(f"[CAMERA] Camera (index {camera_index}) đã khởi động.")
    
    retries = 0; max_retries = 5
    while system.main_loop_running:
        if system.error_manager.is_maintenance():
            time.sleep(0.5); continue
            
        ret, frame = camera.read()
        
        if not ret:
            retries += 1
            logging.warning(f"[WARN] Mất camera (lần {retries}/{max_retries}), thử khởi động lại...")
            system.broadcast_log("error", f"Mất camera (lần {retries}), đang thử lại...")
            if retries > max_retries:
                logging.critical("[ERROR] Camera lỗi vĩnh viễn. Chuyển sang chế độ bảo trì.")
                system.error_manager.trigger_maintenance("Camera lỗi vĩnh viễn (mất kết nối).")
                break
            camera.release(); time.sleep(1); camera = cv2.VideoCapture(camera_index)
            continue
        retries = 0

        frame_count += 1
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        if elapsed_time >= 1.0:
            system.fps_value = frame_count / elapsed_time
            frame_count = 0
            start_time = current_time
        
        with system.frame_lock:
            system.latest_frame = frame.copy()
            
        time.sleep(1 / 60) # Cung cấp 60 FPS
        
    camera.release()
    logging.info("[CAMERA] Luồng camera đã dừng.")