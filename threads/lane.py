# pi/threads/lane.py
import time
import logging
import threading

def start_lane_monitor_thread(system):
    """
    Luồng giám sát CẢM BIẾN TẠI LÀN (LOGIC PULL).
    Lấy logic từ 'lane_sensor_monitoring_thread' của app_god.py.
    """
    
    # Khởi tạo trạng thái ban đầu
    try:
        num_lanes = 0
        with system.state_lock:
            num_lanes = len(system.system_state['lanes'])
        
        # Đảm bảo các mảng trạng thái có kích thước đúng
        system.last_sensor_state = [1] * num_lanes
        system.last_sensor_trigger_time = [0.0] * num_lanes
        system.auto_test_last_state = [1] * num_lanes
        system.auto_test_last_trigger = [0.0] * num_lanes
        
        last_sensor_state_prev = list(system.last_sensor_state)
        logging.info(f"[LANE_S] Luồng giám sát sensor làn (Pull Logic) đã khởi động cho {num_lanes} làn.")
    
    except Exception as e:
        logging.error(f"[LANE_S] Lỗi khởi tạo luồng sensor: {e}", exc_info=True)
        system.error_manager.trigger_maintenance(f"Lỗi luồng Lane Sensor: {e}")
        return

    try:
        while system.main_loop_running:
            if system.error_manager.is_maintenance():
                time.sleep(0.1); continue

            # Lấy config (cần cho cả 2 chế độ)
            debounce_time, current_queue_timeout, num_lanes = 0.1, 15.0, 0
            with system.state_lock:
                cfg_timing = system.system_state['timing_config']
                debounce_time = cfg_timing.get('sensor_debounce', 0.1)
                current_queue_timeout = cfg_timing.get('queue_head_timeout', 15.0)
                num_lanes = len(system.system_state['lanes'])
            now = time.time()
            
            # --- LOGIC AUTO TEST (Lấy từ app_god.py) ---
            if system.auto_test_enabled:
                if len(system.auto_test_last_state) != num_lanes:
                    system.auto_test_last_state = [1] * num_lanes
                    system.auto_test_last_trigger = [0.0] * num_lanes

                for i in range(num_lanes):
                    sensor_pin, push_pin, pull_pin, lane_name_for_log = None, None, None, "UNKNOWN"
                    with system.state_lock:
                        if not (0 <= i < len(system.system_state["lanes"])): continue
                        lane_for_read = system.system_state["lanes"][i]
                        sensor_pin = lane_for_read.get("sensor_pin"); push_pin = lane_for_read.get("push_pin")
                        pull_pin = lane_for_read.get("pull_pin"); lane_name_for_log = lane_for_read['name']

                    if sensor_pin is None or (push_pin is None or pull_pin is None):
                        continue 
                    if (sensor_pin == SENSOR_ENTRY_PIN) or \
                       (isinstance(system.gpio, MockGPIO) and sensor_pin == SENSOR_ENTRY_MOCK_PIN):
                        continue

                    try:
                        sensor_now = system.gpio.input(sensor_pin)
                    except Exception as gpio_e:
                        logging.error(f"[AUTO-TEST] Lỗi đọc GPIO pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                        system.error_manager.trigger_maintenance(f"Lỗi đọc sensor pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                        continue
                    
                    with system.state_lock:
                        if 0 <= i < len(system.system_state["lanes"]):
                            system.system_state["lanes"][i]["sensor_reading"] = sensor_now

                    prev_state = system.auto_test_last_state[i]
                    
                    if sensor_now == 0 and prev_state == 1: # Cạnh xuống (mới kích hoạt)
                        if (now - system.auto_test_last_trigger[i]) > debounce_time:
                            system.auto_test_last_trigger[i] = now
                            system.broadcast_log("info", f"[Auto-Test] Kích hoạt {lane_name_for_log}!")
                            # Chạy sorting_process trong luồng riêng
                            threading.Thread(target=system.sorting_process, args=(i, "AUTO-TEST"), daemon=True).start()
                    
                    system.auto_test_last_state[i] = sensor_now

                time.sleep(0.02) 
                continue # Bỏ qua logic queue bên dưới
            
            # --- (HẾT LOGIC AUTO TEST) ---
            
            # --- LOGIC CHÍNH (QUEUE) ---
            
            if len(last_sensor_state_prev) != num_lanes:
                last_sensor_state_prev = [1] * num_lanes
                system.last_sensor_trigger_time = [0.0] * num_lanes
                logging.warning(f"[SENSOR] Đã phát hiện thay đổi config. Đồng bộ state (size {num_lanes}).")

            # Xử lý Timeout
            current_queue_indices = []
            with system.processing_queue_lock:
                current_queue_indices = [j["lane_index"] for j in system.processing_queue]
                if system.processing_queue and system.queue_head_since > 0.0:
                    if (now - system.queue_head_since) > current_queue_timeout:
                        job_timeout = system.processing_queue.pop(0) 
                        job_id_timeout = job_timeout.get('job_id', '???') 
                        expected_lane_index = job_timeout['lane_index']
                        expected_lane_name = "UNKNOWN"
                        current_queue_indices = [j["lane_index"] for j in system.processing_queue]
                        
                        with system.state_lock:
                            if 0 <= expected_lane_index < len(system.system_state["lanes"]):
                                expected_lane_name = system.system_state['lanes'][expected_lane_index]['name']
                                if system.system_state["lanes"][expected_lane_index]["status"].startswith("Đang chờ vật"):
                                    system.system_state["lanes"][expected_lane_index]["status"] = "Sẵn sàng"
                            system.system_state["queue_indices"] = current_queue_indices
                            system.system_state["entry_queue_size"] = len(current_queue_indices)

                        system.queue_head_since = now if system.processing_queue else 0.0

                        system.broadcast_log("warn",
                            f"[JobID {job_id_timeout}] TIMEOUT! Đã tự động xóa Job cho {expected_lane_name} (>{current_queue_timeout}s).",
                            data={"queue": current_queue_indices}
                        )
                        logging.warning(f"[SENSOR] [JobID {job_id_timeout}] TIMEOUT! Xóa Job cho {expected_lane_name}.")
            
            # Quét tất cả sensor
            for i in range(num_lanes):
                sensor_pin, push_pin, lane_name_for_log = None, None, "UNKNOWN"
                with system.state_lock:
                    if not (0 <= i < len(system.system_state["lanes"])): continue
                    lane_for_read = system.system_state["lanes"][i]
                    sensor_pin = lane_for_read.get("sensor_pin"); push_pin = lane_for_read.get("push_pin")
                    lane_name_for_log = lane_for_read['name']

                if sensor_pin is None: continue
                if (sensor_pin == SENSOR_ENTRY_PIN) or \
                   (isinstance(system.gpio, MockGPIO) and sensor_pin == SENSOR_ENTRY_MOCK_PIN):
                    continue

                try:
                    sensor_now = system.gpio.input(sensor_pin)
                except Exception as gpio_e:
                    logging.error(f"[SENSOR] Lỗi đọc GPIO pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                    system.error_manager.trigger_maintenance(f"Lỗi đọc sensor pin {sensor_pin} ({lane_name_for_log}): {gpio_e}")
                    continue

                with system.state_lock:
                    if 0 <= i < len(system.system_state["lanes"]):
                        system.system_state["lanes"][i]["sensor_reading"] = sensor_now

                prev_state = last_sensor_state_prev[i]

                if sensor_now == 0 and prev_state == 1: # Cạnh xuống (Kích hoạt)
                    if (now - system.last_sensor_trigger_time[i]) > debounce_time:
                        system.last_sensor_trigger_time[i] = now

                        job_to_run = None
                        is_head_match = False
                        
                        with system.processing_queue_lock:
                            current_queue_indices_for_log = [j["lane_index"] for j in system.processing_queue]

                            while system.processing_queue:
                                job_head = system.processing_queue[0]
                                job_id_head = job_head.get('job_id', '???')

                                if job_head["lane_index"] == i:
                                    is_head_match = True
                                    job_to_run = system.processing_queue.pop(0)
                                    system.queue_head_since = now if system.processing_queue else 0.0
                                    break 

                                elif job_head["lane_index"] == system.NG_LANE_INDEX:
                                    job_ng_removed = system.processing_queue.pop(0)
                                    system.queue_head_since = now if system.processing_queue else 0.0