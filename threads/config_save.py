# pi/threads/config_save.py
import time
import logging
import json
import os

# Cần đường dẫn file config
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(BASE_DIR, "config", "config.json")

def start_periodic_config_save_thread(system):
    """Lưu config định kỳ (Lấy từ app_god.py)"""
    while system.main_loop_running:
        time.sleep(60)
        if system.error_manager.is_maintenance(): continue
        
        config_to_save = {}
        
        try:
            with system.state_lock:
                config_to_save['timing_config'] = system.system_state['timing_config'].copy()
                config_to_save['ai_config'] = system.system_state['ai_config'].copy()
                config_to_save['camera_settings'] = system.system_state['camera_settings'].copy()
                
                current_lanes_config = []
                for lane_state in system.system_state['lanes']:
                    current_lanes_config.append({
                        "id": lane_state['id'], "name": lane_state['name'],
                        "sensor_pin": lane_state.get('sensor_pin'), 
                        "push_pin": lane_state.get('push_pin'), 
                        "pull_pin": lane_state.get('pull_pin')
                    })
                config_to_save['lanes_config'] = current_lanes_config
            
            with system.config_file_lock:
                with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                    json.dump(config_to_save, f, indent=4)
            logging.info("[CONFIG] Đã tự động lưu config (timing, ai, lanes, camera).")

        except Exception as e:
            logging.error(f"[CONFIG] Lỗi tự động lưu config: {e}")