# pi/threads/broadcast.py
import json
import time
import logging

def start_broadcast_state_thread(system):
    """Gửi state tới WS (Lấy từ app_god.py)"""
    last_state_str = ""
    while system.main_loop_running:
        try:
            state_copy = system.get_full_state()
            if state_copy is None:
                time.sleep(0.5); continue
            
            # Thêm trạng thái auth (do web/app.py quản lý)
            state_copy["auth_enabled"] = system.system_state.get("auth_enabled", False)
                
            current_msg = json.dumps({"type": "state_update", "state": state_copy})
            
            clients_to_send = []
            with system.ws_lock: clients_to_send = list(system.ws_clients)
            
            if not clients_to_send:
                time.sleep(0.5); continue

            if current_msg != last_state_str:
                with system.broadcast_lock:
                    for client in clients_to_send:
                        try: client.send(current_msg)
                        except Exception: 
                            system.remove_ws_client(client) # Xóa client hỏng
                last_state_str = current_msg
            
            time.sleep(0.5) # Tần suất cập nhật state
            
        except Exception as e:
            logging.error(f"[BROADCAST] Lỗi nghiêm trọng: {e}", exc_info=True)
            time.sleep(1.0)