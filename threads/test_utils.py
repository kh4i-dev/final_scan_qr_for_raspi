# pi/threads/test_utils.py
import time
import logging

def run_test_relay(system, lane_index, relay_action):
    """Test relay thủ công (Lấy từ app_god.py)"""
    push_pin, pull_pin, lane_name = None, None, f"Lane {lane_index + 1}"
    try:
        with system.state_lock:
            if not (0 <= lane_index < len(system.system_state["lanes"])):
                return system.broadcast_log("error", f"Test thất bại: Lane index {lane_index} không hợp lệ.")
            lane_state = system.system_state["lanes"][lane_index]
            lane_name = lane_state['name']
            push_pin = lane_state.get("push_pin"); pull_pin = lane_state.get("pull_pin")
            if push_pin is None and pull_pin is None:
                return system.broadcast_log("warn", f"Lane '{lane_name}' là lane đi thẳng.")
            if (push_pin is None or pull_pin is None):
                 return system.broadcast_log("error", f"Test thất bại: Lane '{lane_name}' thiếu pin PUSH hoặc PULL.")

        if relay_action == "push":
            system.broadcast_log("info", f"Test: Kích hoạt ĐẨY (PUSH) cho '{lane_name}'.")
            system.RELAY_OFF(pull_pin); system.RELAY_ON(push_pin)
            with system.state_lock:
                if 0 <= lane_index < len(system.system_state["lanes"]):
                    system.system_state["lanes"][lane_index]["relay_grab"] = 0
                    system.system_state["lanes"][lane_index]["relay_push"] = 1
        
        elif relay_action == "grab":
            system.broadcast_log("info", f"Test: Kích hoạt THU (PULL/GRAB) cho '{lane_name}'.")
            system.RELAY_OFF(push_pin); system.RELAY_ON(pull_pin)
            with system.state_lock:
                if 0 <= lane_index < len(system.system_state["lanes"]):
                    system.system_state["lanes"][lane_index]["relay_grab"] = 1
                    system.system_state["lanes"][lane_index]["relay_push"] = 0
    except Exception as e:
        logging.error(f"[TEST] Lỗi test relay '{relay_action}' cho '{lane_name}': {e}", exc_info=True)
        system.broadcast_log("error", f"Lỗi test '{relay_action}' trên '{lane_name}': {e}")
        system.reset_all_relays_to_default()

def run_test_all_relays(system):
    """Test relay tuần tự (Lấy từ app_god.py)"""
    with system.test_seq_lock:
        if system.test_seq_running:
            return system.broadcast_log("warn", "Test tuần tự đang chạy.")
        system.test_seq_running = True

    logging.info("[TEST] Bắt đầu test tuần tự (Cycle) relay...")
    system.broadcast_log("info", "Bắt đầu test tuần tự (Cycle) relay...")
    stopped_early = False

    try:
        num_lanes = 0
        cycle_delay, settle_delay = 0.3, 0.2
        with system.state_lock:
            num_lanes = len(system.system_state['lanes'])
            cfg = system.system_state['timing_config']
            cycle_delay = cfg.get('cycle_delay', 0.3)
            settle_delay = cfg.get('settle_delay', 0.2)

        for i in range(num_lanes):
            with system.test_seq_lock: stop_requested = not system.main_loop_running or not system.test_seq_running
            if stop_requested: stopped_early = True; break

            lane_name, push_pin, pull_pin = f"Lane {i+1}", None, None
            with system.state_lock:
                if 0 <= i < len(system.system_state['lanes']):
                    lane_state = system.system_state['lanes'][i]
                    lane_name = lane_state['name']
                    push_pin = lane_state.get("push_pin"); pull_pin = lane_state.get("pull_pin")
            
            if push_pin is None or pull_pin is None:
                system.broadcast_log("info", f"Bỏ qua '{lane_name}' (lane đi thẳng).")
                continue

            system.broadcast_log("info", f"Testing Cycle cho '{lane_name}'...")
            
            system.RELAY_OFF(pull_pin);
            with system.state_lock: system.system_state["lanes"][i]["relay_grab"] = 0
            time.sleep(settle_delay)
            if not system.main_loop_running or not system.test_seq_running: stopped_early = True; break

            system.RELAY_ON(push_pin);
            with system.state_lock: system.system_state["lanes"][i]["relay_push"] = 1
            time.sleep(cycle_delay)
            if not system.main_loop_running or not system.test_seq_running: stopped_early = True; break

            system.RELAY_OFF(push_pin);
            with system.state_lock: system.system_state["lanes"][i]["relay_push"] = 0
            time.sleep(settle_delay)
            if not system.main_loop_running or not system.test_seq_running: stopped_early = True; break

            system.RELAY_ON(pull_pin)
            with system.state_lock: system.system_state["lanes"][i]["relay_grab"] = 1
            
            time.sleep(0.5)

        if stopped_early: system.broadcast_log("warn", "Test tuần tự đã dừng.")
        else: system.broadcast_log("info", "Test tuần tự hoàn tất.")
    finally:
        with system.test_seq_lock: system.test_seq_running = False
        system.reset_all_relays_to_default()