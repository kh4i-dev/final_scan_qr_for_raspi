let ws;
let autoTestEnabled = false;
let qrTimer;
let sortBarChart;
let sortDoughnutChart;
let laneNamesMap = {};
let laneIdMap = {};
let sensorPinMap = {};
let uiLaneCount = 0; // Biến để theo dõi số lượng lane đang hiển thị

// Tải stream video
document.getElementById("video_feed").src = "/video_feed";
connectWebSocket();
// loadConfig(); // Tải config ngay lập tức -- (XÓA DÒNG NÀY)
showPage('home');


// ===== KẾT NỐI WEBSOCKET =====
function connectWebSocket() {
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${wsProtocol}//${window.location.host}/ws`;

    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
        addLog("success", "Đã kết nối WebSocket với server.");
        loadConfig(); // Tải lại config khi kết nối
    };

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (data.type === "state_update") {
                updateState(data.state);
            } else if (data.type === "log") {
                addLog(data.log_type, data.message, data.timestamp, data.data);
            }
        } catch (e) {
            console.error("Lỗi parse JSON từ WS:", e, event.data);
            addLog("error", "Nhận được tin nhắn WebSocket không hợp lệ.");
        }
    };

    ws.onclose = (event) => {
        addLog("error", "Mất kết nối WebSocket. Đang thử kết nối lại sau 3s...");
        setTimeout(connectWebSocket, 3000);
    };

    ws.onerror = () => {
        addLog("error", "Lỗi WebSocket.");
    };
}

// ===== ĐIỀU HƯỚNG TRANG =====
function showPage(pageName) {
    const pages = ['page-home', 'page-config', 'page-test', 'page-stats'];
    const navs = ['nav-home', 'nav-config', 'nav-test', 'nav-stats'];

    pages.forEach((id) => {
        document.getElementById(id)?.classList.toggle('hidden', id !== `page-${pageName}`);
    });
    navs.forEach((id) => {
        document.getElementById(id)?.classList.toggle('active', id === `nav-${pageName}`);
    });

    if (pageName === 'stats') {
        loadSortChart();
    }
}

// ===== MODAL XÁC NHẬN =====
let confirmCallback = null;
function showConfirmModal(text, onConfirm) {
    document.getElementById('confirm-modal-text').textContent = text;
    confirmCallback = onConfirm;

    const modal = document.getElementById('confirm-modal');
    const modalContent = document.getElementById('confirm-modal-content');

    modal.classList.remove('hidden');
    setTimeout(() => {
        modal.style.opacity = '1';
        modalContent.style.transform = 'scale(1)';
    }, 10);

    document.getElementById('confirm-modal-ok').onclick = () => {
        if (confirmCallback) confirmCallback();
        closeConfirmModal();
    };
    document.getElementById('confirm-modal-cancel').onclick = closeConfirmModal;
}

function closeConfirmModal() {
    const modal = document.getElementById('confirm-modal');
    const modalContent = document.getElementById('confirm-modal-content');

    modal.style.opacity = '0';
    modalContent.style.transform = 'scale(0.95)';
    setTimeout(() => {
        modal.classList.add('hidden');
        confirmCallback = null;
    }, 200);
}

// ===== HIỂN THỊ LOG =====
function addLog(type, message, timestamp, data) {
    const logContainer = document.getElementById("log-container");
    if (!logContainer) return;

    const time = timestamp || new Date().toLocaleTimeString();
    let colorClass = "text-gray-400";
    let prefix = `[INFO]`;

    switch (type) {
        case "success": colorClass = "text-green-400"; prefix = `[OK]`; break;
        case "error": colorClass = "text-red-400"; prefix = `[LỖI]`; break;
        case "warn": colorClass = "text-yellow-400"; prefix = `[WARN]`; break;
        case "sort": colorClass = "text-cyan-400"; prefix = `[SORT]`; message = `Phân loại ${data.name}, tổng: ${data.count}`; break;
        case "pass": colorClass = "text-indigo-400"; prefix = `[PASS]`; message = `Đếm vật phẩm đi thẳng qua ${data.name}, tổng: ${data.count}`; break;
        case "qr":
            colorClass = "text-blue-400";
            prefix = `[QR]`;
            const qr_lane_index = Object.keys(laneIdMap).find(key => laneIdMap[key] === data.data_key);
            const display_name = laneNamesMap[qr_lane_index] || data.data_key;

            message = `Phát hiện ${display_name} (ID: ${data.data_key})`;
            showQrOverlay(display_name, 'bg-blue-600');
            break;
        case "qr_ng": colorClass = "text-red-500"; prefix = `[QR]`; message = `Hàng NG: ${data}`; showQrOverlay(data, 'bg-red-600'); break;
        case "unknown_qr": colorClass = "text-yellow-500"; prefix = `[QR]`; message = `Không rõ: ${data.data_key}`; showQrOverlay(data.data_key, 'bg-yellow-500'); break;
    }

    const logEntry = document.createElement("div");
    logEntry.className = `flex ${colorClass}`;
    logEntry.innerHTML = `<span class="flex-shrink-0 w-20">[${time}]</span><span class="flex-shrink-0 w-16">${prefix}</span><span class="flex-1">${message}</span>`;

    // SỬA LỖI CUỘN: prepend (thêm lên đầu) và scrollTop = 0 (cuộn lên đầu)
    logContainer.prepend(logEntry);
    logContainer.scrollTop = 0;
}

// ===== CẬP NHẬT TRẠNG THÁI (STATE) =====
function updateState(state) {
    if (!state || !state.lanes) return;

    // (MỚI) Cập nhật Sensor Gác Cổng (nếu được bật)
    const useGantry = state.timing_config?.use_sensor_entry_gantry;
    const gantryHome = document.getElementById('sensor-gantry-status-home');
    const gantryTest = document.getElementById('sensor-gantry-status-test');

    if (useGantry && gantryHome && gantryTest) {
        gantryHome.classList.remove('hidden');
        gantryTest.classList.remove('hidden');

        const gantryActive = state.sensor_entry_reading === 0;
        const textEl = document.getElementById('sensor-gantry-text-home');
        const lightHome = document.getElementById('sensor-gantry-light-home');
        const lightTest = document.getElementById('test-sensor-gantry');

        if (textEl) textEl.textContent = gantryActive ? "KÍCH HOẠT" : "Chờ";
        if (textEl) textEl.classList.toggle('text-yellow-400', gantryActive);
        if (textEl) textEl.classList.toggle('text-gray-500', !gantryActive);

        if (lightHome) {
            lightHome.classList.toggle('sensor-active', gantryActive);
            lightHome.classList.toggle('sensor-inactive', !gantryActive);
        }
        if (lightTest) {
            lightTest.classList.toggle('sensor-active', gantryActive);
            lightTest.classList.toggle('sensor-inactive', !gantryActive);
        }

    } else if (gantryHome && gantryTest) {
        gantryHome.classList.add('hidden');
        gantryTest.classList.add('hidden');
    }

    // 1. Cập nhật Maps (Map ID và Map Name) dựa trên state mới nhất
    laneNamesMap = {};
    laneIdMap = {};
    sensorPinMap = {};
    state.lanes.forEach((lane, i) => {
        laneNamesMap[i] = lane.name;
        laneIdMap[i] = lane.id;
        sensorPinMap[i] = lane.sensor_pin;
    });

    // 2. Cập nhật giao diện Lanes (Tự động)
    // Chỉ render lại HTML nếu số lượng lane thay đổi
    if (state.lanes.length !== uiLaneCount) {
        renderAllLanesUI(state.lanes);
        uiLaneCount = state.lanes.length;
    }

    // 3. Cập nhật dữ liệu cho từng lane (count, sensor, relay, status)
    state.lanes.forEach((lane, i) => {
        const isSortingLane = lane.push_pin !== null || lane.pull_pin !== null;

        // Trang Chủ
        const countEl = document.getElementById(`lane-${i}-count`);
        if (countEl) countEl.textContent = lane.count;

        const laneSensorEl = document.getElementById(`lane-${i}-sensor`);
        if (laneSensorEl) {
            laneSensorEl.classList.toggle("sensor-active", lane.sensor_reading === 0);
            laneSensorEl.classList.toggle("sensor-inactive", lane.sensor_reading !== 0);
        }

        const grabEl = document.getElementById(`lane-${i}-grab`);
        if (grabEl) {
            grabEl.classList.toggle("relay-active", isSortingLane && lane.relay_grab === 1);
            grabEl.classList.toggle("relay-inactive", isSortingLane && lane.relay_grab !== 1);
        }
        const pushEl = document.getElementById(`lane-${i}-push`);
        if (pushEl) {
            pushEl.classList.toggle("relay-active", isSortingLane && lane.relay_push === 1);
            pushEl.classList.toggle("relay-inactive", isSortingLane && lane.relay_push !== 1);
        }

        const statusEl = document.getElementById(`status-text-${i}`);
        if (statusEl) {
            statusEl.textContent = lane.status;
            const isWaiting = lane.status.includes("Đang chờ") || lane.status.includes("Đang phân loại") || lane.status.includes("Đang đi thẳng");
            statusEl.classList.toggle("status-pulse", isWaiting);
            statusEl.classList.toggle("bg-blue-500", isWaiting && isSortingLane);
            statusEl.classList.toggle("bg-indigo-500", isWaiting && !isSortingLane);
            statusEl.classList.toggle("bg-gray-700", !isWaiting);
        }

        // Trang Test
        const testSensor = document.getElementById(`test-sensor-${i}`);
        if (testSensor) {
            testSensor.classList.toggle("sensor-active", lane.sensor_reading === 0);
            testSensor.classList.toggle("sensor-inactive", lane.sensor_reading !== 0);
        }

        // Trang Mock (nếu hiển thị)
        const mockStatusEl = document.getElementById(`mock-lane-status-${i}`);
        if (mockStatusEl) {
            const isActive = lane.sensor_reading === 0;
            mockStatusEl.textContent = isActive ? 'ĐANG KÍCH HOẠT (LOW)' : 'KHÔNG KÍCH HOẠT (HIGH)';
            mockStatusEl.classList.toggle('text-red-400', isActive);
            mockStatusEl.classList.toggle('text-green-400', !isActive);

            const activeBtn = document.getElementById(`mock-btn-active-${i}`);
            if (activeBtn) activeBtn.disabled = isActive;

            const idleBtn = document.getElementById(`mock-btn-idle-${i}`);
            if (idleBtn) idleBtn.disabled = !isActive;
        }
    });

    // 4. Cập nhật Badges (Huy hiệu)
    document.getElementById('mock-badge').classList.toggle('hidden', !state.is_mock);
    document.getElementById('maintenance-badge').classList.toggle('hidden', !state.maintenance_mode);

    document.getElementById('mock-controls').classList.toggle('hidden', !state.is_mock);

    // 5. Cập nhật Banner Bảo trì
    if (state.maintenance_mode) {
        const errorMsg = state.last_error || "Lỗi không xác định.";
        document.getElementById('maintenance-error').textContent = errorMsg;
        document.getElementById('maintenance-banner').classList.remove('hidden');
        document.querySelectorAll('button, input, select, textarea').forEach(el => {
            const allowDuringMaintenance = el.dataset && el.dataset.allowMaintenance === 'true';
            if (!allowDuringMaintenance) {
                el.disabled = true;
                el.classList.add('opacity-50', 'cursor-not-allowed');
            }
        });
    } else {
        document.getElementById('maintenance-banner').classList.add('hidden');
        document.querySelectorAll('button, input, select, textarea').forEach(el => {
            el.disabled = false;
            el.classList.remove('opacity-50', 'cursor-not-allowed');
        });
    }

    // 6. Cập nhật hàng chờ (Queue) từ state
    updateQueueUI(state.queue_indices);
}

// HÀM MỚI: Tự động render toàn bộ UI cho các Lanes
function renderAllLanesUI(lanes) {
    const laneStatusContainer = document.getElementById('lane-status-container');
    const sensorStatusContainer = document.getElementById('sensor-status');
    const relayTestContainer = document.getElementById('manual-relay-test-container');
    const mockLaneContainer = document.getElementById('mock-lane-container');

    if (!laneStatusContainer || !sensorStatusContainer || !relayTestContainer || !mockLaneContainer) return;

    // Xóa nội dung cũ
    laneStatusContainer.innerHTML = '';
    sensorStatusContainer.innerHTML = '';
    relayTestContainer.innerHTML = '';
    mockLaneContainer.innerHTML = '';

    // Tính toán số cột (tối đa 3)
    const numLanes = lanes.length;
    const gridCols = Math.min(numLanes, 3);
    sensorStatusContainer.className = `grid grid-cols-2 md:grid-cols-${gridCols} gap-3`;
    relayTestContainer.className = `grid grid-cols-2 md:grid-cols-${gridCols} gap-3 mb-4`;
    mockLaneContainer.className = `grid grid-cols-1 md:grid-cols-${gridCols} gap-3`;


    lanes.forEach((lane, i) => {
        const isSortingLane = lane.push_pin !== null || lane.pull_pin !== null;
        const hasSensor = lane.sensor_pin !== null;

        // 1. Render Trang Chủ (lane-status-container)
        const laneStatusHTML = `
            <div id="lane-${i}" class="border border-gray-700 p-3 rounded-md">
                <div class="flex justify-between items-center mb-1">
                    <span id="lane-${i}-name" class="font-bold">${lane.name}</span>
                    <span id="status-text-${i}" class="text-xs px-2 py-1 rounded-full bg-gray-700 transition-all">Sẵn sàng</span>
                </div>
                <div class="flex justify-between text-sm mb-1">
                    <span>Đã đếm:</span>
                    <span id="lane-${i}-count" class="font-bold text-lg">0</span>
                </div>
                <div class="flex justify-between text-xs ${hasSensor ? '' : 'opacity-30'}">
                    <span>Cảm biến:</span>
                    <div id="lane-${i}-sensor" class="sensor-light ${hasSensor ? 'sensor-inactive' : ''}"></div>
                </div>
                <div class="flex justify-between text-xs mt-1">
                    <span>Relay Thu:</span>
                    <div id="lane-${i}-grab" class="relay-light ${isSortingLane ? 'relay-inactive' : 'relay-disabled'}"></div>
                </div>
                <div class="flex justify-between text-xs mt-1">
                    <span>Relay Đẩy:</span>
                    <div id="lane-${i}-push" class="relay-light ${isSortingLane ? 'relay-inactive' : 'relay-disabled'}"></div>
                </div>
            </div>
        `;
        laneStatusContainer.innerHTML += laneStatusHTML;

        // 2. Render Trang Test (sensor-status)
        const sensorTestHTML = `
            <div class="bg-gray-900 p-3 rounded-lg text-center ${hasSensor ? '' : 'opacity-30'}">
                <p class="text-sm text-gray-400 mb-2">${lane.name}</p>
                <div id="test-sensor-${i}" class="sensor-light ${hasSensor ? 'sensor-inactive' : ''} mx-auto mt-1 w-6 h-6"></div>
            </div>
        `;
        sensorStatusContainer.innerHTML += sensorTestHTML;

        // 3. Render Trang Test (manual-relay-test-container)
        let relayTestHTML;
        if (isSortingLane) {
            relayTestHTML = `
                <div class="bg-gray-900 p-3 rounded-lg text-center">
                    <h4 class="font-semibold mb-2 text-white">${lane.name}</h4>
                    <button onclick="testRelay(${i},'grab')"
                        class="test-btn-${i}-grab bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded mr-2 text-sm">Thu</button>
                    <button onclick="testRelay(${i},'push')"
                        class="test-btn-${i}-push bg-red-600 hover:bg-red-700 text-white px-3 py-1 rounded text-sm">Đẩy</button>
                </div>
            `;
        } else {
            relayTestHTML = `
                <div class="bg-gray-900 p-3 rounded-lg text-center opacity-50">
                    <h4 class="font-semibold mb-2 text-white">${lane.name}</h4>
                    <span class="text-xs text-gray-500">Lane đi thẳng</span>
                </div>
            `;
        }
        relayTestContainer.innerHTML += relayTestHTML;

        // 4. Render Trang Test (mock-lane-container)
        let mockLaneHTML;
        if (hasSensor) {
            mockLaneHTML = `
                <div class="bg-gray-800 border border-gray-700 rounded-lg p-3 space-y-3">
                    <h4 class="text-white font-semibold text-center">${lane.name}</h4>
                    <p class="text-xs text-gray-400 text-center">Sensor pin: <span>${lane.sensor_pin}</span></p>
                    <p id="mock-lane-status-${i}" class="mock-lane-status text-sm text-center text-green-400">KHÔNG KÍCH HOẠT (HIGH)</p>
                    <div class="flex flex-col space-y-2">
                        <button id="mock-btn-active-${i}" class="bg-red-600 hover:bg-red-700 text-white font-semibold py-1 px-3 rounded" onclick="setMockSensor(${i}, true)">Giả lập phát hiện</button>
                        <button id="mock-btn-idle-${i}" class="bg-green-600 hover:bg-green-700 text-white font-semibold py-1 px-3 rounded" onclick="setMockSensor(${i}, false)">Trở về bình thường</button>
                    </div>
                </div>
            `;
        } else {
            mockLaneHTML = `
                <div class="bg-gray-800 border border-gray-700 rounded-lg p-3 space-y-3 opacity-50">
                    <h4 class="text-white font-semibold text-center">${lane.name}</h4>
                    <p class="text-xs text-gray-400 text-center mt-2">Lane này không có Sensor để mô phỏng.</p>
                </div>
            `;
        }
        mockLaneContainer.innerHTML += mockLaneHTML;
    });
}

// Cập nhật UI hàng chờ
function updateQueueUI(qrQueueIndices) {
    const qrQueueContainer = document.getElementById("qr-queue-container");
    const qrEmptyMessage = document.getElementById("qr-queue-empty");

    if (qrQueueContainer && qrEmptyMessage) {
        qrQueueContainer.querySelectorAll('.queue-item-qr').forEach(el => el.remove());

        if (!qrQueueIndices || qrQueueIndices.length === 0) {
            qrEmptyMessage.classList.remove('hidden');
        } else {
            qrEmptyMessage.classList.add('hidden');
            const colors = ['bg-blue-600', 'bg-green-600', 'bg-yellow-500', 'bg-purple-600', 'bg-pink-600'];

            qrQueueIndices.forEach((laneIndex, i) => {
                const laneName = laneNamesMap[laneIndex] || `Lane ${laneIndex + 1}`;
                const colorClass = colors[laneIndex % colors.length] || 'bg-gray-600';

                const itemEl = document.createElement("span");
                itemEl.className = `queue-item-qr text-xs font-bold text-white px-3 py-1 rounded-full ${colorClass}`;

                if (i === 0) {
                    itemEl.innerHTML = `→ ${laneName} (Next)`;
                    itemEl.classList.add('ring-2', 'ring-white', 'shadow-lg');
                } else {
                    itemEl.textContent = laneName;
                }
                qrQueueContainer.appendChild(itemEl);
            });
        }
    }
}

function showQrOverlay(text, bgColorClass) {
    const overlay = document.getElementById("qr-overlay");
    if (!overlay) return;
    if (qrTimer) clearTimeout(qrTimer);
    overlay.textContent = text;
    overlay.className = ``;
    overlay.classList.add('absolute', 'top-4', 'right-4', 'p-2', 'px-4', 'rounded-lg', 'text-xl', 'font-bold', 'text-white', 'opacity-0', 'transition-opacity', 'duration-500', 'shadow-lg');
    overlay.classList.add(...bgColorClass.split(' '));
    setTimeout(() => overlay.style.opacity = '1', 10);
    qrTimer = setTimeout(() => {
        overlay.style.opacity = '0';
    }, 2500);
}

// ===== CÁC HÀM TƯƠNG TÁC =====

function resetMaintenance() {
    _fetch('/api/reset_maintenance', { method: 'POST' })
        .then(res => res.json())
        .then(data => {
            if (data?.error) {
                addLog('error', data.error);
            } else {
                addLog('info', data.message || 'Đã gửi yêu cầu thoát bảo trì.');
            }
        })
        .catch(err => {
            if (err?.isAuthError) return;
            addLog('error', `Không thể reset bảo trì: ${err.message}`);
        });
}
function resetCount(laneIndex) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify({ action: "reset_count", lane_index: laneIndex }));
}

function _fetch(url, options = {}) {
    return fetch(url, options).then(response => {
        if (response.status === 401) {
            addLog("error", "Máy chủ yêu cầu đăng nhập để sử dụng chức năng này.");
            const err = new Error("Unauthorized");
            err.isAuthError = true;
            throw err;
        }
        return response;
    });
}

// HÀM LOAD CONFIG ĐÃ SỬA
function loadConfig() {
    _fetch("/config")
        .then(res => {
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            return res.json();
        })
        .then(cfg => {
            document.getElementById("config-full-json").value = JSON.stringify(cfg, null, 4);

            // Cập nhật maps logic
            laneNamesMap = {};
            laneIdMap = {};
            sensorPinMap = {};
            let newLaneCount = 0;
            if (cfg.lanes_config) {
                newLaneCount = cfg.lanes_config.length;
                cfg.lanes_config.forEach((lane, i) => {
                    laneNamesMap[i] = lane.name;
                    laneIdMap[i] = lane.id;
                    sensorPinMap[i] = lane.sensor_pin;
                });
            }

            // Nếu số lượng lane thay đổi, render lại UI
            if (newLaneCount !== uiLaneCount) {
                renderAllLanesUI(cfg.lanes_config);
                uiLaneCount = newLaneCount;
            }

            addLog("success", "Đã tải cấu hình từ server.");
        })
        .catch(err => {
            if (err?.isAuthError) return;
            addLog("error", "Không thể tải cấu hình.");
            console.error("Lỗi fetch /config:", err);
        });
}

// HÀM SAVE CONFIG ĐÃ SỬA
function saveConfig() {
    let configPayload = null;

    try {
        const configJsonText = document.getElementById('config-full-json').value;
        configPayload = JSON.parse(configJsonText);

        if (typeof configPayload !== 'object' || configPayload === null || Array.isArray(configPayload)) {
            throw new Error("Dữ liệu config phải là một đối tượng JSON (Object).");
        }

    } catch (e) {
        addLog('error', `Lỗi JSON Cấu hình: ${e.message}`);
        return;
    }

    _fetch("/update_config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(configPayload) // Gửi thẳng object đã parse
    })
        .then(res => {
            if (!res.ok) throw new Error(`Lỗi server: ${res.status}`);
            return res.json();
        })
        .then(data => {
            const msg = data.message || "Đã lưu cấu hình mới.";
            addLog(data.restart_required ? "warn" : "success", msg);

            // Tải lại config và render lại UI
            loadConfig();
        })
        .catch(err => {
            if (err?.isAuthError) return;
            addLog("error", `Không thể lưu config: ${err.message}`);
        });
}

function testRelay(laneIndex, action) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        addLog("error", "Không thể test: Mất kết nối WebSocket.");
        return;
    }
    ws.send(JSON.stringify({ action: "test_relay", lane_index: laneIndex, relay_action: action }));
    addLog("info", `Test ${action.toUpperCase()} cho Lane ${laneIndex + 1}...`);
}

function testAllRelays() {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        addLog("error", "Không thể test: Mất kết nối WebSocket.");
        return;
    }
    ws.send(JSON.stringify({ action: "test_all_relays" }));
    addLog("info", "Đang test tuần tự các Lane có Relay...");
}

function setMockSensor(index, active) {
    const pin = sensorPinMap[index];
    const laneName = laneNamesMap[index];

    if (pin === null || pin === undefined) {
        addLog('error', `Lane/Sensor ${laneName} không có sensor pin để mô phỏng.`);
        return;
    }
    const state = active ? 0 : 1;
    const payload = { state: state, lane_index: index };

    _fetch('/api/mock_gpio', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    })
        .then(res => res.json())
        .then(data => {
            if (data?.error) {
                addLog('error', data.error);
            } else {
                const stateLabel = active ? 'PHÁT HIỆN' : 'BÌNH THƯỜNG';
                addLog('info', `Đã đặt cảm biến ${data.lane} về trạng thái ${stateLabel} (pin ${data.pin}).`);
            }
        })
        .catch(err => {
            if (err?.isAuthError) return;
            addLog('error', `Không thể mô phỏng cảm biến: ${err.message}`);
        });
}
function toggleAutoTest() {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
        addLog("error", "Không thể test: Mất kết nối WebSocket.");
        return;
    }
    autoTestEnabled = !autoTestEnabled;
    ws.send(JSON.stringify({ action: "toggle_auto_test", enabled: autoTestEnabled }));

    const btn = document.getElementById("toggle-auto-test");
    if (autoTestEnabled) {
        btn.textContent = "🛑 Tắt Auto Test Sensor";
        btn.classList.remove("bg-blue-600", "hover:bg-blue-700");
        btn.classList.add("bg-red-600", "hover:bg-red-700");
        addLog("warn", "ĐÃ BẬT chế độ Auto-Test (Sensor -> Relay).");
    } else {
        btn.textContent = "🔄 Bật Auto Test Sensor";
        btn.classList.add("bg-blue-600", "hover:bg-blue-700");
        btn.classList.remove("bg-red-600", "hover:bg-red-700");
        addLog("info", "Đã tắt chế độ Auto-Test.");
    }
}

function resetQueueManual() {
    showConfirmModal('Bạn có chắc muốn reset HÀNG CHỜ XỬ LÝ?', () => {
        _fetch('/api/queue/reset', { method: 'POST' })
            .then(res => res.json())
            .then(data => {
                if (data?.error) {
                    addLog('error', data.error);
                } else {
                    addLog('info', data.message || 'Đã reset hàng chờ thành công.');
                }
            })
            .catch(err => {
                if (err?.isAuthError) return;
                addLog('error', `Không thể reset hàng chờ: ${err.message}`);
            });
    });
}

function loadSortChart() {
    _fetch("/api/sort_log")
        .then(res => {
            if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
            return res.json();
        })
        .then(data => {
            renderCharts(data);
        })
        .catch(err => {
            addLog("error", "Không thể tải dữ liệu thống kê.");
            console.error("Lỗi fetch /api/sort_log:", err);
        });
}

function renderCharts(data) {
    const today = new Date().toISOString().split('T')[0];
    const days = Object.keys(data).sort().slice(-7);
    const labels = days;

    const laneIndices = Object.keys(laneNamesMap).map(Number).sort((a, b) => a - b);
    const laneNames = laneIndices.map(i => laneNamesMap[i] || `Lane ${i + 1}`);

    const colors = ['#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899'];

    const chartDatasets = laneNames.map((name, i) => {
        return {
            label: name,
            data: labels.map(day => data[day]?.[name] || 0),
            backgroundColor: colors[i % colors.length],
        };
    });

    const todayData = data[today] || {};
    const doughnutData = laneNames.map(name => todayData[name] || 0);
    const totalToday = doughnutData.reduce((a, b) => a + b, 0);

    const ctxBar = document.getElementById('sort-chart-bar')?.getContext('2d');
    if (!ctxBar) return;
    if (sortBarChart) sortBarChart.destroy();
    sortBarChart = new Chart(ctxBar, {
        type: 'bar',
        data: { labels, datasets: chartDatasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { stacked: true, ticks: { color: '#9ca3af' } },
                y: { stacked: true, beginAtZero: true, ticks: { color: '#9ca3af', stepSize: 1 } }
            },
            plugins: {
                legend: { labels: { color: '#d1d5db' } },
                title: { display: true, text: 'Sản Lượng 7 Ngày Gần Nhất', color: '#fff' }
            }
        }
    });

    const ctxDoughnut = document.getElementById('sort-chart-doughnut')?.getContext('2d');
    if (!ctxDoughnut) return;
    if (sortDoughnutChart) sortDoughnutChart.destroy();
    sortDoughnutChart = new Chart(ctxDoughnut, {
        type: 'doughnut',
        data: {
            labels: laneNames,
            datasets: [{
                data: doughnutData,
                backgroundColor: colors,
                borderColor: '#4b5563'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'bottom', labels: { color: '#d1d5db' } },
                title: { display: true, text: `Hôm Nay (Tổng: ${totalToday})`, color: '#fff' }
            }
        }
    });
}

document.addEventListener("DOMContentLoaded", () => {
    const sidebar = document.getElementById("sidebar");
    const main = document.getElementById("main-content");
    const content = document.getElementById("sidebar-content");

    sidebar.addEventListener("click", (event) => {
        if (event.target.tagName === "A" || event.target.closest("a")) return;

        const expanded = sidebar.classList.toggle("expanded");

        if (expanded) {
            sidebar.style.width = "14rem";
            main.style.marginLeft = "14rem";
            content.style.width = "14rem";
            content.style.opacity = "1";
        } else {
            sidebar.style.width = "6px";
            main.style.marginLeft = "0.5rem";
            content.style.width = "0";
            content.style.opacity = "0";
        }
    });
}); 