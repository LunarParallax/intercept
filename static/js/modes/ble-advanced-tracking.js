/**
 * BLE Advanced Tracking Features
 * Enhanced Bluetooth Low Energy tracking capabilities
 */

const BLEAdvancedTracking = (function() {
    'use strict';

    // State
    let trackingEnabled = false;
    let movementDetectionEnabled = false;
    let triangulationActive = false;
    let alertRules = [];
    let historicalData = new Map();
    let movementPatterns = new Map();
    let fingerprintCache = new Map();
    let triangulationData = new Map();

    // Configuration
    const config = {
        movementThreshold: 3, // RSSI change threshold for movement detection
        historyMaxAge: 300000, // 5 minutes
        fingerprintSamples: 10,
        triangulationMinReaders: 2,
        alertCheckInterval: 5000
    };

    // Alert rule types
    const AlertTypes = {
        MOVEMENT: 'movement',
        PROXIMITY: 'proximity',
        FINGERPRINT_CHANGE: 'fingerprint_change',
        GEO_FENCE: 'geo_fence',
        SIGNAL_LOSS: 'signal_loss'
    };

    /**
     * Initialize advanced tracking features
     */
    function init() {
        console.log('[BLE-ADV] Initializing advanced tracking features');
        loadSettings();
        setupEventListeners();
        startAlertChecker();
    }

    /**
     * Load settings from localStorage
     */
    function loadSettings() {
        try {
            const saved = localStorage.getItem('ble_advanced_tracking');
            if (saved) {
                const settings = JSON.parse(saved);
                trackingEnabled = settings.trackingEnabled ?? false;
                movementDetectionEnabled = settings.movementDetectionEnabled ?? false;
                alertRules = settings.alertRules ?? [];
                config.movementThreshold = settings.movementThreshold ?? config.movementThreshold;
            }
        } catch (e) {
            console.error('[BLE-ADV] Failed to load settings:', e);
        }
    }

    /**
     * Save settings to localStorage
     */
    function saveSettings() {
        try {
            localStorage.setItem('ble_advanced_tracking', JSON.stringify({
                trackingEnabled,
                movementDetectionEnabled,
                alertRules,
                movementThreshold: config.movementThreshold
            }));
        } catch (e) {
            console.error('[BLE-ADV] Failed to save settings:', e);
        }
    }

    /**
     * Setup UI event listeners
     */
    function setupEventListeners() {
        // Toggle advanced tracking
        const toggleBtn = document.getElementById('bleAdvancedToggle');
        if (toggleBtn) {
            toggleBtn.addEventListener('click', toggleTracking);
            updateToggleButton(toggleBtn);
        }

        // Movement detection toggle
        const movementToggle = document.getElementById('bleMovementToggle');
        if (movementToggle) {
            movementToggle.addEventListener('click', toggleMovementDetection);
            updateMovementButton(movementToggle);
        }

        // Alert rules management
        const alertForm = document.getElementById('bleAlertForm');
        if (alertForm) {
            alertForm.addEventListener('submit', handleAlertFormSubmit);
        }

        const alertList = document.getElementById('bleAlertList');
        if (alertList) {
            renderAlertRules();
        }

        // Triangulation controls
        const triangulationBtn = document.getElementById('bleTriangulationBtn');
        if (triangulationBtn) {
            triangulationBtn.addEventListener('click', toggleTriangulation);
        }

        // Export tracking data
        const exportBtn = document.getElementById('bleExportTracking');
        if (exportBtn) {
            exportBtn.addEventListener('click', exportTrackingData);
        }
    }

    /**
     * Toggle advanced tracking
     */
    function toggleTracking() {
        trackingEnabled = !trackingEnabled;
        saveSettings();
        
        const btn = document.getElementById('bleAdvancedToggle');
        if (btn) updateToggleButton(btn);

        if (trackingEnabled) {
            console.log('[BLE-ADV] Advanced tracking enabled');
            showNotification('BLE Tracking', 'Advanced tracking features enabled', 'success');
        } else {
            console.log('[BLE-ADV] Advanced tracking disabled');
            showNotification('BLE Tracking', 'Advanced tracking features disabled', 'info');
        }
    }

    /**
     * Update tracking toggle button state
     */
    function updateToggleButton(btn) {
        btn.classList.toggle('active', trackingEnabled);
        btn.textContent = trackingEnabled ? 'Tracking: ON' : 'Tracking: OFF';
    }

    /**
     * Toggle movement detection
     */
    function toggleMovementDetection() {
        movementDetectionEnabled = !movementDetectionEnabled;
        saveSettings();

        const btn = document.getElementById('bleMovementToggle');
        if (btn) updateMovementButton(btn);

        if (movementDetectionEnabled) {
            console.log('[BLE-ADV] Movement detection enabled');
            showNotification('Movement Detection', 'Device movement detection enabled', 'success');
        } else {
            console.log('[BLE-ADV] Movement detection disabled');
            showNotification('Movement Detection', 'Device movement detection disabled', 'info');
        }
    }

    /**
     * Update movement detection button state
     */
    function updateMovementButton(btn) {
        btn.classList.toggle('active', movementDetectionEnabled);
        btn.textContent = movementDetectionEnabled ? 'Movement: ON' : 'Movement: OFF';
    }

    /**
     * Process device update for advanced tracking
     */
    function processDeviceUpdate(device) {
        if (!trackingEnabled) return;

        const deviceId = device.device_id || device.address;
        
        // Update historical data
        updateHistory(device);

        // Check for movement
        if (movementDetectionEnabled) {
            detectMovement(device);
        }

        // Update fingerprint
        updateFingerprint(device);

        // Check alert rules
        checkAlertRules(device);

        // Update triangulation if active
        if (triangulationActive) {
            updateTriangulation(device);
        }
    }

    /**
     * Update historical data for a device
     */
    function updateHistory(device) {
        const deviceId = device.device_id || device.address;
        const now = Date.now();

        if (!historicalData.has(deviceId)) {
            historicalData.set(deviceId, []);
        }

        const history = historicalData.get(deviceId);
        history.push({
            timestamp: now,
            rssi: device.rssi_current,
            rssi_ema: device.rssi_ema,
            estimated_distance: device.estimated_distance_m,
            agent: getCurrentAgentName(),
            proximity_band: device.proximity_band
        });

        // Prune old entries
        const cutoff = now - config.historyMaxAge;
        while (history.length > 0 && history[0].timestamp < cutoff) {
            history.shift();
        }

        // Calculate trends
        calculateTrends(deviceId);
    }

    /**
     * Calculate signal trends for a device
     */
    function calculateTrends(deviceId) {
        const history = historicalData.get(deviceId);
        if (!history || history.length < 5) return;

        const recent = history.slice(-10);
        const rssiValues = recent.map(h => h.rssi).filter(r => r != null);
        
        if (rssiValues.length < 2) return;

        // Calculate moving average and trend direction
        const avg = rssiValues.reduce((a, b) => a + b, 0) / rssiValues.length;
        const firstHalf = rssiValues.slice(0, Math.floor(rssiValues.length / 2));
        const secondHalf = rssiValues.slice(Math.floor(rssiValues.length / 2));
        
        const firstAvg = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
        const secondAvg = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;
        
        const trend = secondAvg - firstAvg;
        
        // Store trend data
        const device = getDeviceById(deviceId);
        if (device) {
            device.signal_trend = trend > 1 ? 'approaching' : trend < -1 ? 'receding' : 'stable';
            device.signal_avg = Math.round(avg);
        }
    }

    /**
     * Detect device movement based on RSSI changes
     */
    function detectMovement(device) {
        const deviceId = device.device_id || device.address;
        const history = historicalData.get(deviceId);
        
        if (!history || history.length < 3) return;

        const recent = history.slice(-5);
        const rssiChanges = [];
        
        for (let i = 1; i < recent.length; i++) {
            if (recent[i].rssi != null && recent[i-1].rssi != null) {
                rssiChanges.push(Math.abs(recent[i].rssi - recent[i-1].rssi));
            }
        }

        if (rssiChanges.length === 0) return;

        const avgChange = rssiChanges.reduce((a, b) => a + b, 0) / rssiChanges.length;
        const maxChange = Math.max(...rssiChanges);

        // Detect movement patterns
        let movementType = 'stationary';
        if (avgChange > config.movementThreshold) {
            movementType = 'moving';
        } else if (maxChange > config.movementThreshold * 2) {
            movementType = 'intermittent';
        }

        // Store movement state
        const prevState = movementPatterns.get(deviceId);
        if (prevState !== movementType) {
            movementPatterns.set(deviceId, {
                type: movementType,
                detected_at: Date.now(),
                confidence: Math.min(1, avgChange / (config.movementThreshold * 3))
            });

            // Trigger movement alert if transitioning from stationary to moving
            if (prevState === 'stationary' && movementType === 'moving') {
                triggerAlert(AlertTypes.MOVEMENT, device, {
                    message: `Device ${device.name || device.address} started moving`,
                    avgChange,
                    maxChange
                });
            }
        }

        // Update device with movement info
        const dev = getDeviceById(deviceId);
        if (dev) {
            dev.movement_state = movementType;
            dev.movement_confidence = movementPatterns.get(deviceId)?.confidence || 0;
        }
    }

    /**
     * Update device fingerprint based on advertising data patterns
     */
    function updateFingerprint(device) {
        const deviceId = device.device_id || device.address;
        
        const fingerprint = {
            manufacturer_id: device.manufacturer_id,
            service_uuids: device.service_uuids?.sort().join(',') || '',
            tx_power: device.tx_power,
            appearance: device.appearance,
            adv_interval_pattern: analyzeAdvertisingInterval(device),
            packet_structure: analyzePacketStructure(device)
        };

        if (!fingerprintCache.has(deviceId)) {
            fingerprintCache.set(deviceId, {
                samples: [],
                stable: false,
                baseline: null
            });
        }

        const cache = fingerprintCache.get(deviceId);
        cache.samples.push(fingerprint);

        // Keep last N samples
        if (cache.samples.length > config.fingerprintSamples) {
            cache.samples.shift();
        }

        // Determine if fingerprint is stable
        if (cache.samples.length >= 5 && !cache.stable) {
            const allMatch = cache.samples.every(s => 
                s.manufacturer_id === cache.samples[0].manufacturer_id &&
                s.service_uuids === cache.samples[0].service_uuids
            );
            
            if (allMatch) {
                cache.stable = true;
                cache.baseline = cache.samples[0];
            }
        }

        // Check for fingerprint changes
        if (cache.stable && cache.baseline) {
            const current = cache.samples[cache.samples.length - 1];
            if (current.manufacturer_id !== cache.baseline.manufacturer_id ||
                current.service_uuids !== cache.baseline.service_uuids) {
                
                triggerAlert(AlertTypes.FINGERPRINT_CHANGE, device, {
                    message: `Device fingerprint changed - possible spoofing or mode change`,
                    old_manufacturer: cache.baseline.manufacturer_id,
                    new_manufacturer: current.manufacturer_id
                });
            }
        }

        // Store fingerprint hash for quick comparison
        const dev = getDeviceById(deviceId);
        if (dev) {
            dev.fingerprint_hash = generateFingerprintHash(fingerprint);
            dev.fingerprint_stable = cache.stable;
        }
    }

    /**
     * Analyze advertising interval pattern
     */
    function analyzeAdvertisingInterval(device) {
        // This would require timestamp analysis of multiple packets
        // Placeholder for future implementation
        return device.adv_interval || 'unknown';
    }

    /**
     * Analyze packet structure for fingerprinting
     */
    function analyzePacketStructure(device) {
        // Analyze the structure of advertising packet
        const structure = {
            has_name: !!device.name,
            name_complete: device.name && !device.name.includes('...'),
            has_tx_power: device.tx_power != null,
            has_service_data: !!device.service_data,
            manufacturer_data_size: device.manufacturer_data?.length || 0
        };
        
        return JSON.stringify(structure);
    }

    /**
     * Generate fingerprint hash
     */
    function generateFingerprintHash(fingerprint) {
        const str = JSON.stringify(fingerprint);
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        return hash.toString(16);
    }

    /**
     * Toggle triangulation mode
     */
    function toggleTriangulation() {
        triangulationActive = !triangulationActive;
        
        const btn = document.getElementById('bleTriangulationBtn');
        if (btn) {
            btn.classList.toggle('active', triangulationActive);
            btn.textContent = triangulationActive ? 'Triangulation: ON' : 'Triangulation: OFF';
        }

        if (triangulationActive) {
            console.log('[BLE-ADV] Triangulation enabled');
            showNotification('Triangulation', 'Multi-point triangulation enabled', 'info');
        } else {
            console.log('[BLE-ADV] Triangulation disabled');
            triangulationData.clear();
        }
    }

    /**
     * Update triangulation data from multiple readers
     */
    function updateTriangulation(device) {
        const deviceId = device.device_id || device.address;
        const agentName = getCurrentAgentName();

        if (!triangulationData.has(deviceId)) {
            triangulationData.set(deviceId, {
                readings: new Map(),
                position: null,
                confidence: 0
            });
        }

        const data = triangulationData.get(deviceId);
        data.readings.set(agentName, {
            rssi: device.rssi_current,
            distance: device.estimated_distance_m,
            timestamp: Date.now(),
            agent_position: getAgentPosition(agentName) // Would need agent location data
        });

        // Calculate position if we have enough readers
        if (data.readings.size >= config.triangulationMinReaders) {
            calculatePosition(deviceId);
        }

        // Update device with triangulation info
        const dev = getDeviceById(deviceId);
        if (dev && data.position) {
            dev.triangulated_position = data.position;
            dev.triangulation_confidence = data.confidence;
        }
    }

    /**
     * Calculate device position using trilateration
     */
    function calculatePosition(deviceId) {
        const data = triangulationData.get(deviceId);
        const readings = Array.from(data.readings.values());

        // Simple trilateration (placeholder - real implementation needs proper math)
        // This is a simplified version for demonstration
        
        let totalWeight = 0;
        let weightedX = 0;
        let weightedY = 0;

        readings.forEach(reading => {
            if (!reading.agent_position || reading.distance == null) return;
            
            const weight = 1 / (reading.distance + 1); // Closer readings have more weight
            totalWeight += weight;
            weightedX += reading.agent_position.x * weight;
            weightedY += reading.agent_position.y * weight;
        });

        if (totalWeight > 0) {
            data.position = {
                x: weightedX / totalWeight,
                y: weightedY / totalWeight
            };
            data.confidence = Math.min(1, readings.length / 4); // More readers = higher confidence
        }
    }

    /**
     * Add alert rule
     */
    function addAlertRule(rule) {
        alertRules.push({
            id: generateRuleId(),
            ...rule,
            created_at: Date.now(),
            enabled: true
        });
        saveSettings();
        renderAlertRules();
    }

    /**
     * Remove alert rule
     */
    function removeAlertRule(ruleId) {
        alertRules = alertRules.filter(r => r.id !== ruleId);
        saveSettings();
        renderAlertRules();
    }

    /**
     * Toggle alert rule
     */
    function toggleAlertRule(ruleId) {
        const rule = alertRules.find(r => r.id === ruleId);
        if (rule) {
            rule.enabled = !rule.enabled;
            saveSettings();
            renderAlertRules();
        }
    }

    /**
     * Handle alert form submission
     */
    function handleAlertFormSubmit(e) {
        e.preventDefault();
        
        const formData = new FormData(e.target);
        const rule = {
            type: formData.get('alert_type'),
            device_filter: formData.get('device_filter'),
            threshold: parseFloat(formData.get('threshold')),
            action: formData.get('action'),
            notification: formData.get('notification') === 'on'
        };

        addAlertRule(rule);
        e.target.reset();
        showNotification('Alert Rule', 'Rule added successfully', 'success');
    }

    /**
     * Render alert rules list
     */
    function renderAlertRules() {
        const container = document.getElementById('bleAlertList');
        if (!container) return;

        if (alertRules.length === 0) {
            container.innerHTML = '<div class="empty-state">No alert rules configured</div>';
            return;
        }

        container.innerHTML = alertRules.map(rule => `
            <div class="alert-rule-item ${rule.enabled ? 'enabled' : 'disabled'}" data-rule-id="${rule.id}">
                <div class="rule-header">
                    <span class="rule-type">${rule.type}</span>
                    <span class="rule-filter">${rule.device_filter || 'All devices'}</span>
                </div>
                <div class="rule-details">
                    Threshold: ${rule.threshold} | Action: ${rule.action}
                </div>
                <div class="rule-actions">
                    <button onclick="BLEAdvancedTracking.toggleAlertRule('${rule.id}')" class="btn-sm">
                        ${rule.enabled ? 'Disable' : 'Enable'}
                    </button>
                    <button onclick="BLEAdvancedTracking.removeAlertRule('${rule.id}')" class="btn-sm btn-danger">
                        Delete
                    </button>
                </div>
            </div>
        `).join('');
    }

    /**
     * Check alert rules against device
     */
    function checkAlertRules(device) {
        alertRules.forEach(rule => {
            if (!rule.enabled) return;

            // Check device filter
            if (rule.device_filter) {
                const matchesFilter = device.name?.toLowerCase().includes(rule.device_filter.toLowerCase()) ||
                                    device.address?.toLowerCase().includes(rule.device_filter.toLowerCase());
                if (!matchesFilter) return;
            }

            // Check rule type
            switch (rule.type) {
                case AlertTypes.PROXIMITY:
                    if (device.estimated_distance_m < rule.threshold) {
                        triggerAlert(AlertTypes.PROXIMITY, device, {
                            message: `Device within ${rule.threshold}m`,
                            distance: device.estimated_distance_m
                        });
                    }
                    break;
                    
                case AlertTypes.SIGNAL_LOSS:
                    // Handled by timeout mechanism
                    break;
            }
        });
    }

    /**
     * Trigger alert
     */
    function triggerAlert(type, device, data) {
        const alert = {
            type,
            device_id: device.device_id || device.address,
            device_name: device.name,
            timestamp: Date.now(),
            ...data
        };

        console.log('[BLE-ADV] Alert triggered:', alert);

        // Show notification
        if (typeof showNotification === 'function') {
            showNotification(`BLE Alert: ${type}`, data.message || 'Alert triggered', 'warning');
        }

        // Could also send to server, play sound, etc.
        dispatchEvent(new CustomEvent('ble-alert', { detail: alert }));
    }

    /**
     * Start periodic alert checker
     */
    function startAlertChecker() {
        setInterval(() => {
            if (!trackingEnabled) return;

            // Check for signal loss
            const now = Date.now();
            historicalData.forEach((history, deviceId) => {
                if (history.length === 0) return;
                
                const lastSeen = history[history.length - 1].timestamp;
                const timeSinceLastSeen = now - lastSeen;
                
                // If no update in 30 seconds, consider it signal loss
                if (timeSinceLastSeen > 30000) {
                    const device = getDeviceById(deviceId);
                    if (device) {
                        triggerAlert(AlertTypes.SIGNAL_LOSS, device, {
                            message: `Signal lost from ${device.name || device.address}`,
                            last_seen: timeSinceLastSeen
                        });
                    }
                }
            });
        }, config.alertCheckInterval);
    }

    /**
     * Export tracking data
     */
    function exportTrackingData() {
        const exportData = {
            export_time: new Date().toISOString(),
            devices: [],
            alerts: []
        };

        historicalData.forEach((history, deviceId) => {
            exportData.devices.push({
                device_id: deviceId,
                history: history
            });
        });

        const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `ble-tracking-${Date.now()}.json`;
        a.click();
        URL.revokeObjectURL(url);

        showNotification('Export', 'Tracking data exported', 'success');
    }

    /**
     * Get device by ID from main BluetoothMode
     */
    function getDeviceById(deviceId) {
        if (typeof BluetoothMode !== 'undefined' && BluetoothMode.getDevices) {
            const devices = BluetoothMode.getDevices();
            return devices.find(d => d.device_id === deviceId);
        }
        return null;
    }

    /**
     * Get current agent name
     */
    function getCurrentAgentName() {
        if (typeof currentAgent === 'undefined' || currentAgent === 'local') {
            return 'Local';
        }
        if (typeof agents !== 'undefined') {
            const agent = agents.find(a => a.id == currentAgent);
            return agent ? agent.name : `Agent ${currentAgent}`;
        }
        return `Agent ${currentAgent}`;
    }

    /**
     * Get agent position (placeholder - would need actual agent location data)
     */
    function getAgentPosition(agentName) {
        // This would integrate with agent location system
        // For now, return null
        return null;
    }

    /**
     * Generate unique rule ID
     */
    function generateRuleId() {
        return 'rule_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    }

    /**
     * Get tracking statistics
     */
    function getStats() {
        return {
            tracking_enabled: trackingEnabled,
            movement_detection_enabled: movementDetectionEnabled,
            triangulation_active: triangulationActive,
            devices_tracked: historicalData.size,
            alert_rules_count: alertRules.length,
            fingerprints_cached: fingerprintCache.size
        };
    }

    /**
     * Clear all tracking data
     */
    function clearData() {
        historicalData.clear();
        movementPatterns.clear();
        fingerprintCache.clear();
        triangulationData.clear();
        console.log('[BLE-ADV] All tracking data cleared');
    }

    // Public API
    return {
        init,
        processDeviceUpdate,
        toggleTracking,
        toggleMovementDetection,
        toggleTriangulation,
        addAlertRule,
        removeAlertRule,
        toggleAlertRule,
        exportTrackingData,
        getStats,
        clearData,
        
        // Getters
        isEnabled: () => trackingEnabled,
        isMovementDetectionEnabled: () => movementDetectionEnabled,
        isTriangulationActive: () => triangulationActive
    };
})();

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        if (document.getElementById('bluetoothMode')) {
            BLEAdvancedTracking.init();
        }
    });
} else {
    if (document.getElementById('bluetoothMode')) {
        BLEAdvancedTracking.init();
    }
}

window.BLEAdvancedTracking = BLEAdvancedTracking;
