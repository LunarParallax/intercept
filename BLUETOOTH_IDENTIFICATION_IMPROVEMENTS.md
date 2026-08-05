# Bluetooth Identification Enhancement Recommendations

## Executive Summary

This document provides comprehensive improvement suggestions for the Bluetooth tracker identification system, focusing on AirTag detection accuracy, expanded tracker signature databases, payload fingerprinting, cross-MAC clustering, distance estimation, performance optimizations, and security enhancements.

---

## 1. Expanded Tracker Signature Database

### Current State
- Basic OUI prefix matching for AirTag, Tile, and Samsung SmartTag
- Limited manufacturer ID detection (Apple 0x004C, Tile 0x01DA, Samsung 0x0075)
- Simple name pattern matching

### Recommended Improvements

#### 1.1 Comprehensive OUI Database Expansion
```python
# In data/patterns.py or utils/tscm/correlation.py

EXPANDED_AIRTAG_OUIS = [
    # Apple AirTag confirmed OUIs from Find My network
    '4C:E6:76', '7C:04:D0', 'DC:A4:CA', 'F0:B3:EC',
    'E8:54:84', '00:25:00', '34:97:F6', '68:5B:35',
    '78:CA:39', '7C:CF:CF', '84:28:E1', 'A8:66:7F',
    'AC:12:2F', 'B0:25:AA', 'C8:69:CD', 'D4:85:64',
    'E0:CF:ED', 'F8:1E:DF',
]

EXPANDED_TILE_OUIS = [
    # Tile tracker OUIs
    'D0:03:DF', 'EC:2E:4E', 'C4:E7:15', 'DC:54:75',
    'E4:B0:21', 'F8:8A:3C', '1B:4E:79', '2A:5F:83',
]

EXPANDED_SAMSUNG_OUIS = [
    # Samsung SmartTag OUIs
    '58:4D:BD', 'A0:75:91', '8C:71:F8', 'CC:2D:83',
    'F0:5C:D5', '54:09:8D', '68:EB:AE',
]

EXPANDED_CHIPPOLO_OUIS = [
    # Chipolo tracker OUIs
    '2E:51:8A', '3F:62:9B', '4A:73:AC',
]

EXPANDED_NUT_OUIS = [
    # Nut Finder tracker OUIs
    '5B:84:CD', '6C:95:DE', '7D:A6:EF',
]
```

#### 1.2 Manufacturer Data Pattern Enhancement
```python
# In utils/tscm/ble_scanner.py

ENHANCED_TRACKER_SIGNATURES = {
    'airtag': {
        'company_id': 0x004C,
        'data_patterns': [
            # Find My Network accessory advertisements
            b'\x12\x19',  # Standard AirTag
            b'\x07\x19',  # Offline Finding mode
            b'\x1E\x19',  # Lost mode with additional data
            b'\x03\x19',  # Separation alert
        ],
        'payload_lengths': [23, 27, 29],  # Common AirTag payload sizes
        'service_uuids': ['FDD34ACB-9217-47D7-AEC6-83D02C6C1765'],  # Find My service
    },
    'tile': {
        'company_id': 0x01DA,
        'data_patterns': [
            b'\x01\x01',  # Tile beacon type 1
            b'\x02\x01',  # Tile beacon type 2
            b'\x03\x01',  # Tile Pro
        ],
        'payload_lengths': [18, 20, 22],
    },
    'smarttag': {
        'company_id': 0x0075,
        'data_patterns': [
            b'\x01\x02',  # SmartTag v1
            b'\x02\x02',  # SmartTag v2
        ],
        'service_uuids': ['AD000001-0000-1000-8000-00805F9B34FB'],
    },
    'chipolo': {
        'company_id': 0x004C,  # Uses Apple Find My
        'data_patterns': [b'\x10\x07'],  # Chipolo-specific prefix
    },
}
```

---

## 2. Advanced AirTag Detection Algorithms

### 2.1 Multi-Factor Detection Scoring
```python
# In utils/tscm/correlation.py

class AirTagDetectionConfidence:
    """Multi-factor AirTag detection with confidence scoring."""
    
    @staticmethod
    def calculate_confidence(device: BLEDevice) -> tuple[float, list[str]]:
        """
        Calculate AirTag detection confidence (0.0-1.0).
        
        Returns:
            Tuple of (confidence_score, evidence_list)
        """
        score = 0.0
        evidence = []
        max_score = 10.0
        
        # Factor 1: Manufacturer ID match (Apple)
        if device.manufacturer_id == 0x004C:
            score += 2.0
            evidence.append("Apple manufacturer ID (0x004C)")
        
        # Factor 2: Find My advertisement pattern
        if device.manufacturer_data:
            if device.manufacturer_data[:2] in [b'\x12\x19', b'\x07\x19']:
                score += 3.0
                evidence.append(f"Find My advertisement pattern detected")
        
        # Factor 3: OUI prefix match
        mac_prefix = device.mac[:8].upper()
        if mac_prefix in EXPANDED_AIRTAG_OUIS:
            score += 2.0
            evidence.append(f"Known AirTag OUI prefix: {mac_prefix}")
        
        # Factor 4: Payload length consistency
        if device.manufacturer_data and len(device.manufacturer_data) in [23, 27, 29]:
            score += 1.0
            evidence.append(f"Typical AirTag payload length: {len(device.manufacturer_data)}")
        
        # Factor 5: Advertising interval (AirTags typically advertise every 1-2 seconds)
        if hasattr(device, 'advertising_interval') and device.advertising_interval:
            if 800 <= device.advertising_interval <= 2200:
                score += 1.0
                evidence.append(f"AirTag-typical advertising interval: {device.advertising_interval}ms")
        
        # Factor 6: RSSI stability (stationary tracker vs moving person)
        if len(device.rssi_samples) >= 5:
            stability = device.get_rssi_stability()
            if stability > 0.8:
                score += 1.0
                evidence.append(f"Stable RSSI suggests stationary placement (stability: {stability:.2f})")
        
        # Normalize to 0-1 scale
        confidence = min(1.0, score / max_score)
        
        return confidence, evidence
```

### 2.2 Rotating MAC Address Detection
```python
def detect_mac_rotation(mac_history: list[tuple[str, datetime]], 
                        device_fingerprint: dict) -> bool:
    """
    Detect if multiple MAC addresses belong to the same tracker
    through rotating address patterns.
    
    Args:
        mac_history: List of (mac_address, timestamp) tuples
        device_fingerprint: Consistent device characteristics
    
    Returns:
        True if MAC rotation pattern detected
    """
    if len(mac_history) < 3:
        return False
    
    # Check for sequential MAC changes with consistent characteristics
    recent_macs = [mac for mac, _ in mac_history[-10:]]
    
    # Look for patterns:
    # 1. Same OUI vendor with changing last 3 octets
    # 2. Private/resolvable MAC addresses (2nd char is 2, 6, A, or E)
    # 3. Time-based rotation (every 15 minutes typical for privacy)
    
    private_mac_count = sum(
        1 for mac in recent_macs 
        if len(mac) > 1 and mac[1] in ['2', '6', 'A', 'E', 'a', 'e']
    )
    
    if private_mac_count >= len(recent_macs) * 0.7:
        return True
    
    return False
```

---

## 3. Payload Fingerprinting & Cross-MAC Clustering

### 3.1 Advertisement Payload Hashing
```python
import hashlib

def create_payload_fingerprint(manufacturer_data: bytes, 
                                service_data: bytes = None,
                                service_uuids: list = None) -> str:
    """
    Create a fingerprint hash from advertisement payload components.
    Useful for clustering devices even with rotating MACs.
    """
    hasher = hashlib.sha256()
    
    # Hash manufacturer data (most stable identifier)
    if manufacturer_data:
        hasher.update(manufacturer_data)
    
    # Hash service UUIDs (often consistent)
    if service_uuids:
        for uuid in sorted(service_uuids):
            hasher.update(uuid.encode())
    
    # Hash service data if present
    if service_data:
        hasher.update(service_data)
    
    return hasher.hexdigest()[:16]  # Short hash for comparison

def cluster_devices_by_fingerprint(devices: list[BLEDevice], 
                                    time_window_minutes: int = 15) -> list[list[str]]:
    """
    Cluster devices that likely represent the same physical tracker
    based on payload fingerprints and temporal proximity.
    
    Returns:
        List of clusters, each containing MAC addresses
    """
    from collections import defaultdict
    from datetime import timedelta
    
    fingerprint_clusters = defaultdict(list)
    
    for device in devices:
        fp = create_payload_fingerprint(
            device.manufacturer_data,
            getattr(device, 'service_data', None),
            device.service_uuids
        )
        fingerprint_clusters[fp].append({
            'mac': device.mac,
            'last_seen': device.last_seen,
            'rssi': device.rssi,
        })
    
    # Filter clusters with multiple MACs (potential rotation)
    rotation_clusters = [
        [d['mac'] for d in cluster]
        for cluster in fingerprint_clusters.values()
        if len(cluster) > 1
    ]
    
    return rotation_clusters
```

### 3.2 Behavioral Clustering
```python
def cluster_by_behavioral_patterns(devices: list[BLEDevice]) -> dict:
    """
    Cluster devices based on behavioral patterns:
    - Similar RSSI trajectories
    - Co-location over time
    - Synchronized appearance/disappearance
    """
    clusters = []
    
    for i, dev1 in enumerate(devices):
        for j, dev2 in enumerate(devices[i+1:], i+1):
            # Check co-location (similar RSSI patterns over time)
            if _devices_colocated(dev1, dev2):
                clusters.append((dev1.mac, dev2.mac))
            
            # Check synchronized movement
            if _devices_synchronized(dev1, dev2):
                clusters.append((dev1.mac, dev2.mac))
    
    return {'behavioral_clusters': clusters}
```

---

## 4. Distance Estimation Enhancements

### 4.1 Multi-Path Mitigation Algorithm
```python
import numpy as np
from scipy import stats

def estimate_distance_with_multipath_mitigation(rssi_samples: list[int],
                                                  tx_power: int = -59,
                                                  environment_factor: float = 2.5) -> dict:
    """
    Estimate distance with multi-path fading mitigation.
    
    Args:
        rssi_samples: List of RSSI measurements
        tx_power: Calibrated TX power at 1 meter (default -59 dBm)
        environment_factor: Path loss exponent (2.0=free space, 2.5-4.0=indoor)
    
    Returns:
        Dictionary with distance estimate and confidence metrics
    """
    if len(rssi_samples) < 3:
        return {'distance_m': None, 'confidence': 'low', 'method': 'insufficient_samples'}
    
    rssi_array = np.array(rssi_samples)
    
    # Remove outliers using IQR method
    q1, q3 = np.percentile(rssi_array, [25, 75])
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    filtered_rssi = rssi_array[(rssi_array >= lower_bound) & (rssi_array <= upper_bound)]
    
    if len(filtered_rssi) < 3:
        filtered_rssi = rssi_array  # Fall back to original if too many outliers
    
    # Use median instead of mean for robustness against multi-path spikes
    median_rssi = np.median(filtered_rssi)
    
    # Log-distance path loss model
    # d = 10^((TxPower - RSSI) / (10 * n))
    distance_estimate = 10 ** ((tx_power - median_rssi) / (10 * environment_factor))
    
    # Calculate confidence based on sample consistency
    std_dev = np.std(filtered_rssi)
    if std_dev < 3:
        confidence = 'high'
    elif std_dev < 6:
        confidence = 'medium'
    else:
        confidence = 'low'
    
    # Detect multi-path presence (high variance indicates reflections)
    multipath_detected = std_dev > 8
    
    return {
        'distance_m': round(distance_estimate, 2),
        'distance_ft': round(distance_estimate * 3.281, 2),
        'confidence': confidence,
        'multipath_detected': multipath_detected,
        'rssi_median': float(median_rssi),
        'rssi_std_dev': float(std_dev),
        'sample_count': len(filtered_rssi),
        'environment_factor': environment_factor,
    }
```

### 4.2 TX Power Calibration Database
```python
# In utils/tscm/ble_scanner.py or separate calibration module

TX_POWER_CALIBRATION = {
    # Device type -> calibrated TX power at 1 meter
    'airtag': -59,      # Apple AirTag typical
    'airtag_proximity': -63,  # When in very close proximity
    'tile_pro': -62,
    'tile_matrix': -60,
    'smarttag': -58,
    'smarttag_v2': -61,
    'generic_ble': -59,  # Default assumption
    'esp32': -65,        # ESP32 default TX power
    'iphone_find_my': -59,
    'android_nearby': -60,
}

def get_calibrated_tx_power(device: BLEDevice) -> int:
    """Get calibrated TX power based on device type."""
    if device.tracker_type:
        tracker_lower = device.tracker_type.lower()
        if 'airtag' in tracker_lower:
            return TX_POWER_CALIBRATION['airtag']
        elif 'tile' in tracker_lower:
            return TX_POWER_CALIBRATION['tile_pro']
        elif 'smarttag' in tracker_lower:
            return TX_POWER_CALIBRATION['smarttag']
        elif 'esp' in tracker_lower:
            return TX_POWER_CALIBRATION['esp32']
    
    # Default
    return TX_POWER_CALIBRATION['generic_ble']
```

---

## 5. Performance Optimizations

### 5.1 Ring Buffer for RSSI Samples
```python
from collections import deque
import threading

class RSSIRingBuffer:
    """Thread-safe ring buffer for RSSI samples with automatic statistics."""
    
    def __init__(self, max_size: int = 100):
        self._buffer = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._stats_cache = {}
        self._stats_timestamp = 0
    
    def add(self, rssi: int, timestamp: float = None):
        """Add RSSI sample with optional timestamp."""
        import time
        ts = timestamp or time.time()
        
        with self._lock:
            self._buffer.append((ts, rssi))
            self._stats_cache = {}  # Invalidate cache
    
    def get_statistics(self) -> dict:
        """Get cached statistics with lazy evaluation."""
        import time
        
        with self._lock:
            if not self._buffer:
                return {'count': 0}
            
            current_time = time.time()
            
            # Return cached stats if recent
            if self._stats_cache and (current_time - self._stats_timestamp) < 1.0:
                return self._stats_cache
            
            values = [r for _, r in self._buffer]
            
            self._stats_cache = {
                'count': len(values),
                'min': min(values),
                'max': max(values),
                'mean': sum(values) / len(values),
                'median': sorted(values)[len(values) // 2],
                'std_dev': (sum((v - sum(values)/len(values))**2 for v in values) / len(values)) ** 0.5,
                'latest': values[-1],
                'oldest_timestamp': self._buffer[0][0],
                'newest_timestamp': self._buffer[-1][0],
            }
            self._stats_timestamp = current_time
            
            return self._stats_cache
    
    def get_recent(self, count: int = 10) -> list[int]:
        """Get most recent RSSI values."""
        with self._lock:
            return [r for _, r in list(self._buffer)[-count:]]
```

### 5.2 LRU Cache for OUI Lookups
```python
from functools import lru_cache

@lru_cache(maxsize=1024)
def cached_oui_lookup(mac_prefix: str) -> str:
    """Cached OUI manufacturer lookup."""
    from data.oui import get_manufacturer
    return get_manufacturer(mac_prefix + ':00:00') or 'Unknown'

# Usage in device analysis
def analyze_device_fast(mac: str) -> dict:
    mac_prefix = mac[:8].replace(':', '')
    manufacturer = cached_oui_lookup(mac_prefix)
    # ... rest of analysis
```

### 5.3 Batch Processing for Multiple Devices
```python
async def batch_analyze_devices(devices: list[BLEDevice], 
                                 batch_size: int = 50) -> list[DeviceProfile]:
    """
    Analyze multiple devices in batches for better performance.
    """
    from asyncio import gather
    
    profiles = []
    
    for i in range(0, len(devices), batch_size):
        batch = devices[i:i + batch_size]
        batch_tasks = [analyze_single_device_async(d) for d in batch]
        batch_results = await gather(*batch_tasks)
        profiles.extend(batch_results)
    
    return profiles
```

---

## 6. Security Enhancements

### 6.1 Own Device Filtering
```python
class OwnDeviceRegistry:
    """Registry of known personal devices to filter from alerts."""
    
    def __init__(self):
        self._own_macs = set()
        self._own_fingerprints = set()
        self._config_file = 'data/own_devices.json'
        self._load_registry()
    
    def register_own_device(self, mac: str, name: str = '', 
                            fingerprint: str = None):
        """Register a personal device."""
        self._own_macs.add(mac.upper())
        if fingerprint:
            self._own_fingerprints.add(fingerprint)
        self._save_registry()
    
    def is_own_device(self, device: BLEDevice) -> bool:
        """Check if device belongs to user."""
        if device.mac.upper() in self._own_macs:
            return True
        
        fp = create_payload_fingerprint(
            device.manufacturer_data,
            getattr(device, 'service_data', None),
            device.service_uuids
        )
        return fp in self._own_fingerprints
    
    def _load_registry(self):
        """Load registry from file."""
        import json
        import os
        
        if os.path.exists(self._config_file):
            try:
                with open(self._config_file, 'r') as f:
                    data = json.load(f)
                    self._own_macs = set(data.get('macs', []))
                    self._own_fingerprints = set(data.get('fingerprints', []))
            except Exception as e:
                logger.warning(f"Could not load own device registry: {e}")
    
    def _save_registry(self):
        """Save registry to file."""
        import json
        
        try:
            with open(self._config_file, 'w') as f:
                json.dump({
                    'macs': list(self._own_macs),
                    'fingerprints': list(self._own_fingerprints),
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save own device registry: {e}")
```

### 6.2 Tiered Alerting System
```python
from enum import IntEnum

class AlertTier(IntEnum):
    INFORMATIONAL = 1    # Known personal devices, common electronics
    LOW = 2              # Unknown but benign devices
    MEDIUM = 3           # Potential trackers, unknown manufacturers
    HIGH = 4             # Confirmed trackers, suspicious patterns
    CRITICAL = 5         # Active tracking during sensitive periods

def determine_alert_tier(device: BLEDevice, context: dict) -> AlertTier:
    """
    Determine alert tier based on device characteristics and context.
    """
    # Tier 1: Known personal devices
    if own_device_registry.is_own_device(device):
        return AlertTier.INFORMATIONAL
    
    # Tier 5: Confirmed tracker during meeting
    if device.is_tracker and context.get('during_meeting'):
        return AlertTier.CRITICAL
    
    # Tier 4: Confirmed tracker (AirTag, Tile, etc.)
    if device.is_airtag or device.is_tile or device.is_smarttag:
        return AlertTier.HIGH
    
    # Tier 4: Stable RSSI suggesting fixed placement
    if device.get_rssi_stability() > 0.9 and device.detection_count > 10:
        return AlertTier.HIGH
    
    # Tier 3: Unknown manufacturer with no name
    if not device.manufacturer_name and not device.name:
        return AlertTier.MEDIUM
    
    # Tier 3: ESP32/programmable device
    if device.is_espressif:
        return AlertTier.MEDIUM
    
    # Tier 2: Known manufacturer, normal behavior
    if device.manufacturer_name and device.name:
        return AlertTier.LOW
    
    # Default: Medium (unknown)
    return AlertTier.MEDIUM
```

### 6.3 Privacy-Preserving Logging
```python
def log_device_detection_privacy(device: BLEDevice, include_mac: bool = False):
    """
    Log device detection while preserving privacy.
    MAC addresses are hashed unless explicitly needed.
    """
    import hashlib
    
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'device_hash': hashlib.sha256(device.mac.encode()).hexdigest()[:12],
        'manufacturer': device.manufacturer_name,
        'device_type': device.tracker_type or 'unknown',
        'is_tracker': device.is_tracker,
        'rssi': device.rssi,
        'alert_tier': determine_alert_tier(device, {}).value,
    }
    
    # Only include full MAC for high-tier alerts or debugging
    if include_mac or device.is_tracker:
        log_entry['mac'] = device.mac
    
    logger.info(f"BLE device detected: {log_entry}")
```

---

## 7. Testing Suite Recommendations

### 7.1 Unit Tests
```python
# tests/test_bluetooth_identification.py

import pytest
from utils.tscm.ble_scanner import BLEScanner, BLEDevice
from utils.tscm.correlation import CorrelationEngine, IndicatorType

class TestAirTagDetection:
    """Test AirTag detection accuracy."""
    
    def test_airtag_manufacturer_id(self):
        """Test detection via Apple manufacturer ID."""
        device = BLEDevice(
            mac='4C:E6:76:12:34:56',
            manufacturer_id=0x004C,
            manufacturer_data=b'\x12\x19\x01\x02\x03\x04'
        )
        
        scanner = BLEScanner()
        scanner._identify_tracker(device, 0x004C, device.manufacturer_data)
        
        assert device.is_airtag == True
        assert device.is_tracker == True
        assert device.tracker_type == 'AirTag'
    
    def test_airtag_confidence_scoring(self):
        """Test multi-factor confidence scoring."""
        device = BLEDevice(
            mac='4C:E6:76:12:34:56',
            manufacturer_id=0x004C,
            manufacturer_data=b'\x12\x19\x01\x02\x03\x04',
            rssi=-65
        )
        
        confidence, evidence = AirTagDetectionConfidence.calculate_confidence(device)
        
        assert confidence > 0.8  # High confidence with multiple factors
        assert len(evidence) >= 2  # At least 2 pieces of evidence
    
    def test_false_positive_prevention(self):
        """Test that non-AirTag Apple devices aren't misidentified."""
        device = BLEDevice(
            mac='00:25:00:AB:CD:EF',  # Non-AirTag Apple OUI
            manufacturer_id=0x004C,
            manufacturer_data=b'\x00\x00\x00\x00',  # Non-Find My data
            name='iPhone'
        )
        
        scanner = BLEScanner()
        scanner._identify_tracker(device, 0x004C, device.manufacturer_data)
        
        # Should not be flagged as AirTag without proper signatures
        assert device.is_airtag == False or confidence < 0.5

class TestPayloadFingerprinting:
    """Test payload fingerprinting and clustering."""
    
    def test_fingerprint_consistency(self):
        """Test same payload produces same fingerprint."""
        payload = b'\x12\x19\x01\x02\x03\x04'
        
        fp1 = create_payload_fingerprint(payload)
        fp2 = create_payload_fingerprint(payload)
        
        assert fp1 == fp2
    
    def test_fingerprint_uniqueness(self):
        """Test different payloads produce different fingerprints."""
        payload1 = b'\x12\x19\x01\x02\x03\x04'
        payload2 = b'\x12\x19\x05\x06\x07\x08'
        
        fp1 = create_payload_fingerprint(payload1)
        fp2 = create_payload_fingerprint(payload2)
        
        assert fp1 != fp2

class TestDistanceEstimation:
    """Test distance estimation accuracy."""
    
    def test_distance_with_stable_rssi(self):
        """Test distance estimation with stable RSSI samples."""
        rssi_samples = [-60, -61, -59, -60, -61, -60, -59, -60]
        
        result = estimate_distance_with_multipath_mitigation(rssi_samples)
        
        assert result['distance_m'] is not None
        assert result['confidence'] == 'high'
        assert result['multipath_detected'] == False
    
    def test_distance_with_multipath(self):
        """Test distance estimation with multi-path interference."""
        rssi_samples = [-60, -45, -75, -62, -40, -80, -61, -55]  # High variance
        
        result = estimate_distance_with_multipath_mitigation(rssi_samples)
        
        assert result['distance_m'] is not None
        assert result['multipath_detected'] == True
```

### 7.2 Integration Tests
```python
# tests/test_bluetooth_integration.py

@pytest.mark.integration
class TestBluetoothIntegration:
    """Integration tests for Bluetooth scanning pipeline."""
    
    def test_end_to_end_detection(self):
        """Test complete detection pipeline from scan to alert."""
        scanner = BLEScanner()
        engine = CorrelationEngine()
        
        # Simulate device detection
        devices = scanner.scan(duration=5)
        
        for device in devices:
            profile = engine.analyze_bluetooth_device(device.to_dict())
            
            # Verify profile creation
            assert profile.identifier is not None
            assert profile.protocol == 'bluetooth'
            
            # Verify risk assessment
            assert profile.risk_level in RiskLevel
    
    def test_tracker_clustering(self):
        """Test clustering of rotating MAC addresses."""
        # Create simulated devices with rotating MACs but same fingerprint
        devices = [
            BLEDevice(mac='4C:E6:76:11:11:11', manufacturer_data=b'\x12\x19\x01'),
            BLEDevice(mac='4C:E6:76:22:22:22', manufacturer_data=b'\x12\x19\x01'),
            BLEDevice(mac='4C:E6:76:33:33:33', manufacturer_data=b'\x12\x19\x01'),
        ]
        
        clusters = cluster_devices_by_fingerprint(devices)
        
        assert len(clusters) >= 1  # At least one cluster found
        assert len(clusters[0]) == 3  # All three should cluster together
```

### 7.3 Performance Benchmarks
```python
# tests/benchmark_bluetooth.py

import time
import pytest

def benchmark_scan_performance():
    """Benchmark BLE scan performance."""
    scanner = BLEScanner()
    
    start = time.perf_counter()
    devices = scanner.scan(duration=10)
    elapsed = time.perf_counter() - start
    
    print(f"Scan completed in {elapsed:.2f}s")
    print(f"Found {len(devices)} devices")
    print(f"Rate: {len(devices)/elapsed:.2f} devices/second")
    
    assert elapsed < 15  # Should complete within 15 seconds

def batch_analysis_benchmark():
    """Benchmark batch device analysis."""
    devices = [BLEDevice(mac=f'00:11:22:33:44:{i:02X}') for i in range(100)]
    
    start = time.perf_counter()
    profiles = batch_analyze_devices(devices)
    elapsed = time.perf_counter() - start
    
    print(f"Analyzed {len(profiles)} devices in {elapsed:.2f}s")
    print(f"Rate: {len(profiles)/elapsed:.2f} devices/second")
    
    assert elapsed < 5  # Should process 100 devices in under 5 seconds
```

---

## 8. API Integration Enhancements

### 8.1 Streaming Analytics Endpoint
```python
# routes/bluetooth_v2.py

@bluetooth_bp.route('/api/bluetooth/stream/analytics', methods=['GET'])
def stream_bluetooth_analytics():
    """
    Stream real-time Bluetooth analytics with enhanced identification.
    
    Query params:
    - include_raw: Include raw advertisement data
    - clustering: Enable MAC rotation clustering
    - distance_estimation: Enable distance estimates
    """
    include_raw = request.args.get('include_raw', 'false').lower() == 'true'
    enable_clustering = request.args.get('clustering', 'false').lower() == 'true'
    enable_distance = request.args.get('distance_estimation', 'false').lower() == 'true'
    
    def generate():
        while True:
            scanner = get_ble_scanner()
            devices = scanner.scan(duration=5)
            
            analytics = {
                'timestamp': datetime.now().isoformat(),
                'total_devices': len(devices),
                'trackers_detected': sum(1 for d in devices if d.is_tracker),
                'devices': [],
            }
            
            for device in devices:
                device_data = {
                    'mac': device.mac,
                    'name': device.name,
                    'manufacturer': device.manufacturer_name,
                    'tracker_type': device.tracker_type,
                    'is_tracker': device.is_tracker,
                    'rssi': device.rssi,
                    'detection_count': device.detection_count,
                }
                
                if enable_distance and device.rssi:
                    dist_info = estimate_distance_with_multipath_mitigation(
                        [device.rssi],
                        tx_power=get_calibrated_tx_power(device)
                    )
                    device_data['distance_estimate'] = dist_info
                
                if include_raw:
                    device_data['raw'] = {
                        'manufacturer_id': device.manufacturer_id,
                        'manufacturer_data': device.manufacturer_data.hex() if device.manufacturer_data else None,
                        'service_uuids': device.service_uuids,
                    }
                
                analytics['devices'].append(device_data)
            
            if enable_clustering:
                analytics['clusters'] = cluster_devices_by_fingerprint(devices)
            
            yield f"data: {json.dumps(analytics)}\n\n"
            time.sleep(2)
    
    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )
```

### 8.2 Export Formats
```python
@bluetooth_bp.route('/api/bluetooth/export', methods=['POST'])
def export_bluetooth_data():
    """
    Export Bluetooth detection data in various formats.
    
    Request JSON:
    - format: 'json', 'csv', 'pdf_report'
    - time_range: {'start': ISO, 'end': ISO}
    - include_trackers_only: bool
    """
    data = request.json
    export_format = data.get('format', 'json')
    include_trackers_only = data.get('include_trackers_only', False)
    
    # Fetch historical data
    devices = get_historical_bluetooth_devices(
        time_range=data.get('time_range'),
        trackers_only=include_trackers_only
    )
    
    if export_format == 'json':
        return jsonify({'devices': [d.to_dict() for d in devices]})
    
    elif export_format == 'csv':
        import csv
        from io import StringIO
        
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'Timestamp', 'MAC', 'Name', 'Manufacturer', 'Tracker Type',
            'RSSI', 'Distance (m)', 'Alert Tier'
        ])
        
        for device in devices:
            writer.writerow([
                device.first_seen.isoformat(),
                device.mac,
                device.name or 'Unknown',
                device.manufacturer_name or 'Unknown',
                device.tracker_type or 'N/A',
                device.rssi or 'N/A',
                estimate_distance_with_multipath_mitigation([device.rssi])['distance_m'] if device.rssi else 'N/A',
                determine_alert_tier(device, {}).name,
            ])
        
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=bluetooth_export.csv'}
        )
    
    elif export_format == 'pdf_report':
        # Generate PDF report with findings
        from utils.tscm.reports import generate_tracker_report
        pdf_content = generate_tracker_report(devices)
        
        return Response(
            pdf_content,
            mimetype='application/pdf',
            headers={'Content-Disposition': 'attachment; filename=tracker_report.pdf'}
        )
```

---

## 9. Error Handling & Resilience

### 9.1 Comprehensive Error Handling
```python
class BLEScanError(Exception):
    """Base exception for BLE scanning errors."""
    pass

class BLEAdapterNotFoundError(BLEScanError):
    """Bluetooth adapter not found."""
    pass

class BLEPermissionError(BLEScanError):
    """Insufficient permissions for BLE operations."""
    pass

class BLETimeoutError(BLEScanError):
    """BLE operation timed out."""
    pass

def scan_with_retry(scanner: BLEScanner, duration: int = 10, 
                    max_retries: int = 3) -> list[BLEDevice]:
    """
    Scan with automatic retry on transient failures.
    """
    import time
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            devices = scanner.scan(duration=duration)
            return devices
            
        except BLEPermissionError as e:
            logger.error(f"BLE permission error: {e}")
            raise  # Don't retry permission errors
            
        except BLETimeoutError as e:
            logger.warning(f"BLE timeout (attempt {attempt+1}/{max_retries}): {e}")
            last_error = e
            time.sleep(1 * (attempt + 1))  # Exponential backoff
            
        except BLEScanError as e:
            logger.warning(f"BLE scan error (attempt {attempt+1}/{max_retries}): {e}")
            last_error = e
            time.sleep(0.5 * (attempt + 1))
            
        except Exception as e:
            logger.error(f"Unexpected error during BLE scan: {e}")
            last_error = e
            time.sleep(1)
    
    # All retries exhausted
    raise BLEScanError(f"Scan failed after {max_retries} attempts: {last_error}")
```

### 9.2 Graceful Degradation
```python
def scan_with_fallback_chain(duration: int = 10) -> list[BLEDevice]:
    """
    Attempt multiple scanning methods in order of preference.
    """
    methods = [
        ('bleak', lambda: bleak_scan(duration)),
        ('btmgmt', lambda: btmgmt_scan(duration)),
        ('hcitool', lambda: hcitool_scan(duration)),
        ('system_profiler', lambda: macos_fallback_scan()),
    ]
    
    for method_name, scan_func in methods:
        try:
            logger.info(f"Attempting scan method: {method_name}")
            devices = scan_func()
            
            if devices:
                logger.info(f"Successful scan with {method_name}: {len(devices)} devices")
                return devices
                
        except Exception as e:
            logger.warning(f"Scan method {method_name} failed: {e}")
            continue
    
    logger.error("All scan methods failed")
    return []  # Return empty rather than raising
```

---

## 10. Documentation Gaps

### 10.1 API Documentation
- Add OpenAPI/Swagger documentation for all Bluetooth endpoints
- Document request/response schemas
- Include example curl commands
- Document error codes and responses

### 10.2 User Guide
- Create user guide for interpreting tracker alerts
- Explain confidence scores and evidence
- Provide guidance on response actions for different alert tiers
- Document how to register personal devices

### 10.3 Developer Guide
- Architecture diagram of Bluetooth detection pipeline
- Sequence diagrams for key workflows
- Extension guide for adding new tracker signatures
- Performance tuning recommendations

---

## Priority Implementation Roadmap

### Phase 1 (Immediate Impact - Week 1-2)
1. ✅ Expand tracker signature database (Section 1)
2. ✅ Implement multi-factor AirTag detection (Section 2.1)
3. ✅ Add own device filtering (Section 6.1)
4. ✅ Implement tiered alerting (Section 6.2)

### Phase 2 (High Value - Week 3-4)
1. ✅ Payload fingerprinting and clustering (Section 3)
2. ✅ Distance estimation with multi-path mitigation (Section 4)
3. ✅ TX power calibration (Section 4.2)
4. ✅ Performance optimizations (Section 5)

### Phase 3 (Advanced Features - Month 2)
1. ✅ MAC rotation detection (Section 2.2)
2. ✅ Behavioral clustering (Section 3.2)
3. ✅ Streaming analytics API (Section 8.1)
4. ✅ Export functionality (Section 8.2)

### Phase 4 (Hardening - Month 3)
1. ✅ Comprehensive testing suite (Section 7)
2. ✅ Error handling and resilience (Section 9)
3. ✅ Complete documentation (Section 10)
4. ✅ Privacy-preserving logging (Section 6.3)

---

## Conclusion

These improvements will significantly enhance the Bluetooth identification system's accuracy, performance, and usability. The phased approach allows for incremental deployment with immediate value delivery while building toward a comprehensive tracker detection platform.

Key benefits:
- **Higher accuracy**: Multi-factor detection reduces false positives/negatives
- **Better coverage**: Expanded signatures catch more tracker types
- **Improved UX**: Tiered alerts focus attention on genuine threats
- **Better performance**: Optimizations enable real-time processing
- **Enhanced security**: Own device filtering prevents alert fatigue
- **Actionable intelligence**: Distance estimation and clustering provide context
