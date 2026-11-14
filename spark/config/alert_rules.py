"""
Alert Thresholds and Rules configuration for Spark Streaming & Analytics.
"""

ALERT_THRESHOLDS = {
    "temperature": {
        "max": 35.0,
        "min": 10.0,
        "severity_high": "CRITICAL",
        "severity_low": "WARNING"
    },
    "humidity": {
        "max": 90.0,
        "min": 30.0,
        "severity_high": "WARNING",
        "severity_low": "CRITICAL"
    },
    "soil_moisture": {
        "min_warning": 30.0,
        "min_critical": 22.0,
        "severity": "CRITICAL"
    },
    "co2": {
        "max": 1200.0,
        "severity": "WARNING"
    }
}

# Cooldown duration in seconds to prevent flooding alert logs for the same greenhouse/parameter
ALERT_COOLDOWN_SECONDS = 180
