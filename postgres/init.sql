-- Schema PostgreSQL pour Serre Agricole Intelligente (Serving Layer)
-- Base de donnees: greenhouse_db

CREATE TABLE IF NOT EXISTS latest_measurements (
    greenhouse_id VARCHAR(50) NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    soil_moisture DOUBLE PRECISION,
    light_lux DOUBLE PRECISION,
    co2 DOUBLE PRECISION,
    status VARCHAR(20),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (greenhouse_id, sensor_id)
);

CREATE TABLE IF NOT EXISTS greenhouse_hourly_metrics (
    greenhouse_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP WITH TIME ZONE NOT NULL,
    window_end TIMESTAMP WITH TIME ZONE NOT NULL,
    avg_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_soil_moisture DOUBLE PRECISION,
    min_soil_moisture DOUBLE PRECISION,
    max_soil_moisture DOUBLE PRECISION,
    avg_light_lux DOUBLE PRECISION,
    avg_co2 DOUBLE PRECISION,
    measurement_count BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (greenhouse_id, window_start)
);

CREATE TABLE IF NOT EXISTS greenhouse_daily_metrics (
    greenhouse_id VARCHAR(50) NOT NULL,
    metric_date DATE NOT NULL,
    avg_temperature DOUBLE PRECISION,
    min_temperature DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_soil_moisture DOUBLE PRECISION,
    total_alerts INTEGER DEFAULT 0,
    irrigation_cycles INTEGER DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (greenhouse_id, metric_date)
);

CREATE TABLE IF NOT EXISTS greenhouse_alerts (
    alert_id VARCHAR(100) PRIMARY KEY,
    greenhouse_id VARCHAR(50) NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    parameter VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    severity VARCHAR(20) NOT NULL,
    description TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    is_resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS weather_observations (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    city VARCHAR(100) NOT NULL,
    outdoor_temperature DOUBLE PRECISION,
    outdoor_humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    rainfall_1h DOUBLE PRECISION DEFAULT 0.0,
    weather_description VARCHAR(255),
    is_synthetic BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS irrigation_predictions (
    id SERIAL PRIMARY KEY,
    greenhouse_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    current_soil_moisture DOUBLE PRECISION,
    predicted_moisture_60min DOUBLE PRECISION,
    irrigation_recommended BOOLEAN,
    recommendation_message TEXT,
    model_version VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_evaluation_metrics (
    id SERIAL PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    version VARCHAR(50) NOT NULL,
    training_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    rmse DOUBLE PRECISION,
    mae DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    samples_count INTEGER,
    features_used TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index pour requetes rapides
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON greenhouse_alerts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_gh ON greenhouse_alerts(greenhouse_id);
CREATE INDEX IF NOT EXISTS idx_predictions_gh ON irrigation_predictions(greenhouse_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather_observations(timestamp DESC);
