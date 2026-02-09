-- Sustained Alerts: sensors where body_temperature > 40 °C for 3+ consecutive readings
-- Uses gaps-and-islands: the difference between two ROW_NUMBER() sequences is constant
-- for consecutive hot readings, creating a natural group identifier (streak_id).

-- Number every reading per sensor chronologically
WITH ordered AS (
    SELECT
        sensor_id,
        event_timestamp,
        body_temperature,
        ROW_NUMBER() OVER (
            PARTITION BY sensor_id
            ORDER BY event_timestamp
        ) AS overall_reading_number
    FROM
        `patient_monitoring.patient_vitals`
),

-- Keep only hot readings and number them separately
hot_readings AS (
    SELECT
        sensor_id,
        event_timestamp,
        body_temperature,
        overall_reading_number,
        ROW_NUMBER() OVER (
            PARTITION BY sensor_id
            ORDER BY event_timestamp
        ) AS hot_reading_number
    FROM
        ordered
    WHERE
        body_temperature > 40
),

-- Consecutive hot readings share the same (overall_reading_number - hot_reading_number) value
streaks AS (
    SELECT
        sensor_id,
        event_timestamp,
        body_temperature,
        (overall_reading_number - hot_reading_number) AS streak_id
    FROM
        hot_readings
)

-- Aggregate per streak, filter to 3+ consecutive readings
SELECT
    sensor_id,
    MIN(event_timestamp)                    AS streak_start,
    MAX(event_timestamp)                    AS streak_end,
    COUNT(*)                                AS consecutive_readings,
    ROUND(AVG(body_temperature), 2)         AS avg_temperature
FROM
    streaks
GROUP BY
    sensor_id, streak_id
HAVING
    COUNT(*) >= 3
ORDER BY
    sensor_id, streak_start;
