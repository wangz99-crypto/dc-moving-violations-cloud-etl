USE mis664_project;
USE mis664_project;


-- Qa: Agencies by month
SELECT
    month,
    issuing_agency_name,
    COUNT(*) AS tickets_count
FROM violations
GROUP BY month, issuing_agency_name
ORDER BY month, issuing_agency_name;

-- Qb: Total number of tickets since 2024-10-01
SELECT
    COUNT(*) AS total_tickets_since_2024_10_01
FROM violations
WHERE violation_date >= '2024-10-01';

-- Qc: Average number of tickets by day of week
SELECT
    weekday,
    AVG(daily_tickets) AS avg_tickets_per_day
FROM (
    SELECT
        violation_date,
        DAYNAME(violation_date) AS weekday,
        COUNT(*) AS daily_tickets
    FROM violations
    GROUP BY violation_date
) AS d
GROUP BY weekday
ORDER BY FIELD(weekday,
               'Monday','Tuesday','Wednesday',
               'Thursday','Friday','Saturday','Sunday');

-- Qd: Tickets on rainy days
SELECT
    COUNT(*) AS tickets_on_rainy_days
FROM violations v
JOIN weather_daily w
      ON v.violation_date = w.weather_date
WHERE w.is_rain = 1;

-- Qe: Total precipitation by month
SELECT
    DATE_FORMAT(weather_date, '%Y-%m') AS year_month_,
    ROUND(SUM(precip),2) AS total_precipitation
FROM weather_daily
GROUP BY year_month_
ORDER BY year_month_;


-- Qf: Total fine per month for vehicles >10 mph over speed limit
SELECT
    month,
    SUM(fine_amount) AS total_fine_over_10mph
FROM violations
WHERE violation_desc LIKE '%MPH OVER%'
  AND violation_desc NOT LIKE '%UP TO TEN MPH OVER%'
  AND violation_desc NOT LIKE '%1-10 MPH OVER%'
GROUP BY month
ORDER BY month;



-- Qg: Average number of tickets per hour of the day
SELECT
    hour_of_day,
    AVG(tickets_per_hour) AS avg_tickets_per_hour
FROM (
    SELECT
        DATE(issue_date) AS violation_day,
        HOUR(issue_date) AS hour_of_day,
        COUNT(*) AS tickets_per_hour
    FROM violations
    GROUP BY DATE(issue_date), HOUR(issue_date)
) AS h
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Qh: Compare accident-related tickets on rainy vs non-rainy days
SELECT
    w.is_rain,
    SUM(CASE WHEN v.accident_indicator = 'Y' THEN 1 ELSE 0 END) AS tickets_with_accident,
    SUM(CASE WHEN v.accident_indicator IS NULL OR v.accident_indicator <> 'Y'
             THEN 1 ELSE 0 END) AS tickets_without_accident
FROM violations v
JOIN weather_daily w
      ON v.violation_date = w.weather_date
GROUP BY w.is_rain;