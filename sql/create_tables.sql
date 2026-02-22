-- 创建数据库（只创建一次也可以反复跑）
CREATE DATABASE IF NOT EXISTS mis664_project;
USE mis664_project;

-- ==========================
-- 1. Weather 表（保持不变，只是改成可重复执行）
-- ==========================
DROP TABLE IF EXISTS weather_daily;

CREATE TABLE weather_daily (
    weather_date DATE PRIMARY KEY,
    tempmax DOUBLE,
    tempmin DOUBLE,
    temp DOUBLE,
    precip DOUBLE,
    humidity DOUBLE,
    windspeed DOUBLE,
    conditions VARCHAR(255),
    is_rain TINYINT
);

-- ==========================
-- 2. Violations 表（为 ArcGIS 数据重新设计）
-- ==========================
DROP TABLE IF EXISTS violations;

CREATE TABLE violations (
    violation_id   VARCHAR(50) PRIMARY KEY,  -- 我们从 OBJECTID 派生
    issue_date     DATETIME,                -- 完整日期时间（主要用于保留信息）
    violation_date DATE,                    -- 只有日期部分，用来和 weather_daily.weather_date 对齐
    location       VARCHAR(255),
    violation_code VARCHAR(50),
    violation_desc VARCHAR(255),
    fine_amount    DOUBLE,
    total_paid     DOUBLE,
    latitude       DOUBLE,
    longitude      DOUBLE,
    month          VARCHAR(7)               -- 'YYYY-MM'，方便做按月统计
);


