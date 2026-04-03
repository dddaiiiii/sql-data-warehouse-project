/*
DDL Script: Create Silver Tables
*/

-- =============================================
-- 1. DIABETES
-- =============================================
IF OBJECT_ID('silver.diabetes', 'U') IS NOT NULL
    DROP TABLE silver.diabetes;
GO

CREATE TABLE silver.diabetes (
    diabetes_id         INT IDENTITY(1,1) PRIMARY KEY,
    gender              VARCHAR(20),
    age                 FLOAT,
    hypertension        INT,
    heart_disease       INT,
    smoking_history     VARCHAR(30),
    bmi                 FLOAT,
    hba1c_level         FLOAT,
    blood_glucose_level INT,
    diabetes            INT,
    disease_category    VARCHAR(20)
);
GO

-- =============================================
-- 2. HEART
-- =============================================
IF OBJECT_ID('silver.heart', 'U') IS NOT NULL
    DROP TABLE silver.heart;
GO

CREATE TABLE silver.heart (
    heart_id             INT IDENTITY(1,1) PRIMARY KEY,
    gender               VARCHAR(10),
    age                  FLOAT,
    blood_pressure       FLOAT,
    cholesterol_level    FLOAT,
    exercise_habits      VARCHAR(20),
    smoking              VARCHAR(10),
    family_heart_disease VARCHAR(10),
    diabetes             VARCHAR(10),
    bmi                  FLOAT,
    high_blood_pressure  VARCHAR(10),
    hdl_status           VARCHAR(10),   -- low_hdl_cholesterol (Yes/No → Good/Poor)
    ldl_status           VARCHAR(10),   -- high_ldl_cholesterol (Yes/No → Normal/High)
    alcohol_consumption  VARCHAR(20),
    stress_level         VARCHAR(10),   -- (High/Medium/Low)
    sleep_hours          FLOAT,
    sugar_consumption    VARCHAR(20),
    triglyceride_level   FLOAT,
    fasting_blood_sugar  FLOAT,
    crp_level            FLOAT,
    homocysteine_level   FLOAT,
    heart_disease_status VARCHAR(10),
    disease_category     VARCHAR(20)
);
GO

-- =============================================
-- 3. HYPERTENSION
-- =============================================
IF OBJECT_ID('silver.hypertension', 'U') IS NOT NULL
    DROP TABLE silver.hypertension;
GO

CREATE TABLE silver.hypertension (
    hypertension_id         INT IDENTITY(1,1) PRIMARY KEY,
    country                 VARCHAR(50),
    age                     INT,
    bmi                     FLOAT,
    cholesterol             INT,
    systolic_bp             INT,
    diastolic_bp            INT,
    smoking_status          VARCHAR(20),
    alcohol_intake          FLOAT,
    physical_activity_level VARCHAR(20),
    family_history          VARCHAR(10),
    diabetes                VARCHAR(10),
    stress_level            VARCHAR(10), --(1-9 → Low/Medium/High)
    salt_intake             FLOAT,
    sleep_duration          FLOAT,
    heart_rate              INT,
    ldl_status              VARCHAR(10), -- (< 100 → Normal / >= 100 → High)
    hdl_status              VARCHAR(10), -- (>= 60 → Good / < 60 → Poor)
    triglycerides           INT,
    glucose                 INT,
    gender                  VARCHAR(10),
    education_level         VARCHAR(20),
    employment_status       VARCHAR(20),
    hypertension            VARCHAR(10),
    disease_category        VARCHAR(20)
);
GO
