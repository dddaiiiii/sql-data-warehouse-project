/*
===============================================================================
DDL: Silver Layer Tables
Medallion Architecture - Medical Data Warehouse

Age handling:
  - Diabetes scaled ages (0.08-1.88) → inverse-scaled to real years (2-80)
  - All ages normalized to 0-1 (global min=2, max=89)
  - Binned into 4 groups based on normalized range
===============================================================================
*/

-- 1. Diabetes
IF OBJECT_ID('silver.diabetes', 'U') IS NOT NULL DROP TABLE silver.diabetes;
GO
CREATE TABLE silver.diabetes (
    gender              NVARCHAR(10),
    age                 FLOAT,          -- real age in years
    age_normalized      FLOAT,          -- 0-1 (global min=2, max=89)
    age_level           NVARCHAR(10),   -- Young/Middle/Mature/Senior
    bmi                 FLOAT,
    bmi_level           NVARCHAR(20),
    smoking             NVARCHAR(10),
    diabetes            NVARCHAR(3),
    composite_key       NVARCHAR(100),
    hypertension        INT,
    heart_disease       INT,
    HbA1c_level         FLOAT,
    glucose             INT,
    disease_flags       NVARCHAR(10),   -- (DI,HT,HY) e.g. '1,0,1'
    disease_category    NVARCHAR(20)
);
GO

-- 2. Heart
IF OBJECT_ID('silver.heart', 'U') IS NOT NULL DROP TABLE silver.heart;
GO
CREATE TABLE silver.heart (
    gender              NVARCHAR(10),
    age                 FLOAT,
    age_normalized      FLOAT,
    age_level           NVARCHAR(10),
    bmi                 FLOAT,
    bmi_level           NVARCHAR(20),
    smoking             NVARCHAR(10),
    diabetes            NVARCHAR(3),
    composite_key       NVARCHAR(100),
    cholesterol         FLOAT,
    physical_activity   NVARCHAR(10),
    family_history      NVARCHAR(3),
    stress_level        NVARCHAR(10),
    sleep_hours         FLOAT,
    triglycerides       FLOAT,
    glucose             FLOAT,
    low_hdl_cholesterol NVARCHAR(3),
    high_ldl_cholesterol NVARCHAR(3),
    blood_pressure      FLOAT,
    high_blood_pressure NVARCHAR(3),
    sugar_consumption   NVARCHAR(10),
    crp_level           FLOAT,
    homocysteine_level  FLOAT,
    disease_flags       NVARCHAR(10),   -- (DI,HT,HY) e.g. '0,1,0'
    disease_category    NVARCHAR(20)
);
GO

-- 3. Hypertension
IF OBJECT_ID('silver.hypertension', 'U') IS NOT NULL DROP TABLE silver.hypertension;
GO
CREATE TABLE silver.hypertension (
    gender              NVARCHAR(10),
    age                 INT,
    age_normalized      FLOAT,
    age_level           NVARCHAR(10),
    bmi                 FLOAT,
    bmi_level           NVARCHAR(20),
    smoking             NVARCHAR(10),
    diabetes            NVARCHAR(3),
    composite_key       NVARCHAR(100),
    cholesterol         INT,
    physical_activity   NVARCHAR(10),
    family_history      NVARCHAR(3),
    stress_level        NVARCHAR(10),
    sleep_hours         FLOAT,
    triglycerides       INT,
    glucose             INT,
    low_hdl_cholesterol NVARCHAR(3),
    high_ldl_cholesterol NVARCHAR(3),
    systolic_bp         INT,
    diastolic_bp        INT,
    alcohol_intake      FLOAT,
    salt_intake         FLOAT,
    heart_rate          INT,
    hdl                 INT,
    ldl                 INT,
    education_level     NVARCHAR(20),
    employment_status   NVARCHAR(20),
    disease_flags       NVARCHAR(10),   -- (DI,HT,HY) e.g. '0,0,1'. HT always 0 (no heart data)
    disease_category    NVARCHAR(20)
);
GO
