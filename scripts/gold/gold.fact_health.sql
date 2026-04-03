/*
DDL Script: Create Gold Views
===============================================================================
-- View 1: gold.fact_health
===============================================================================
*/

IF OBJECT_ID('gold.fact_health', 'V') IS NOT NULL
    DROP VIEW gold.fact_health;
GO

CREATE VIEW gold.fact_health AS
SELECT
    gender,
    age,
    bmi,
    glucose_level,
    hba1c_level,
    cholesterol,
    blood_pressure,
    triglycerides,
    hdl_status,
    ldl_status,
    sleep_hours,
    stress_level,
    smoking,
    family_history,
    diabetes_flag,
    alcohol,
    disease_category
FROM (

    -- ========================
    -- diabetes
    -- ========================
    SELECT
        gender                          AS gender,
        age                             AS age,
        bmi                             AS bmi,
        blood_glucose_level             AS glucose_level,
        hba1c_level                     AS hba1c_level,
        NULL                            AS cholesterol,
        NULL                            AS blood_pressure,
        NULL                            AS triglycerides,
        NULL                            AS hdl_status,
        NULL                            AS ldl_status,
        NULL                            AS sleep_hours,
        NULL                            AS stress_level,
        smoking_history                 AS smoking,
        NULL                            AS family_history,
        CAST(diabetes AS VARCHAR(5))    AS diabetes_flag,
        NULL                            AS alcohol,
        disease_category                AS disease_category
    FROM silver.diabetes

    UNION ALL

    -- ========================
    -- heart
    -- ========================
    SELECT
        gender                          AS gender,
        age                             AS age,
        bmi                             AS bmi,
        fasting_blood_sugar             AS glucose_level,
        NULL                            AS hba1c_level,
        cholesterol_level               AS cholesterol,
        blood_pressure                  AS blood_pressure,
        triglyceride_level              AS triglycerides,
        hdl_status                      AS hdl_status,
        ldl_status                      AS ldl_status,
        sleep_hours                     AS sleep_hours,
        stress_level                    AS stress_level,
        smoking                         AS smoking,
        family_heart_disease            AS family_history,
        diabetes                        AS diabetes_flag,
        alcohol_consumption             AS alcohol,
        disease_category                AS disease_category
    FROM silver.heart

    UNION ALL

    -- ========================
    -- hypertension
    -- ========================
    SELECT
        gender                          AS gender,
        age                             AS age,
        bmi                             AS bmi,
        glucose                         AS glucose_level,
        NULL                            AS hba1c_level,
        cholesterol                     AS cholesterol,
        systolic_bp                     AS blood_pressure,
        triglycerides                   AS triglycerides,
        hdl_status                      AS hdl_status,
        ldl_status                      AS ldl_status,
        sleep_duration                  AS sleep_hours,
        stress_level                    AS stress_level,
        smoking_status                  AS smoking,
        family_history                  AS family_history,
        diabetes                        AS diabetes_flag,
        CAST(alcohol_intake AS VARCHAR) AS alcohol,
        disease_category                AS disease_category
    FROM silver.hypertension

) AS raw_data;
GO


-- =============================================
-- Quality Check
-- =============================================

-- Row counts
SELECT COUNT(*) AS total_rows FROM gold.fact_health;       -- 281,128

-- Preview 5 rows from each disease 
SELECT TOP 5 * FROM gold.fact_health WHERE disease_category = 'Diabetes'
UNION ALL
SELECT TOP 5 * FROM gold.fact_health WHERE disease_category = 'Heart Disease'
UNION ALL
SELECT TOP 5 * FROM gold.fact_health WHERE disease_category = 'Hypertension';


