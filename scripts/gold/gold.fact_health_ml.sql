/*
DDL Script: Create Gold Views
===============================================================================
-- View 2: gold.fact_health_ml
===============================================================================
*/


IF OBJECT_ID('gold.fact_health_ml', 'V') IS NOT NULL
    DROP VIEW gold.fact_health_ml;
GO
 
CREATE VIEW gold.fact_health_ml AS
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
        gender                              AS gender,
        age                                 AS age,
        bmi                                 AS bmi,
        blood_glucose_level                 AS glucose_level,
        hba1c_level                         AS hba1c_level,
        -1                                  AS cholesterol,
        -1                                  AS blood_pressure,
        -1                                  AS triglycerides,
        'Unknown'                           AS hdl_status,
        'Unknown'                           AS ldl_status,
        -1                                  AS sleep_hours,
        'Unknown'                           AS stress_level,
        smoking_history                     AS smoking,
        'Unknown'                           AS family_history,
        CAST(diabetes AS VARCHAR(5))        AS diabetes_flag,
        'Unknown'                           AS alcohol,
        disease_category                    AS disease_category
    FROM silver.diabetes
 
    UNION ALL
 
    -- ========================
    -- heart
    -- ========================
    SELECT
        gender                                  AS gender,
        age                                     AS age,
        bmi                                     AS bmi,
        ISNULL(fasting_blood_sugar, -1)         AS glucose_level,
        -1                                      AS hba1c_level,
        ISNULL(cholesterol_level, -1)           AS cholesterol,
        ISNULL(blood_pressure, -1)              AS blood_pressure,
        ISNULL(triglyceride_level, -1)          AS triglycerides,
        ISNULL(hdl_status, 'Unknown')           AS hdl_status,
        ISNULL(ldl_status, 'Unknown')           AS ldl_status,
        ISNULL(sleep_hours, -1)                 AS sleep_hours,
        ISNULL(stress_level, 'Unknown')         AS stress_level,
        ISNULL(smoking, 'Unknown')              AS smoking,
        ISNULL(family_heart_disease, 'Unknown') AS family_history,
        ISNULL(diabetes, 'Unknown')             AS diabetes_flag,
        ISNULL(alcohol_consumption, 'Unknown')  AS alcohol,
        disease_category                        AS disease_category
    FROM silver.heart
 
    UNION ALL
 
    -- ========================
    -- hypertension
    -- ========================
    SELECT
        gender                              AS gender,
        age                                 AS age,
        bmi                                 AS bmi,
        glucose                             AS glucose_level,
        -1                                  AS hba1c_level,
        cholesterol                         AS cholesterol,
        systolic_bp                         AS blood_pressure,
        triglycerides                       AS triglycerides,
        hdl_status                          AS hdl_status,
        ldl_status                          AS ldl_status,
        sleep_duration                      AS sleep_hours,
        stress_level                        AS stress_level,
        smoking_status                      AS smoking,
        family_history                      AS family_history,
        diabetes                            AS diabetes_flag,
        CAST(alcohol_intake AS VARCHAR)     AS alcohol,
        disease_category                    AS disease_category
    FROM silver.hypertension
 
) AS ml_data;
GO




-- =============================================
-- Quality Check
-- =============================================

-- Row counts
SELECT COUNT(*) AS total_rows FROM gold.fact_health_ml;    --281,128

-- Preview 5 rows from each disease 
SELECT TOP 5 * FROM gold.fact_health_ml WHERE disease_category = 'Diabetes'
UNION ALL
SELECT TOP 5 * FROM gold.fact_health_ml WHERE disease_category = 'Heart Disease'
UNION ALL
SELECT TOP 5 * FROM gold.fact_health_ml WHERE disease_category = 'Hypertension';
