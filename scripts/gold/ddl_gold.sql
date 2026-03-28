/*
DDL Script: Create Gold Views
*/

-- =============================================
-- Create Dimension: gold.dim_patients
-- =============================================
IF OBJECT_ID('gold.dim_patients', 'V') IS NOT NULL
    DROP VIEW gold.dim_patients;
GO

CREATE VIEW gold.dim_patients AS
SELECT
    ROW_NUMBER() OVER (ORDER BY gender, age) AS patient_key, -- Surrogate key
    gender                                   AS gender,
    age                                      AS age,
    bmi                                      AS bmi,
    smoking_history                          AS smoking_status,
    disease_category                         AS disease_type
FROM (
    -- diabetes
    SELECT 
        gender,
        age,
        bmi,
        smoking_history,
        disease_category
    FROM silver.diabetes

    UNION ALL

    -- heart
    SELECT 
        gender,
        age,
        bmi,
        smoking,
        disease_category
    FROM silver.heart

    UNION ALL

    -- hypertension
    SELECT 
        gender,
        age,
        bmi,
        smoking_status,
        disease_category
    FROM silver.hypertension
) AS p;
GO


-- =============================================
-- Create Dimension: gold.dim_conditions
-- =============================================
IF OBJECT_ID('gold.dim_conditions', 'V') IS NOT NULL
    DROP VIEW gold.dim_conditions;
GO

CREATE VIEW gold.dim_conditions AS
SELECT
    ROW_NUMBER() OVER (ORDER BY disease_category) AS condition_key, -- Surrogate key
    disease_category                              AS disease_name
FROM (
    SELECT disease_category FROM silver.diabetes
    UNION
    SELECT disease_category FROM silver.heart
    UNION
    SELECT disease_category FROM silver.hypertension
) AS d;
GO


-- =============================================
-- Create Fact Table: gold.fact_health
-- =============================================
IF OBJECT_ID('gold.fact_health', 'V') IS NOT NULL
    DROP VIEW gold.fact_health;
GO

CREATE VIEW gold.fact_health AS
SELECT
    fh.record_id        AS record_number,
    pt.patient_key      AS patient_key,
    dc.condition_key    AS condition_key,
    fh.age              AS record_age,
    fh.bmi              AS bmi,
    fh.glucose_level    AS glucose_level,
    fh.cholesterol      AS cholesterol,
    fh.blood_pressure   AS blood_pressure

FROM (
    -- unified dataset

    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS record_id,
        gender,
        age,
        bmi,
        blood_glucose_level        AS glucose_level,
        NULL                       AS cholesterol,
        NULL                       AS blood_pressure,
        disease_category
    FROM silver.diabetes

    UNION ALL

    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
        gender,
        age,
        bmi,
        fasting_blood_sugar,
        cholesterol_level,
        NULL,
        disease_category
    FROM silver.heart

    UNION ALL

    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
        gender,
        age,
        bmi,
        glucose,
        cholesterol,
        systolic_bp,
        disease_category
    FROM silver.hypertension

) AS fh

LEFT JOIN gold.dim_patients pt
    ON fh.gender = pt.gender AND fh.age = pt.age

LEFT JOIN gold.dim_conditions dc
    ON fh.disease_category = dc.disease_name;
GO

SELECT
    fh.*,
    dc.disease_name
FROM gold.fact_health fh
LEFT JOIN gold.dim_conditions dc
    ON fh.condition_key = dc.condition_key;
