/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
Usage Example:
    EXEC Silver.load_silver;
===============================================================================
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT 'Loading Silver Layer';

        -- =============================================
        -- 1. DIABETES
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.diabetes';
        TRUNCATE TABLE silver.diabetes;
        PRINT '>> Inserting Data Into: silver.diabetes';

        INSERT INTO silver.diabetes (
            gender, age, hypertension, heart_disease,
            smoking_history, bmi, hba1c_level,
            blood_glucose_level, diabetes,
            disease_category
        )
        SELECT
            TRIM(gender)                                AS gender,
            age,
            hypertension,
            heart_disease,
            CASE WHEN TRIM(smoking_history) = 'No Info'
                 THEN 'Unknown'
                 ELSE TRIM(smoking_history)
            END                                         AS smoking_history,
            bmi,
            HbA1c_level                                 AS hba1c_level,
            blood_glucose_level,
            diabetes,
            'Diabetes'                                  AS disease_category
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        gender, age, hypertension, heart_disease,
                        smoking_history, bmi, HbA1c_level,
                        blood_glucose_level, diabetes
                    ORDER BY (SELECT NULL)
                ) AS row_num
            FROM bronze.diabetes
        ) AS cleaned
        WHERE row_num = 1;
        -- RESULTS = 96,146 rows

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- =============================================
        -- 2. HEART
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.heart';
        TRUNCATE TABLE silver.heart;
        PRINT '>> Inserting Data Into: silver.heart';

        INSERT INTO silver.heart (
            gender, age, blood_pressure, cholesterol_level,
            exercise_habits, smoking, family_heart_disease,
            diabetes, bmi, high_blood_pressure,
            hdl_status,           -- كانت low_hdl_cholesterol
            ldl_status,           -- كانت high_ldl_cholesterol
            alcohol_consumption, stress_level,
            sleep_hours, sugar_consumption, triglyceride_level,
            fasting_blood_sugar, crp_level, homocysteine_level,
            heart_disease_status, disease_category
        )
        SELECT
            ISNULL(TRIM(Gender), 'Unknown')                     AS gender,
            Age                                                 AS age,
            Blood_Pressure                                      AS blood_pressure,
            Cholesterol_Level                                   AS cholesterol_level,
            ISNULL(TRIM(Exercise_Habits), 'Unknown')            AS exercise_habits,
            ISNULL(TRIM(Smoking), 'Unknown')                    AS smoking,
            ISNULL(TRIM(Family_Heart_Disease), 'Unknown')       AS family_heart_disease,
            ISNULL(TRIM(Diabetes), 'Unknown')                   AS diabetes,
            BMI                                                 AS bmi,
            ISNULL(TRIM(High_Blood_Pressure), 'Unknown')        AS high_blood_pressure,

            -- low_hdl_cholesterol: Yes = Poor, No = Good
            CASE WHEN TRIM(Low_HDL_Cholesterol) = 'Yes' THEN 'Poor'
                 WHEN TRIM(Low_HDL_Cholesterol) = 'No'  THEN 'Good'
                 ELSE 'Unknown'
            END                                                 AS hdl_status,

            -- high_ldl_cholesterol: Yes = High, No = Normal
            CASE WHEN TRIM(High_LDL_Cholesterol) = 'Yes' THEN 'High'
                 WHEN TRIM(High_LDL_Cholesterol) = 'No'  THEN 'Normal'
                 ELSE 'Unknown'
            END                                                 AS ldl_status,

            CASE WHEN Alcohol_Consumption IS NULL
                      OR TRIM(Alcohol_Consumption) = 'None'
                 THEN 'Unknown'
                 ELSE TRIM(Alcohol_Consumption)
            END                                                 AS alcohol_consumption,

            -- stress_level بيفضل كما هو (High/Medium/Low)
            ISNULL(TRIM(Stress_Level), 'Unknown')               AS stress_level,

            Sleep_Hours                                         AS sleep_hours,
            ISNULL(TRIM(Sugar_Consumption), 'Unknown')          AS sugar_consumption,
            Triglyceride_Level                                  AS triglyceride_level,
            Fasting_Blood_Sugar                                 AS fasting_blood_sugar,
            CRP_Level                                           AS crp_level,
            Homocysteine_Level                                  AS homocysteine_level,
            TRIM(Heart_Disease_Status)                          AS heart_disease_status,
            'Heart Disease'                                     AS disease_category
        FROM bronze.heart;
        -- RESULTS = 10,000 rows

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- =============================================
        -- 3. HYPERTENSION
        -- =============================================
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.hypertension';
        TRUNCATE TABLE silver.hypertension;
        PRINT '>> Inserting Data Into: silver.hypertension';

        INSERT INTO silver.hypertension (
            country, age, bmi, cholesterol, systolic_bp, diastolic_bp,
            smoking_status, alcohol_intake, physical_activity_level,
            family_history, diabetes, stress_level, salt_intake,
            sleep_duration, heart_rate,
            ldl_status,   -- كان ldl INT
            hdl_status,   -- كان hdl INT
            triglycerides, glucose, gender,
            education_level, employment_status,
            hypertension, disease_category
        )
        SELECT
            TRIM(Country)                               AS country,
            Age                                         AS age,
            BMI                                         AS bmi,
            Cholesterol                                 AS cholesterol,
            Systolic_BP                                 AS systolic_bp,
            Diastolic_BP                                AS diastolic_bp,
            TRIM(Smoking_Status)                        AS smoking_status,
            Alcohol_Intake                              AS alcohol_intake,
            TRIM(Physical_Activity_Level)               AS physical_activity_level,
            TRIM(Family_History)                        AS family_history,
            TRIM(Diabetes)                              AS diabetes,

            -- stress_level: 1-3 → Low / 4-6 → Medium / 7-9 → High
            CASE WHEN Stress_Level BETWEEN 1 AND 3 THEN 'Low'
                 WHEN Stress_Level BETWEEN 4 AND 6 THEN 'Medium'
                 WHEN Stress_Level BETWEEN 7 AND 9 THEN 'High'
                 ELSE 'Unknown'
            END                                         AS stress_level,

            Salt_Intake                                 AS salt_intake,
            Sleep_Duration                              AS sleep_duration,
            Heart_Rate                                  AS heart_rate,

            -- ldl: < 100 → Normal / >= 100 → High
            CASE WHEN LDL < 100  THEN 'Normal'
                 ELSE 'High'
            END                                         AS ldl_status,

            -- hdl: >= 60 → Good / < 60 → Poor
            CASE WHEN HDL >= 60  THEN 'Good'
                 ELSE 'Poor'
            END                                         AS hdl_status,

            Triglycerides                               AS triglycerides,
            Glucose                                     AS glucose,
            TRIM(Gender)                                AS gender,
            TRIM(Education_Level)                       AS education_level,
            TRIM(Employment_Status)                     AS employment_status,
            TRIM(Hypertension)                          AS hypertension,
            'Hypertension'                              AS disease_category
        FROM bronze.hypertension;
        -- RESULTS = 174,982 rows

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='

    END TRY
    BEGIN CATCH
        PRINT '=========================================='
        PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: '  + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: '   + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT '=========================================='
    END CATCH
END
GO


-- =============================================
-- Quality Check
-- =============================================

-- Row counts
SELECT COUNT(*) AS diabetes_rows     FROM silver.diabetes;      -- 96,146
SELECT COUNT(*) AS heart_rows        FROM silver.heart;         -- 10,000
SELECT COUNT(*) AS hypertension_rows FROM silver.hypertension;  -- 174,982

-- Check No Info removed from diabetes
SELECT COUNT(*) FROM silver.diabetes
WHERE smoking_history = 'No Info';                              -- 0

-- Check disease_category correct
SELECT DISTINCT disease_category FROM silver.diabetes;          -- 'Diabetes'
SELECT DISTINCT disease_category FROM silver.heart;             -- 'Heart Disease'
SELECT DISTINCT disease_category FROM silver.hypertension;      -- 'Hypertension'

-- Check no nulls in heart
SELECT COUNT(*) FROM silver.heart
WHERE gender IS NULL;                                           -- 0

-- Check None removed from alcohol
SELECT COUNT(*) FROM silver.heart
WHERE alcohol_consumption = 'None';                             -- 0

-- Check hdl_status values in heart
SELECT DISTINCT hdl_status FROM silver.heart;                   -- 'Good' / 'Poor' / 'Unknown'

-- Check ldl_status values in heart
SELECT DISTINCT ldl_status FROM silver.heart;                   -- 'Normal' / 'High' / 'Unknown'

-- Check stress_level values in both heart and hypertension
SELECT DISTINCT stress_level FROM silver.heart;                 -- 'High' / 'Medium' / 'Low' / 'Unknown'
SELECT DISTINCT stress_level FROM silver.hypertension;          -- 'High' / 'Medium' / 'Low'

-- Check hdl_status values in hypertension
SELECT DISTINCT hdl_status FROM silver.hypertension;            -- 'Good' / 'Poor'

-- Check ldl_status values in hypertension
SELECT DISTINCT ldl_status FROM silver.hypertension;            -- 'Normal' / 'High'
