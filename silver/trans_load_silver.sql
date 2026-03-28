
/*
Stored Procedure: Load Silver Layer (Bronze -> Silver)
Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT 'Loading Silver Layer';

 SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.diabetes';
		TRUNCATE TABLE silver.diabetes;
		PRINT '>> Inserting Data Into: silver.diabetes';
        INSERT INTO silver.diabetes (
        gender, age, hypertension, heart_disease,
        smoking_history, bmi, HbA1c_level,
        blood_glucose_level, diabetes,
        disease_category
    )

SELECT
        TRIM(gender) AS gender,
        age,
        hypertension,
        heart_disease,
        CASE WHEN TRIM(smoking_history) = 'No Info'
             THEN 'Unknown'
             ELSE TRIM(smoking_history)
        END                                         AS smoking_history,
        bmi,
        HbA1c_level,
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


              SET @start_time = GETDATE();
		      PRINT '>> Truncating Table: silver.heart';
		      TRUNCATE TABLE silver.heart;
		      PRINT '>> Inserting Data Into: silver.heart';
              INSERT INTO silver.heart (
                    gender, age, blood_pressure, cholesterol_level,
                    exercise_habits, smoking, family_heart_disease,
                    diabetes, bmi, high_blood_pressure, low_hdl_cholesterol,
                    high_ldl_cholesterol, alcohol_consumption, stress_level,
                    sleep_hours, sugar_consumption, triglyceride_level,
                    fasting_blood_sugar, crp_level, homocysteine_level,
                    heart_disease_status,
                    disease_category
                  )
SELECT
    ISNULL(TRIM(Gender), 'Unknown')               AS gender,
    Age                                           AS age,
    Blood_Pressure                                AS blood_pressure,
    Cholesterol_Level                             AS cholesterol_level,
    ISNULL(TRIM(Exercise_Habits), 'Unknown')      AS exercise_habits,
    ISNULL(TRIM(Smoking), 'Unknown')              AS smoking,
    ISNULL(TRIM(Family_Heart_Disease), 'Unknown') AS family_heart_disease,
    ISNULL(TRIM(Diabetes), 'Unknown')             AS diabetes,
    BMI                                           AS bmi,
    ISNULL(TRIM(High_Blood_Pressure), 'Unknown')  AS high_blood_pressure,
    ISNULL(TRIM(Low_HDL_Cholesterol), 'Unknown')  AS low_hdl_cholesterol,
    ISNULL(TRIM(High_LDL_Cholesterol), 'Unknown') AS high_ldl_cholesterol,
    CASE WHEN Alcohol_Consumption IS NULL
              OR TRIM(Alcohol_Consumption) = 'None'
         THEN 'Unknown'
         ELSE TRIM(Alcohol_Consumption)
    END                                           AS alcohol_consumption,
    ISNULL(TRIM(Stress_Level), 'Unknown')         AS stress_level,
    Sleep_Hours                                   AS sleep_hours,
    ISNULL(TRIM(Sugar_Consumption), 'Unknown')    AS sugar_consumption,
    Triglyceride_Level                            AS triglyceride_level,
    Fasting_Blood_Sugar                           AS fasting_blood_sugar,
    CRP_Level                                     AS crp_level,
    Homocysteine_Level                            AS homocysteine_level,
    TRIM(Heart_Disease_Status)                    AS heart_disease_status,
    'Heart Disease'                               AS disease_category
FROM bronze.heart;
-- RESULTS = 10,000 rows
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.hypertension';
		TRUNCATE TABLE silver.hypertension;
		PRINT '>> Inserting Data Into: silver.hypertension';
INSERT INTO silver.hypertension (
    country, age, bmi, cholesterol, systolic_bp, diastolic_bp,
    smoking_status, alcohol_intake, physical_activity_level,
    family_history, diabetes, stress_level, salt_intake,
    sleep_duration, heart_rate, ldl, hdl, triglycerides,
    glucose, gender, education_level, employment_status,
    hypertension,
    disease_category
)
SELECT
    TRIM(Country)                   AS country,
    Age                             AS age,
    BMI                             AS bmi,
    Cholesterol                     AS cholesterol,
    Systolic_BP                     AS systolic_bp,
    Diastolic_BP                    AS diastolic_bp,
    TRIM(Smoking_Status)            AS smoking_status,
    Alcohol_Intake                  AS alcohol_intake,
    TRIM(Physical_Activity_Level)   AS physical_activity_level,
    TRIM(Family_History)            AS family_history,
    TRIM(Diabetes)                  AS diabetes,
    Stress_Level                    AS stress_level,
    Salt_Intake                     AS salt_intake,
    Sleep_Duration                  AS sleep_duration,
    Heart_Rate                      AS heart_rate,
    LDL                             AS ldl,
    HDL                             AS hdl,
    Triglycerides                   AS triglycerides,
    Glucose                         AS glucose,
    TRIM(Gender)                    AS gender,
    TRIM(Education_Level)           AS education_level,
    TRIM(Employment_Status)         AS employment_status,
    TRIM(Hypertension)              AS hypertension,
    'Hypertension'                  AS disease_category
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
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END



-- =============================================
-- Quality Check
-- =============================================
-- Row counts
SELECT COUNT(*) AS diabetes_rows     FROM silver.diabetes;      -- 96,146
SELECT COUNT(*) AS heart_rows        FROM silver.heart;         -- 10,000
SELECT COUNT(*) AS hypertension_rows FROM silver.hypertension;  -- 174,982

-- Check No Info removed from diabetes
SELECT COUNT(*) FROM silver.diabetes
WHERE smoking_history = 'No Info';    -- 0

-- Check disease_category correct
SELECT DISTINCT disease_category FROM silver.diabetes;      -- 'Diabetes'
SELECT DISTINCT disease_category FROM silver.heart;         -- 'Heart Disease'
SELECT DISTINCT disease_category FROM silver.hypertension;  -- 'Hypertension'

-- Check no nulls in heart
SELECT COUNT(*) FROM silver.heart
WHERE gender IS NULL;                 -- 0

-- Check None removed from alcohol
SELECT COUNT(*) FROM silver.heart
WHERE alcohol_consumption = 'None';   -- 0


-- =============================================
-- 1. DIABETES
-- =============================================
-- Checked Duplicates 
-- SELECT gender, age, hypertension, heart_disease,
--        smoking_history, bmi, HbA1c_level,
--        blood_glucose_level, diabetes,
--        COUNT(*) AS duplicate_count
-- FROM bronze.diabetes
-- GROUP BY gender, age, hypertension, heart_disease,
--          smoking_history, bmi, HbA1c_level,
--          blood_glucose_level, diabetes
-- HAVING COUNT(*) > 1
-- ORDER BY duplicate_count DESC;
-- RESULTS = 3854 duplicated rows
-- Load to Silver (Remove Duplicates + TRIM + No Info → Unknown + disease_category)

-- =============================================
-- 2. HEART
-- =============================================
-- Checked Duplicates 
-- RESULTS = 0 duplicated rows
-- Load to Silver (TRIM + NULL → Unknown + None → Unknown + disease_category)


-- =============================================
-- 3. HYPERTENSION
-- =============================================
-- Checked Duplicates, Nulls
-- RESULTS = 0 duplicated rows
-- Load to Silver (Only Trim + No Nulls + disease_category)
