/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
Medallion Architecture - Medical Data Warehouse

Age pipeline:
  1. Inverse scale: Diabetes ages < 2 mapped from 0.08-1.88 back to 2-80
  2. Normalize: all ages → 0-1 using global min=2, max=89
  3. Bin on normalized value → 4 groups (Young/Middle/Mature/Senior)

Usage: EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Silver Layer';
        PRINT '==========================================';


        -- ============================================
        -- 1. DIABETES
        -- ============================================
        PRINT '------------------------------------------------';
        PRINT 'Loading Diabetes Table';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.diabetes;

        INSERT INTO silver.diabetes (
            gender, age, age_normalized, age_level,
            bmi, bmi_level, smoking, diabetes, composite_key,
            hypertension, heart_disease, HbA1c_level, glucose,
            disease_flags, disease_category
        )
        SELECT
            gender,
            age_fixed AS age,

            -- Normalize: (age - global_min) / (global_max - global_min)
            (age_fixed - 2.0) / 87.0 AS age_normalized,

            -- Bin on normalized value (4 groups)
            CASE
                WHEN (age_fixed - 2.0) / 87.0 < 0.25 THEN 'Young'
                WHEN (age_fixed - 2.0) / 87.0 < 0.50 THEN 'Middle'
                WHEN (age_fixed - 2.0) / 87.0 < 0.75 THEN 'Mature'
                ELSE 'Senior'
            END AS age_level,

            bmi,

            -- BMI: WHO categories
            CASE
                WHEN bmi < 18.5 THEN 'Underweight'
                WHEN bmi < 25   THEN 'Normal'
                WHEN bmi < 30   THEN 'Overweight'
                ELSE 'Obese'
            END AS bmi_level,

            -- Smoking: 6 values → 3 + Unknown
            CASE
                WHEN LOWER(TRIM(smoking_history)) = 'never'       THEN 'Never'
                WHEN LOWER(TRIM(smoking_history)) = 'current'     THEN 'Current'
                WHEN LOWER(TRIM(smoking_history)) IN ('former','ever','not current') THEN 'Former'
                ELSE 'Unknown'
            END AS smoking,

            -- Diabetes: 0/1 → Yes/No
            CASE WHEN diabetes = 1 THEN 'Yes' ELSE 'No' END AS diabetes,

            -- Composite key: age_level + gender + bmi_level + smoking + diabetes
            CASE
                WHEN (age_fixed - 2.0) / 87.0 < 0.25 THEN 'Young'
                WHEN (age_fixed - 2.0) / 87.0 < 0.50 THEN 'Middle'
                WHEN (age_fixed - 2.0) / 87.0 < 0.75 THEN 'Mature'
                ELSE 'Senior'
            END + '_' +
            gender + '_' +
            CASE
                WHEN bmi < 18.5 THEN 'Underweight'
                WHEN bmi < 25   THEN 'Normal'
                WHEN bmi < 30   THEN 'Overweight'
                ELSE 'Obese'
            END + '_' +
            CASE
                WHEN LOWER(TRIM(smoking_history)) = 'never'       THEN 'Never'
                WHEN LOWER(TRIM(smoking_history)) = 'current'     THEN 'Current'
                WHEN LOWER(TRIM(smoking_history)) IN ('former','ever','not current') THEN 'Former'
                ELSE 'Unknown'
            END + '_' +
            CASE WHEN diabetes = 1 THEN 'Yes' ELSE 'No' END
            AS composite_key,

            hypertension,
            heart_disease,
            HbA1c_level,
            blood_glucose_level AS glucose,

            -- Disease flags: (DI,HT,HY) from real columns in this dataset
            CAST(diabetes AS VARCHAR) + ',' + CAST(heart_disease AS VARCHAR) + ',' + CAST(hypertension AS VARCHAR) AS disease_flags,

            CASE WHEN diabetes = 1 THEN 'Diabetes' ELSE 'Healthy' END AS disease_category

        FROM (
            -- Subquery: fix scaled ages + remove duplicates
            SELECT *,
                -- Inverse scale: map 0.08-1.88 back to 2-80
                CASE WHEN age < 2 THEN ROUND((age - 0.08) / 1.8 * 78 + 2, 0) ELSE age END AS age_fixed,
                ROW_NUMBER() OVER (
                    PARTITION BY gender, age, hypertension, heart_disease,
                                 smoking_history, bmi, HbA1c_level,
                                 blood_glucose_level, diabetes
                    ORDER BY (SELECT NULL)
                ) AS row_flag
            FROM bronze.diabetes
            WHERE gender != 'Other'     -- Remove 18 Other gender rows
        ) t
        WHERE row_flag = 1;             -- Remove 3854 duplicates

        SET @end_time = GETDATE();
        PRINT '>> Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows, ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';

        -- Fix Unknown smoking: stratified imputation (group mode)
        PRINT '>> Imputing Unknown smoking (group mode)';
        UPDATE d
        SET d.smoking = g.mode_smoking
        FROM silver.diabetes d
        INNER JOIN (
            SELECT age_level, gender, bmi_level, diabetes, smoking AS mode_smoking
            FROM (
                SELECT age_level, gender, bmi_level, diabetes, smoking,
                       ROW_NUMBER() OVER (PARTITION BY age_level, gender, bmi_level, diabetes ORDER BY COUNT(*) DESC) AS rn
                FROM silver.diabetes WHERE smoking != 'Unknown'
                GROUP BY age_level, gender, bmi_level, diabetes, smoking
            ) ranked WHERE rn = 1
        ) g ON d.age_level = g.age_level AND d.gender = g.gender
           AND d.bmi_level = g.bmi_level AND d.diabetes = g.diabetes
        WHERE d.smoking = 'Unknown';

        -- Fallback: remaining Unknown → overall mode (Never)
        UPDATE silver.diabetes
        SET smoking = (SELECT TOP 1 smoking FROM silver.diabetes WHERE smoking != 'Unknown' GROUP BY smoking ORDER BY COUNT(*) DESC)
        WHERE smoking = 'Unknown';

        -- Rebuild composite key after smoking fix
        UPDATE silver.diabetes
        SET composite_key = age_level + '_' + gender + '_' + bmi_level + '_' + smoking + '_' + diabetes
        WHERE composite_key LIKE '%Unknown%';

        PRINT '>> Smoking imputation done';


        -- ============================================
        -- 2. HEART
        -- ============================================
        PRINT '------------------------------------------------';
        PRINT 'Loading Heart Table';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.heart;

        INSERT INTO silver.heart (
            gender, age, age_normalized, age_level,
            bmi, bmi_level, smoking, diabetes, composite_key,
            cholesterol, physical_activity, family_history, stress_level,
            sleep_hours, triglycerides, glucose, low_hdl_cholesterol, high_ldl_cholesterol,
            blood_pressure, high_blood_pressure, sugar_consumption, crp_level, homocysteine_level,
            disease_flags, disease_category
        )
        SELECT
            Gender,
            Age,

            -- Normalize: global min=2, max=89
            (Age - 2.0) / 87.0 AS age_normalized,

            -- Bin on normalized value
            CASE
                WHEN (Age - 2.0) / 87.0 < 0.25 THEN 'Young'
                WHEN (Age - 2.0) / 87.0 < 0.50 THEN 'Middle'
                WHEN (Age - 2.0) / 87.0 < 0.75 THEN 'Mature'
                ELSE 'Senior'
            END AS age_level,

            BMI,
            CASE
                WHEN BMI < 18.5 THEN 'Underweight'
                WHEN BMI < 25   THEN 'Normal'
                WHEN BMI < 30   THEN 'Overweight'
                ELSE 'Obese'
            END AS bmi_level,

            -- Smoking: Yes→Current, No→Never
            CASE WHEN UPPER(TRIM(Smoking)) = 'YES' THEN 'Current' WHEN UPPER(TRIM(Smoking)) = 'NO' THEN 'Never' ELSE 'Unknown' END AS smoking,

            Diabetes,

            -- Composite key
            CASE
                WHEN (Age - 2.0) / 87.0 < 0.25 THEN 'Young'
                WHEN (Age - 2.0) / 87.0 < 0.50 THEN 'Middle'
                WHEN (Age - 2.0) / 87.0 < 0.75 THEN 'Mature'
                ELSE 'Senior'
            END + '_' +
            Gender + '_' +
            CASE WHEN BMI < 18.5 THEN 'Underweight' WHEN BMI < 25 THEN 'Normal' WHEN BMI < 30 THEN 'Overweight' ELSE 'Obese' END + '_' +
            CASE WHEN UPPER(TRIM(Smoking)) = 'YES' THEN 'Current' WHEN UPPER(TRIM(Smoking)) = 'NO' THEN 'Never' ELSE 'Unknown' END + '_' +
            Diabetes
            AS composite_key,

            -- Unified columns (renamed for consistency)
            Cholesterol_Level AS cholesterol,
            CASE WHEN UPPER(TRIM(Exercise_Habits)) = 'MEDIUM' THEN 'Moderate' ELSE TRIM(Exercise_Habits) END AS physical_activity,
            Family_Heart_Disease AS family_history,
            Stress_Level AS stress_level,
            Sleep_Hours AS sleep_hours,
            Triglyceride_Level AS triglycerides,
            Fasting_Blood_Sugar AS glucose,
            Low_HDL_Cholesterol AS low_hdl_cholesterol,
            High_LDL_Cholesterol AS high_ldl_cholesterol,

            -- Heart-only columns
            Blood_Pressure AS blood_pressure,
            High_Blood_Pressure AS high_blood_pressure,
            Sugar_Consumption AS sugar_consumption,
            CRP_Level AS crp_level,
            Homocysteine_Level AS homocysteine_level,

            -- Disease flags: (DI,HT,HY) from real columns in this dataset
            CASE WHEN UPPER(TRIM(Diabetes)) = 'YES' THEN '1' ELSE '0' END + ',' +
            CASE WHEN UPPER(TRIM(Heart_Disease_Status)) = 'YES' THEN '1' ELSE '0' END + ',' +
            CASE WHEN UPPER(TRIM(High_Blood_Pressure)) = 'YES' THEN '1' ELSE '0' END AS disease_flags,

            CASE WHEN UPPER(TRIM(Heart_Disease_Status)) = 'YES' THEN 'Heart' ELSE 'Healthy' END AS disease_category

        FROM bronze.heart
        WHERE Gender IS NOT NULL AND Age IS NOT NULL AND BMI IS NOT NULL
          AND Smoking IS NOT NULL AND Diabetes IS NOT NULL;

        SET @end_time = GETDATE();
        PRINT '>> Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows, ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';

        -- Impute 343 single-null rows: numerical → group mean
        PRINT '>> Imputing Heart nulls (numerical → group mean)';

        UPDATE h SET h.cholesterol = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, AVG(cholesterol) AS v FROM silver.heart WHERE cholesterol IS NOT NULL GROUP BY composite_key) g ON h.composite_key = g.composite_key WHERE h.cholesterol IS NULL;
        UPDATE h SET h.sleep_hours = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, AVG(sleep_hours) AS v FROM silver.heart WHERE sleep_hours IS NOT NULL GROUP BY composite_key) g ON h.composite_key = g.composite_key WHERE h.sleep_hours IS NULL;
        UPDATE h SET h.triglycerides = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, AVG(triglycerides) AS v FROM silver.heart WHERE triglycerides IS NOT NULL GROUP BY composite_key) g ON h.composite_key = g.composite_key WHERE h.triglycerides IS NULL;
        UPDATE h SET h.glucose = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, AVG(glucose) AS v FROM silver.heart WHERE glucose IS NOT NULL GROUP BY composite_key) g ON h.composite_key = g.composite_key WHERE h.glucose IS NULL;
        UPDATE h SET h.blood_pressure = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, AVG(blood_pressure) AS v FROM silver.heart WHERE blood_pressure IS NOT NULL GROUP BY composite_key) g ON h.composite_key = g.composite_key WHERE h.blood_pressure IS NULL;
        UPDATE h SET h.crp_level = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, AVG(crp_level) AS v FROM silver.heart WHERE crp_level IS NOT NULL GROUP BY composite_key) g ON h.composite_key = g.composite_key WHERE h.crp_level IS NULL;
        UPDATE h SET h.homocysteine_level = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, AVG(homocysteine_level) AS v FROM silver.heart WHERE homocysteine_level IS NOT NULL GROUP BY composite_key) g ON h.composite_key = g.composite_key WHERE h.homocysteine_level IS NULL;

        -- Impute: categorical → group mode
        PRINT '>> Imputing Heart nulls (categorical → group mode)';

        UPDATE h SET h.physical_activity = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, physical_activity AS v FROM (SELECT composite_key, physical_activity, ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY COUNT(*) DESC) AS rn FROM silver.heart WHERE physical_activity IS NOT NULL GROUP BY composite_key, physical_activity) r WHERE rn=1) g ON h.composite_key = g.composite_key WHERE h.physical_activity IS NULL;
        UPDATE h SET h.family_history = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, family_history AS v FROM (SELECT composite_key, family_history, ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY COUNT(*) DESC) AS rn FROM silver.heart WHERE family_history IS NOT NULL GROUP BY composite_key, family_history) r WHERE rn=1) g ON h.composite_key = g.composite_key WHERE h.family_history IS NULL;
        UPDATE h SET h.stress_level = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, stress_level AS v FROM (SELECT composite_key, stress_level, ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY COUNT(*) DESC) AS rn FROM silver.heart WHERE stress_level IS NOT NULL GROUP BY composite_key, stress_level) r WHERE rn=1) g ON h.composite_key = g.composite_key WHERE h.stress_level IS NULL;
        UPDATE h SET h.high_blood_pressure = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, high_blood_pressure AS v FROM (SELECT composite_key, high_blood_pressure, ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY COUNT(*) DESC) AS rn FROM silver.heart WHERE high_blood_pressure IS NOT NULL GROUP BY composite_key, high_blood_pressure) r WHERE rn=1) g ON h.composite_key = g.composite_key WHERE h.high_blood_pressure IS NULL;
        UPDATE h SET h.low_hdl_cholesterol = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, low_hdl_cholesterol AS v FROM (SELECT composite_key, low_hdl_cholesterol, ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY COUNT(*) DESC) AS rn FROM silver.heart WHERE low_hdl_cholesterol IS NOT NULL GROUP BY composite_key, low_hdl_cholesterol) r WHERE rn=1) g ON h.composite_key = g.composite_key WHERE h.low_hdl_cholesterol IS NULL;
        UPDATE h SET h.high_ldl_cholesterol = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, high_ldl_cholesterol AS v FROM (SELECT composite_key, high_ldl_cholesterol, ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY COUNT(*) DESC) AS rn FROM silver.heart WHERE high_ldl_cholesterol IS NOT NULL GROUP BY composite_key, high_ldl_cholesterol) r WHERE rn=1) g ON h.composite_key = g.composite_key WHERE h.high_ldl_cholesterol IS NULL;
        UPDATE h SET h.sugar_consumption = g.v FROM silver.heart h INNER JOIN (SELECT composite_key, sugar_consumption AS v FROM (SELECT composite_key, sugar_consumption, ROW_NUMBER() OVER (PARTITION BY composite_key ORDER BY COUNT(*) DESC) AS rn FROM silver.heart WHERE sugar_consumption IS NOT NULL GROUP BY composite_key, sugar_consumption) r WHERE rn=1) g ON h.composite_key = g.composite_key WHERE h.sugar_consumption IS NULL;

        PRINT '>> Heart imputation done';


        -- ============================================
        -- 3. HYPERTENSION
        -- ============================================
        PRINT '------------------------------------------------';
        PRINT 'Loading Hypertension Table';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.hypertension;

        INSERT INTO silver.hypertension (
            gender, age, age_normalized, age_level,
            bmi, bmi_level, smoking, diabetes, composite_key,
            cholesterol, physical_activity, family_history, stress_level,
            sleep_hours, triglycerides, glucose, low_hdl_cholesterol, high_ldl_cholesterol,
            systolic_bp, diastolic_bp, alcohol_intake, salt_intake, heart_rate,
            hdl, ldl, education_level, employment_status,
            disease_flags, disease_category
        )
        SELECT
            Gender,
            Age,

            -- Normalize: global min=2, max=89
            (Age - 2.0) / 87.0 AS age_normalized,

            -- Bin on normalized value
            CASE
                WHEN (Age - 2.0) / 87.0 < 0.25 THEN 'Young'
                WHEN (Age - 2.0) / 87.0 < 0.50 THEN 'Middle'
                WHEN (Age - 2.0) / 87.0 < 0.75 THEN 'Mature'
                ELSE 'Senior'
            END AS age_level,

            BMI,
            CASE WHEN BMI < 18.5 THEN 'Underweight' WHEN BMI < 25 THEN 'Normal' WHEN BMI < 30 THEN 'Overweight' ELSE 'Obese' END AS bmi_level,

            -- Smoking: already Current/Former/Never
            Smoking_Status AS smoking,
            Diabetes AS diabetes,

            -- Composite key
            CASE
                WHEN (Age - 2.0) / 87.0 < 0.25 THEN 'Young'
                WHEN (Age - 2.0) / 87.0 < 0.50 THEN 'Middle'
                WHEN (Age - 2.0) / 87.0 < 0.75 THEN 'Mature'
                ELSE 'Senior'
            END + '_' +
            Gender + '_' +
            CASE WHEN BMI < 18.5 THEN 'Underweight' WHEN BMI < 25 THEN 'Normal' WHEN BMI < 30 THEN 'Overweight' ELSE 'Obese' END + '_' +
            Smoking_Status + '_' +
            Diabetes
            AS composite_key,

            -- Unified columns
            Cholesterol AS cholesterol,
            Physical_Activity_Level AS physical_activity,
            Family_History AS family_history,
            -- Stress: 1-9 → Low/Medium/High
            CASE WHEN Stress_Level BETWEEN 1 AND 3 THEN 'Low' WHEN Stress_Level BETWEEN 4 AND 6 THEN 'Medium' WHEN Stress_Level BETWEEN 7 AND 9 THEN 'High' ELSE 'Unknown' END AS stress_level,
            Sleep_Duration AS sleep_hours,
            Triglycerides AS triglycerides,
            Glucose AS glucose,
            -- HDL/LDL: medical thresholds → Yes/No
            CASE WHEN HDL < 40 THEN 'Yes' ELSE 'No' END AS low_hdl_cholesterol,
            CASE WHEN LDL >= 130 THEN 'Yes' ELSE 'No' END AS high_ldl_cholesterol,

            -- Hypertension-only columns
            Systolic_BP, Diastolic_BP, Alcohol_Intake, Salt_Intake, Heart_Rate,
            HDL, LDL, Education_Level, Employment_Status,

            -- Disease flags: (DI,HT,HY). HT=0 always (no heart disease data in this dataset)
            CASE WHEN UPPER(TRIM(Diabetes)) = 'YES' THEN '1' ELSE '0' END + ',' +
            '0' + ',' +
            CASE WHEN UPPER(TRIM(Hypertension)) = 'HIGH' THEN '1' ELSE '0' END AS disease_flags,

            CASE WHEN UPPER(TRIM(Hypertension)) = 'HIGH' THEN 'Hypertension' ELSE 'Healthy' END AS disease_category

        FROM bronze.hypertension;
        -- Cleanest dataset: no nulls, no duplicates. Country dropped (not in other datasets).

        SET @end_time = GETDATE();
        PRINT '>> Loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows, ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 's';


        -- ============================================
        -- DONE
        -- ============================================
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Silver Layer Complete - ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 's total';
        PRINT '==========================================';

    END TRY
    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR: ' + ERROR_MESSAGE();
        PRINT '==========================================';
    END CATCH
END
