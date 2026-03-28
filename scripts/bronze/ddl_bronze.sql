/*
====================================================================
DDL Script: Create Bronze Tables
====================================================================

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables

====================================================================
*/


IF OBJECT_ID('bronze.diabetes', 'U') IS NOT NULL
    DROP TABLE bronze.diabetes;
GO

CREATE TABLE bronze.diabetes (
	gender             VARCHAR(20),
    age                FLOAT,
    hypertension       INT,
    heart_disease      INT,
    smoking_history    VARCHAR(30),
    bmi                FLOAT,
    HbA1c_level        FLOAT,
    blood_glucose_level INT,
    diabetes           INT
);
GO

IF OBJECT_ID('bronze.heart', 'U') IS NOT NULL
    DROP TABLE bronze.heart;
GO

CREATE TABLE bronze.heart (
    Age                  FLOAT,
    Gender               VARCHAR(10),
    Blood_Pressure       FLOAT,
    Cholesterol_Level    FLOAT,
    Exercise_Habits      VARCHAR(20),
    Smoking              VARCHAR(10),
    Family_Heart_Disease VARCHAR(10),
    Diabetes             VARCHAR(10),
    BMI                  FLOAT,
    High_Blood_Pressure  VARCHAR(10),
    Low_HDL_Cholesterol  VARCHAR(10),
    High_LDL_Cholesterol VARCHAR(10),
    Alcohol_Consumption  VARCHAR(20),
    Stress_Level         VARCHAR(20),
    Sleep_Hours          FLOAT,
    Sugar_Consumption    VARCHAR(20),
    Triglyceride_Level   FLOAT,
    Fasting_Blood_Sugar  FLOAT,
    CRP_Level            FLOAT,
    Homocysteine_Level   FLOAT,
    Heart_Disease_Status VARCHAR(10)
);
GO

IF OBJECT_ID('bronze.hypertension', 'U') IS NOT NULL
    DROP TABLE bronze.hypertension;
GO

CREATE TABLE bronze.hypertension ( 
    Country                 VARCHAR(50),
    Age                     INT,
    BMI                     FLOAT,
    Cholesterol             INT,
    Systolic_BP             INT,
    Diastolic_BP            INT,
    Smoking_Status          VARCHAR(20),
    Alcohol_Intake          FLOAT,
    Physical_Activity_Level VARCHAR(20),
    Family_History          VARCHAR(10),
    Diabetes                VARCHAR(10),
    Stress_Level            INT,
    Salt_Intake             FLOAT,
    Sleep_Duration          FLOAT,
    Heart_Rate              INT,
    LDL                     INT,
    HDL                     INT,
    Triglycerides           INT,
    Glucose                 INT,
    Gender                  VARCHAR(10),
    Education_Level         VARCHAR(20),
    Employment_Status       VARCHAR(20),
    Hypertension            VARCHAR(10)
);
GO
