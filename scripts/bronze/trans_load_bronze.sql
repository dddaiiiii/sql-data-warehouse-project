/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)
Usage Example:  EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
     DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
     BEGIN TRY
            SET @batch_start_time = GETDATE();
            PRINT '=========================================================';
            PRINT 'Loading Bronze Layer';
            PRINT '=========================================================';

            PRINT '_________________________________________________________';
            PRINT 'Loading Diseases Datasets';  
            PRINT '_________________________________________________________';

                    SET @start_time = GETDATE();
                    PRINT '>> Truncating Table: Diabetes'
                    TRUNCATE TABLE bronze.diabetes
                    PRINT '>> Inserting Data Into: Diabetes'
                    BULK INSERT bronze.diabetes
                    FROM 'D:\Building Medical DataWarehouse\datasets\diabetes.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );
                   SET @end_time = GETDATE();
                    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		            PRINT '>> -------------';



                    SET @start_time = GETDATE();
                    PRINT '>> Truncating Table: Heart'
                    TRUNCATE TABLE bronze.heart
                    PRINT '>> Inserting Data Into: Heart'
                    BULK INSERT bronze.heart
                    FROM 'D:\Building Medical DataWarehouse\datasets\heart.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );
                    SET @end_time = GETDATE();
                    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		            PRINT '>> -------------';



                    SET @start_time = GETDATE();
                    PRINT '>> Truncating Table: Hypertension'
                    TRUNCATE TABLE bronze.hypertension
                    PRINT '>> Inserting Data Into: Hypertension'
                    BULK INSERT bronze.hypertension
                    FROM 'D:\Building Medical DataWarehouse\datasets\hypertension.csv'
                    WITH (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '0x0a',
                        TABLOCK
                    );
                   SET @end_time = GETDATE();
		           PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		           PRINT '>> -------------'; 
                   

                   SET @batch_end_time = GETDATE();
    	PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
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