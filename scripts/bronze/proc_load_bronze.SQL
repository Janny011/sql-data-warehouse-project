/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
===============================================================================
*/

EXEC bronze.load_bronze

-- Step 2: Creating stored procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	-- Step 4: Find time taken to load data
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	-- Step 3: Handling bugs
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		-- Step 1: Load data into table
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info		-- Only if you want to empty the table
												-- Otherwize, ignore command

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL2022\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.crm_cust_info'
		PRINT '>> load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info		-- Only if you want to empty the table
												-- Otherwize, ignore command

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQL2022\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.crm_prd_info'
		PRINT '>> load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details		-- Only if you want to empty the table
													-- Otherwize, ignore command

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\SQL2022\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.crm_sales_details'
		PRINT '>> load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details		-- Only if you want to empty the table
													-- Otherwize, ignore command

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\SQL2022\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.erp_cust_az12'
		PRINT '>> load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details		-- Only if you want to empty the table
													-- Otherwize, ignore command

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\SQL2022\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.erp_loc_a101'
		PRINT '>> load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2		-- Only if you want to empty the table
													-- Otherwize, ignore command

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\SQL2022\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT 'bronze.erp_px_cat_g1v2'
		PRINT '>> load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------'

	SET @batch_end_time = GETDATE();
	PRINT '======================================='
	PRINT 'Bronze layer is completed'
	PRINT '- Total load duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '======================================='

	END TRY
	BEGIN CATCH
		PRINT '=================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================================='
	END CATCH
END
