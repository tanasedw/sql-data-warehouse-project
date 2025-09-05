exec bronze.load_bronze

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print 'loading bronze layer CRM ================================';
		set @start_time = getdate();
		truncate table bronze.crm_cust_info
		bulk insert bronze.crm_cust_info
		from 'C:\Users\user\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'

	    set @start_time = getdate();
		truncate table bronze.crm_prd_info
		bulk insert bronze.crm_prd_info
		from 'C:\Users\user\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'

		set @start_time = getdate();
		truncate table bronze.crm_sales_details
		bulk insert bronze.crm_sales_details
		from 'C:\Users\user\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
	
		print 'loading bronze layer ERP ================================';

		set @start_time = getdate();
		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\user\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'

		set @start_time = getdate();
		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\user\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'

		set @start_time = getdate();
		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\user\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
		set @batch_end_time = GETDATE();
		print 'loading done!'
		print 'total duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' s'

	end try
	begin catch
		print 'error message' + error_message();
		print 'error message' + error_number();
	end catch
end
