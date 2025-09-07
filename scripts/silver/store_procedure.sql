exec silver.load_silver

create or alter procedure silver.load_silver as
begin
  declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
  begin try
    set @batch_start_time = GETDATE();
		print 'loading silver layer CRM ================================';

    -----1
    set @start_time = getdate();

    truncate table silver.crm_cust_info;
    insert into silver.crm_cust_info(
    	cst_id,
    	cst_key,
    	cst_firstname,
    	cst_lastname,
    	cst_marital_status,
    	cst_gndr,
    	cst_create_date)
    SELECT 
    cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case when upper(trim(cst_marital_status)) = 'S' then 'single'
         when upper(trim(cst_marital_status)) = 'M' then 'maried'
    	 else 'n/a'
    end cst_marital_status,
    case when upper(trim(cst_gndr)) = 'F' then 'female'
    	 when upper(trim(cst_gndr)) = 'M' then 'male'
    	 else 'n/a'
    end cst_gndr,
    cst_create_date
    FROM (SELECT 
    *,
    ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
    FROM [DataWarehouse].[bronze].[crm_cust_info]
    where cst_id is not null)t where flag_last = 1

    set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
      
    -----2
    set @start_time = getdate();

    truncate table silver.crm_prd_info;
    insert into silver.crm_prd_info(
    prd_id
    ,cat_id
    ,prd_key
    ,prd_nm
    ,prd_cost
    ,prd_line
    ,prd_start_dt
    ,prd_end_dt
    )
    SELECT [prd_id]
          ,replace(substring([prd_key],1,5), '-', '_') as cat_id
          ,substring([prd_key],7, len(prd_key)-6) as prd_key
          ,[prd_nm]
          ,isnull([prd_cost], 0) as prd_cost
          ,case when upper(trim([prd_line])) = 'M' then 'mountain'
                when upper(trim([prd_line])) = 'R' then 'road'
                when upper(trim([prd_line])) = 'S' then 'other sales'
                when upper(trim([prd_line])) = 'T' then 'touring'
                else 'n/a'
            end as prd_line
          ,cast([prd_start_dt] as date) as prd_start_dt
          ,cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
    FROM [bronze].[crm_prd_info]
      
    set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
    
    -----3
    set @start_time = getdate();

    truncate table silver.crm_sales_details;
    insert into [silver].[crm_sales_details]([sls_ord_num]
          ,[sls_prd_key]
          ,[sls_cust_id]
          ,[sls_order_dt]
          ,[sls_ship_dt]
          ,[sls_due_dt]
          ,[sls_sales]
          ,[sls_quantity]
          ,[sls_price]
    )
    SELECT [sls_ord_num]
          ,[sls_prd_key]
          ,[sls_cust_id]
          ,case when [sls_order_dt] = 0 or len(sls_order_dt) != 8 then null
                else cast(cast(sls_order_dt as varchar) as date)
           end sls_order_dt
          ,case when [sls_ship_dt] = 0 or len(sls_ship_dt) != 8 then null
               else cast(cast(sls_ship_dt as varchar) as date)
           end sls_ship_dt
          ,case when [sls_due_dt] = 0 or len(sls_due_dt) != 8 then null
               else cast(cast(sls_due_dt as varchar) as date)
           end sls_due_dt
          ,case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity*abs(sls_price)
            then sls_quantity*abs(sls_price)
            else sls_sales
           end sls_sales
          ,[sls_quantity]
          ,case when sls_price is null or sls_price <= 0
            then sls_sales / nullif(sls_quantity,0)
            else sls_price
           end sls_price
      FROM [DataWarehouse].[bronze].[crm_sales_details]

    set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
    
    -----4
    set @start_time = getdate();

    truncate table silver.erp_cust_az12;
    insert into silver.erp_cust_az12(
           [cid]
          ,[bdate]
          ,[gen])
    SELECT 
          case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
          else cid
          end as cid
          ,cast(case when bdate > getdate() then null
          else bdate
          end as date) as bdate
          ,case when upper(trim(gen)) in ('F', 'Female') then 'Female'
                when upper(trim(gen)) in ('M', 'Male') then 'Male'
           else 'n/a'
           end as gen
    FROM [bronze].[erp_cust_az12]

    set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
    
    -----5
    set @start_time = getdate();

    truncate table [silver].[erp_loc_a101];
    insert into [silver].[erp_loc_a101](
    cid
    ,cntry
    )
    SELECT replace([cid], '-', '') as cid
          ,case when trim(cntry) = 'DE' then 'Germany'
                when trim(cntry) in ('US', 'USA') then 'United States'
                when trim(cntry) = ' ' or cntry is null then 'n/a'
            else trim(cntry)
           end cntry
    FROM [DataWarehouse].[bronze].[erp_loc_a101]

    set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
    
    -----6
    set @start_time = getdate();

    truncate table silver.erp_px_cat_g1v2;
    insert into silver.erp_px_cat_g1v2(
           [id]
          ,[cat]
          ,[subcat]
          ,[maintenance]
    )
    select id
    ,cat
    ,subcat
    ,maintenance
    from bronze.erp_px_cat_g1v2

    set @end_time = getdate();
		print 'load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' s'
      
    set @batch_end_time = GETDATE();
		print 'loading done!'
		print 'total duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' s'
    -----
  end try
  begin catch
		print 'error message' + error_message();
		print 'error message' + error_number();
	end catch
end
