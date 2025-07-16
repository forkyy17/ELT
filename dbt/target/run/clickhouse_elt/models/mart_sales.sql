
  
    
    
    
        
        insert into default.mart_sales__dbt_backup
        ("year", "data_value")
select year, data_value
FROM default.stg_sales
  
  