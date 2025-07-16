
  
    
    
    
        
        insert into default.stg_sales__dbt_backup
        ("year", "anzsic_descriptor", "data_value")

select year, anzsic_descriptor, data_value
from 
default.sales
  
  