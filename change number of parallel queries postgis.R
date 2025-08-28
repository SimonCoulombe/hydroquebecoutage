How to increase number of parallel workers on postgis docker:
  
* terminal in postgis , then run 
`psql -U POSTGIS_USERNAME -d POSTGIS_DBNAME`

roulez ces 4 commandes:
ALTER SYSTEM SET max_parallel_workers_per_gather = 5;
ALTER SYSTEM SET max_parallel_workers = 5;
ALTER SYSTEM SET parallel_setup_cost = 0;
ALTER SYSTEM  SET parallel_tuple_cost = 0;
puis reload la conf:  
  SELECT pg_reload_conf();


# alternativement, à partir de R 
dbExecute(con, "ALTER SYSTEM SET max_parallel_workers_per_gather = 5;")
dbExecute(con, "ALTER SYSTEM SET max_parallel_workers = 5;")      
dbExecute(con, "ALTER SYSTEM SET parallel_setup_cost = 0;")        
dbExecute(con, "ALTER SYSTEM  SET parallel_tuple_cost = 0;")        
