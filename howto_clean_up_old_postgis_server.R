library(ggplot2)
library(DBI)
library(RPostgres)
library(sf)
library(leaflet)
library(mapview)
con <- dbConnect(
  RPostgres::Postgres(),
  host = "192.168.2.15",
  dbname = "postgres",
  port = 5432,
  user = "postgres",
  password = "postgres"
)

schemas <- dbGetQuery(con, "
  SELECT schema_name
  FROM information_schema.schemata
  ORDER BY schema_name;
")
print(schemas)

tables <- dbGetQuery(con, "
  SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_type = 'BASE TABLE'
  ORDER BY table_schema, table_name;
")

print(tables)


# drop individual tables
dbExecute(con, "DROP TABLE IF EXISTS my_schema.iris CASCADE;")
dbExecute(con, "DROP TABLE IF EXISTS my_schema.iris_computed_from_con CASCADE;")
dbExecute(con, "DROP TABLE IF EXISTS my_schema.iris_computed_from_pool CASCADE;")

# move tables from  my_schema to public 
dbExecute(con, "ALTER TABLE my_schema.batiments SET SCHEMA public;")
dbExecute(con, "ALTER TABLE my_schema.batiments_4326 SET SCHEMA public;")
dbExecute(con, "ALTER TABLE my_schema.outages SET SCHEMA public;")


