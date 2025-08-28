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

# move tables from public to my_schema
dbExecute(con, "ALTER TABLE public.batiments SET SCHEMA my_schema;")
dbExecute(con, "ALTER TABLE public.batiments_4326 SET SCHEMA my_schema;")
dbExecute(con, "ALTER TABLE public.outages SET SCHEMA my_schema;")


# buildings in outage!
buildings_in_outages <- dbGetQuery(con, "
  SELECT DISTINCT b.*
  FROM my_schema.batiments_4326 AS b
  JOIN my_schema.outages AS o
    ON ST_Intersects(b.geom, o.geom);
")

geom_cols <- dbGetQuery(con, "
  SELECT f_table_schema, f_table_name, f_geometry_column, srid, type
  FROM geometry_columns
  WHERE f_table_schema = 'my_schema'
    AND f_table_name IN ('batiments_4326', 'outages');
")

print(geom_cols)


buildings_sf <- st_read(con, query = "
  SELECT DISTINCT b.*
  FROM my_schema.batiments_4326 AS b
  JOIN my_schema.outages AS o
    ON ST_Intersects(b.geom, o.geometry);
")

outages_sf <- st_read(con, query = "
  SELECT *
  FROM my_schema.outages;
")

ggplot() +
  geom_sf(data = outages_sf, fill = "red", alpha = 0.3) +
  geom_sf(data = buildings_sf, fill = "blue", color = "black") +
  theme_minimal() +
  labs(title = "Buildings in Outage")

buildings_sf <- st_transform(buildings_sf, 4326)
outages_sf <- st_transform(outages_sf, 4326)

leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%  # Positron basemap
  addPolygons(data = outages_sf, color = "red", fillColor = "red",
              fillOpacity = 0.3, weight = 1, group = "Outages") %>%
  addPolygons(data = buildings_sf, color = "blue", fillColor = "blue",
              fillOpacity = 0.7, weight = 1, group = "Buildings") %>%
  addLayersControl(
    overlayGroups = c("Outages", "Buildings"),
    options = layersControlOptions(collapsed = FALSE)
  ) %>%
  addLegend(colors = c("red", "blue"), labels = c("Outages", "Buildings in Outage"))

# Quick interactive map of buildings
buildings_map <- mapview(buildings_sf, col.regions = "blue", alpha = 0.7, legend = TRUE)
leaflet_buildings <- buildings_map@map %>%  # extract the Leaflet map
  #addProviderTiles(providers$CartoDB.Positron) %>%
  addPolygons(data = outages_sf, color = "red", fillColor = "red",
              fillOpacity = 0.3, weight = 1, group = "Outages") #%>%
  # addLayersControl(
  #   overlayGroups = c("Outages"),
  #   options = layersControlOptions(collapsed = FALSE)
  # )

leaflet_buildings
