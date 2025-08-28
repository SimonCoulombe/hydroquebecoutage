library(dplyr)
library(DBI)
library(RPostgres)
library(sf)
library(leaflet)
library(pool)
library(lubridate)
library(ggplot2)

# connect ----
con <- dbPool(
  RPostgres::Postgres(),
  host = "192.168.2.15",
  dbname = Sys.getenv("POSTGIS_DBNAME"),
  port = 5432,
  user = Sys.getenv("POSTGIS_USER"),
  password = Sys.getenv("POSTGIS_PASSWORD")
)


# list tables and schemas ----
dbListTables(con)

schemas <- dbGetQuery(con, "
  SELECT schema_name
  FROM information_schema.schemata
  ORDER BY schema_name;
")

tables <- dbGetQuery(con, "
  SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_type = 'BASE TABLE'
  ORDER BY table_schema, table_name;
")

print(tables)

# Demo data ----
## combien de lignes dans chacun?   ----
dbGetQuery(con, "SELECT count(*) as count  FROM outages ;")
# 2 641 483


dbGetQuery(con, "SELECT count(*) as count  FROM batiments_4326 ;")
# 3 289 166

## what does le data look like ----

outages <- st_read(con, query = "SELECT * FROM outages limit 50")
outages
leaflet(outages) %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(
    color = "red",
    weight = 1
  )


batiments <- st_read(con, query = "SELECT * FROM batiments_4326 limit 50")
leaflet(batiments %>% head(1)) %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(
    color = "red",
    weight = 1
  )

# ajouter une colonne point on surface aux batiments et l'indéxer ----

# letS create a point on surface for buildings
dbExecute(con, "
ALTER TABLE batiments_4326
ADD COLUMN geom_inside geometry(Point, 4326);
")

dbExecute(con, "
UPDATE batiments_4326
SET geom_inside = ST_PointOnSurface(geom);
")

dbExecute(con, "
CREATE INDEX IF NOT EXISTS batiments_inside_idx
ON batiments_4326 USING GIST(geom_inside);
")

# creates index pour les 2 autres colonnes geographiques  ----
dbExecute(con, "
  CREATE INDEX IF NOT EXISTS batiments_geom_idx
  ON batiments_4326
  USING GIST(geom);
")

dbExecute(con, "
  CREATE INDEX IF NOT EXISTS outages_geom_idx
  ON outages
  USING GIST(geom);
")

dbExecute(con, "
  CREATE INDEX IF NOT EXISTS outages_start_idx
  ON outages(outage_start DESC);
")


# create the table with points inside outages (cheaper) ----

# Drop the table if it exists
dbExecute(con, "DROP TABLE IF EXISTS points_in_outages;")


tictoc::tic()
dbExecute(con, "
  CREATE TABLE points_in_outages AS
SELECT b.id AS building_id,
o.source_file, o.row_number, 
o.outage_duration_sec
FROM batiments_4326 AS b
JOIN outages AS o
ON ST_Contains(o.geom, b.geom_inside);
")
tictoc::toc() # 2 minutes

tictoc::tic()
dbExecute(con, "
  CREATE TABLE points_in_outages_date AS
  SELECT DISTINCT
      building_id,
      CAST(LEFT(regexp_replace(split_part(source_file, '/', -1), '\\.kmz$', ''), 8) AS date) AS outage_date
  FROM points_in_outages;
")
tictoc::toc()

# créer building_outages_monthly, qui donne le nombre d'outages à chaque mois
tictoc::tic()
dbExecute(con, "
CREATE TABLE building_outages_monthly AS
SELECT
building_id,
DATE_TRUNC('month', outage_date)::date AS month,
COUNT(*) AS outages_count
FROM points_in_outages_date
GROUP BY building_id, DATE_TRUNC('month', outage_date)
ORDER BY building_id, month;
")

tictoc::toc()
#18 342 319
# 57.64 sec elapsed


# sortir les polygones des 10 batiments du top 10 ----
# il faut aussi définir quelle géométrie sf doit utiliser, ainsi que le crs, qui est somehow perdu en chemin
tictoc::tic()
top10_buildings <- st_read(con, query = "
            SELECT b.*
FROM batiments_4326 AS b
JOIN (
    SELECT
        building_id
    FROM building_outages_monthly
    GROUP BY building_id
    ORDER BY SUM(outages_count) DESC
    LIMIT 10
) top10
ON b.id = top10.building_id;")
tictoc::toc()  


top10_buildings_polygons <- st_set_geometry(top10_buildings, "geom")
st_crs(top10_buildings_polygons) <- 4326
mapview::mapview(top10_buildings_polygons)

#data outages pour plot du nombre de pannes à chaque mois du top10 ----
top10_plot_data <- dbGetQuery(con, 
                              "SELECT bom.*
                                FROM building_outages_monthly AS bom
                              JOIN (
                                SELECT building_id
                                FROM building_outages_monthly
                                GROUP BY building_id
                                ORDER BY SUM(outages_count) DESC
                                LIMIT 10
                              ) top10
                              ON bom.building_id = top10.building_id
                              ORDER BY bom.building_id, bom.month;")



# plot lac des seize îles

top10_plot_data %>%  
  #filter(building_id == 429738) %>%
  filter(building_id == 1565816) %>% 
  mutate(building_id = as.factor(building_id)) %>%  
  ggplot(aes(x= month, y = outages_count)) +
  geom_col() + 
  scale_y_continuous(breaks = scales::pretty_breaks(n = 10)) +  
  theme_minimal() + 
  labs(x = NULL, 
       y = "Nombre de jours dans le mois avec au moins 1 panne" ,
       title = "Le 202 Côté Estate du Lac à côté du Lac des Seize Îles\n est le bâtiment avec le plus de pannes depuis 2022.", 
       caption = "Données: Info-Panne Hydro-Québec, Référentiel Bâti, Scraping et code: Simon Coulombe") 


  


