library(DBI)
library(RPostgres)
library(sf)
library(leaflet)
con <- dbConnect(
  RPostgres::Postgres(),
  host = "192.168.2.15",
  dbname = "postgres",
  port = 5432,
  user = "postgres",
  password = "postgres"
)

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


# Get and sort KMZ files
files <- list.files("data", pattern = "\\.kmz$", full.names = TRUE)
files <- sort(files)


# Alternative function using manual zip extraction with better error handling
read_kmz_manual <- function(path) {
  tryCatch({
    # Create unique temporary directory for each file
    tmpdir <- file.path(tempdir(), basename(path))
    dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE)
    
    # Use utils::unzip instead of zip package - more robust
    utils::unzip(path, exdir = tmpdir, junkpaths = TRUE)
    
    # Find KML file
    kml_files <- list.files(tmpdir, pattern = "\\.kml$", full.names = TRUE)
    if (length(kml_files) == 0) {
      stop("No KML file found in KMZ archive")
    }
    
    # Read the first KML file
    sf_obj <- sf::st_read(kml_files[1], quiet = TRUE)
    
    # Clean up temporary directory
    unlink(tmpdir, recursive = TRUE)
    
    # Remove Z/M dimensions unconditionally
    sf_obj <- sf::st_zm(sf_obj, drop = TRUE, what = "ZM")
    
    # Add metadata
    sf_obj$source_file <- basename(path)
    sf_obj$file_path <- path
    
    return(sf_obj)
    
  }, error = function(e) {
    cat("Failed to read", basename(path), ":", conditionMessage(e), "\n")
    # Clean up on error
    if (exists("tmpdir") && dir.exists(tmpdir)) {
      unlink(tmpdir, recursive = TRUE)
    }
    return(NULL)
  })
}

# demo upload outage
kmz <- read_kmz_manual(files[[1]])

st_write(
  kmz,
  dsn = con,
  layer = "outages",    # table name
  driver = "PostgreSQL",
  append = FALSE         # set FALSE the very first time to create the table
)

# demo read outage
outages <- st_read(con, query = "SELECT * FROM outages")



leaflet(outages) %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(
    color = "red",
    weight = 1
  )


# demo read batiments


batiments <- st_read(con, query = "SELECT * FROM batiments_4326 limit 50")


leaflet(batiments %>% head(1)) %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(
    color = "red",
    weight = 1
  )

