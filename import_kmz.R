library(dplyr)
library(sf)
library(zip)
library(mapview)
library(lubridate)
library(purrr)
library(furrr)
plan(multisession, workers = parallel::detectCores() - 1)  # use all but one core
# fonction pour aller chercher tous les noms de fichiers et trouver le nom du suivant

files <- list.files("data", pattern = "\\.kmz$", full.names = TRUE)
files <- sort(files)

extract_ts <- function(path) {
  ts_str <- gsub("\\.kmz$", "", basename(path))
  ymd_hms(ts_str)
}

timestamps <- extract_ts(files)

lookup <- tibble(
  path = files,
  outage_start = timestamps,
  outage_end   = lead(timestamps)
)


read_kmz_safe <- function(path) {
  tryCatch({
    tmpdir <- tempdir()
    unzip(path, exdir = tmpdir, junkpaths = TRUE)
    
    kml_file <- list.files(tmpdir, pattern = "\\.kml$", full.names = TRUE)[1]
    if (is.na(kml_file)) stop("No KML found in KMZ")
    
    sf_obj <- sf::st_read(kml_file, quiet = TRUE)
    sf_obj <- sf::st_zm(sf_obj, drop = TRUE, what = "ZM")
    sf_obj$path <- path
    sf_obj
  }, error = function(e) {
    message("Failed to read ", path, ": ", e$message)
    return(NULL)
  })
}
#kmz <- read_kmz("data/20250824010018.kmz") 

## read polygons and join with lookup
polygons <- future_map_dfr(files, read_kmz_safe, .options = furrr_options(seed = TRUE))

# join start/end info
polygons <- polygons %>%
  left_join(lookup, by = "path")

