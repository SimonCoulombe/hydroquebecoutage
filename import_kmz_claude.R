library(dplyr)
library(sf)
library(lubridate)
library(purrr)
library(furrr)
library(DBI)
library(RPostgres)
library(sf)

con <- dbConnect(
  RPostgres::Postgres(),
  host = "192.168.2.15",
  dbname = Sys.getenv("POSTGIS_DBNAME"),
  port = 5432,
  user = Sys.getenv("POSTGIS_USER"),
  password = Sys.getenv("POSTGIS_PASSWORD")
)

# Set up parallel processing
plan(multisession, workers = parallel::detectCores() - 1)

# Get and sort KMZ files
files <- list.files("data", pattern = "\\.kmz$", full.names = TRUE)
files <- sort(files)


# Function to get files that need to be uploaded
get_missing_files <- function(files, con, table_name = "my_schema.outages") {
  
  # Parse schema and table name
  schema_table <- strsplit(table_name, "\\.")[[1]]
  if (length(schema_table) == 2) {
    schema_name <- schema_table[1]
    table_only <- schema_table[2]
  } else {
    schema_name <- "public"  # default schema
    table_only <- table_name
  }
  
  # Check if table exists using SQL query
  table_exists_query <- paste0("
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_schema = '", schema_name, "' 
      AND table_name = '", table_only, "'
    );")
  
  table_exists <- DBI::dbGetQuery(con, table_exists_query)[[1]]
  
  if (!table_exists) {
    cat("Table", table_name, "doesn't exist. All", length(files), "files will be uploaded.\n")
    return(files)
  }
  
  
  # Get existing file paths from the database
  existing_paths_query <- paste0("SELECT DISTINCT file_path FROM ", table_name)
  existing_paths <- DBI::dbGetQuery(con, existing_paths_query)$file_path
  
  # Filter out files that already exist
  missing_files <- files[!files %in% existing_paths]
  
  cat("Found", length(existing_paths), "files already in database\n")
  cat("Found", length(missing_files), "new files to upload\n")
  cat("Total files available:", length(files), "\n")
  
  return(missing_files)
}
missing_files <- get_missing_files(files, con ,"my_schema.outages")

# # Extract timestamps from filenames
# extract_ts <- function(path) {
#   ts_str <- gsub("\\.kmz$", "", basename(path))
#   ymd_hms(ts_str)
# }
# 
# # Create lookup table with outage start/end times
# timestamps <- extract_ts(files)
# lookup <- tibble(
#   path = files,
#   outage_start = timestamps,
#   outage_end = lead(timestamps)
# )



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
    sf_obj$row_number <- 1:nrow(sf_obj)
    
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


# Simplified upload function (no duplicate checking needed since we pre-filter)
upload_kmz_to_outages <- function(kmz_path, con, table_name = "my_schema.outages") {
  
  # Read the KMZ file
  cat("Reading", basename(kmz_path), "...\n")
  kmz_data <- read_kmz_manual(kmz_path)
  
  # Check if reading was successful
  if (is.null(kmz_data)) {
    cat("Failed to read", basename(kmz_path), ". Skipping.\n")
    return(FALSE)
  }
  
  # Check if table exists for append logic
  table_exists <- DBI::dbExistsTable(con, table_name)
  
  # Upload to PostGIS
  tryCatch({
    # Use st_write with separate layer and schema specification
    if (length(schema_table) == 2) {
      st_write(kmz_data, 
               con, 
               layer = table_only,
               schema = schema_name,
               append = table_exists,  
               row.names = FALSE)
    } else {
      st_write(kmz_data, 
               con, 
               table_name, 
               append = table_exists,  
               row.names = FALSE)
    }
    
    cat("Successfully uploaded", basename(kmz_path), "to", table_name, 
        "(", nrow(kmz_data), "features)\n")
    return(TRUE)  # Return TRUE to indicate successful upload
    
  }, error = function(e) {
    cat("Failed to upload", basename(kmz_path), "to database:", conditionMessage(e), "\n")
    return(FALSE)
  })
}


# Process files with progress tracking
cat("Processing", length(files), "KMZ files using manual extraction...\n")

# Use the reliable manual extraction method
polygons <- future_map(files[1:100], read_kmz_manual, 
                       .options = furrr_options(seed = TRUE),
                       .progress = TRUE)


# Remove NULL results and show summary
polygons_all <- polygons[!sapply(polygons, is.null)]  # Remove any remaining NULLs

if (length(polygons_all) > 0) {
  # Convert list to single sf object
  polygons <- do.call(rbind, polygons_all)
  
  # Separate files with data from empty ones
  files_with_data <- polygons[polygons$has_data == TRUE, ]
  empty_files <- polygons[polygons$has_data == FALSE, ]
  
  cat("Processing Summary:\n")
  cat("- Total files processed:", length(files), "\n")
  cat("- Files with polygon data:", nrow(files_with_data), "\n") 
  cat("- Empty files (no outages):", nrow(empty_files), "\n")
  cat("- Total polygons:", nrow(files_with_data), "\n")
  
  # Show some empty file examples (for verification)
  if (nrow(empty_files) > 0) {
    cat("\nExample empty files:\n")
    print(head(empty_files$source_file, 10))
  }
  
  # Join with lookup table - keep all records including empty ones
  polygons_with_timing <- polygons %>%
    left_join(lookup, by = c("file_path" = "path"))
  
  # If you only want files with actual polygon data:
  # polygons_data_only <- files_with_data %>%
  #   left_join(lookup, by = c("file_path" = "path"))
  
} else {
  cat("No files were successfully processed\n")
}

# Clean up parallel processing
plan(sequential)