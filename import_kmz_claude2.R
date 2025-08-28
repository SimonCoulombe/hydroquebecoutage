library(dplyr)
library(sf)
library(lubridate)
library(purrr)
library(furrr)
library(DBI)


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

# Extract timestamps from filenames
extract_ts <- function(path) {
  ts_str <- gsub("\\.kmz$", "", basename(path))
  ymd_hms(ts_str)
}


# Function to read KMZ files with better error handling
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
    
    # Check if the file is empty (no features)
    if (nrow(sf_obj) == 0) {
      cat("KMZ file", basename(path), "is empty (0 features). Skipping.\n")
      return(NULL)
    }
    
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



# Function to get files that need to be uploaded
get_missing_files <- function(files, con, table_name = "outages") {
  
  # Check if table exists
  table_exists <- DBI::dbExistsTable(con, table_name)
  
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


# Simplified upload function (no duplicate checking needed since we pre-filter)
upload_kmz_to_outages <- function(kmz_path, con, table_name = "outages", lookup) {
  
  # Check if table exists for append logic
  table_exists <- DBI::dbExistsTable(con, table_name)
  
  # Read the KMZ file
  cat("Reading", basename(kmz_path), "...\n")
  kmz_data <- read_kmz_manual(kmz_path)
  
  # Check if reading was successful or if file is empty
  if (is.null(kmz_data)) {
    cat("Skipping", basename(kmz_path), "(failed to read or empty file)\n")
    return(FALSE)
  }
  

  # append timestamp and duration
  kmz_data <- kmz_data %>% left_join(lookup)
  # Upload to PostGIS
  tryCatch({
    st_write(kmz_data, 
             con, 
             table_name, 
             append = table_exists,  
             row.names = FALSE)
    
    cat("Successfully uploaded", basename(kmz_path), "to", table_name, 
        "(", nrow(kmz_data), "features)\n")
    return(TRUE)  # Return TRUE to indicate successful upload
    
  }, error = function(e) {
    cat("Failed to upload", basename(kmz_path), "to database:", conditionMessage(e), "\n")
    return(FALSE)
  })
}



# Function to batch process multiple KMZ files
batch_upload_kmz <- function(kmz_paths, con, table_name = "outages", lookup) {
  
  results <- tibble(
    file_path = kmz_paths,
    uploaded = logical(length(kmz_paths)),
    timestamp = Sys.time()
  )
  
  for (i in seq_along(kmz_paths)) {
    cat("\nProcessing file", i, "of", length(kmz_paths), "\n")
    results$uploaded[i] <- upload_kmz_to_outages(kmz_paths[i], con, table_name, lookup)
  }
  
  # Summary
  uploaded_count <- sum(results$uploaded)
  skipped_count <- sum(!results$uploaded)
  
  cat("\n=== BATCH UPLOAD SUMMARY ===\n")
  cat("Files uploaded:", uploaded_count, "\n")
  cat("Files skipped:", skipped_count, "\n")
  cat("Total files processed:", length(kmz_paths), "\n")
  
  return(results)
}



# === MAIN SCRIPT ===

# Get and sort ALL KMZ files
all_files <- list.files("data", pattern = "\\.kmz$", full.names = TRUE)
all_files <- sort(all_files)

# Create lookup table with outage start/end times (only for new files)
timestamps <- extract_ts(all_files)
lookup <- tibble(
  file_path = all_files,
  outage_start = timestamps,
  outage_end = lead(timestamps)
) %>%
  mutate(outage_duration_sec  = as.numeric(outage_end - outage_start, units = "secs"))

# Show the lookup table
cat("Timestamp lookup for new files:\n")
print(lookup)

cat("Found", length(all_files), "KMZ files in data folder\n")



# Filter to only files that need to be uploaded
# dont upload if already in DB
# dont upload if last file because it wont get a duration..
all_files_without_last <- all_files[-length(all_files)]
files_to_upload <- get_missing_files(all_files_without_last, con, "outages")

files_to_upload <- files_to_upload[1:100]
# Only proceed if there are files to upload
if (length(files_to_upload) > 0) {
  
  cat("\n=== PROCESSING NEW FILES ===\n")
  

  
  # Upload the missing files
  results <- batch_upload_kmz(files_to_upload, con, "outages", lookup)
  
  # Show final results
  cat("\n=== FINAL RESULTS ===\n")
  print(results)
  
} else {
  cat("All files are already uploaded!\n")
}

#dbExecute(con, "drop table outages")
zz <- st_read(con, query = "SELECT * FROM outages") 
zz %>% glimpse()
#zz %>% distinct(source_file)
