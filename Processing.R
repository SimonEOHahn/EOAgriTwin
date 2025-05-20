# Load required packages
library(dplyr)
library(readr)
library(lubridate)

# Set working directory
setwd("D:/EOAgriTwin/Evapotranspiration")

############Process ICOS Reference data#################

# Define latent heat of vaporization function
latent_heat_vaporization <- function(temp) {
  2.501e6 - 2370 * temp  # J/kg
}

# Define data directory
data_dir <- "D:/EOAgriTwin/Evapotranspiration/Raw/Reference/ICOS/Relevant/"
output_dir <- "D:/EOAgriTwin/Evapotranspiration/Processed/Reference/ICOSETC/Output/"

# Function to process a single ICOS reference station
process_icos_station <- function(station) {
  # Possible filenames
  file_patterns <- c(
    paste0("ICOSETC_DE-", station, "_FLUXNET_DD_INTERIM_L2.csv"),
    paste0("ICOSETC_DE-", station, "_FLUXNET_DD_L2.csv")
  )
  
  # Try to find the correct input file
  input_file <- NULL
  for (pattern in file_patterns) {
    candidate <- file.path(data_dir, pattern)
    if (file.exists(candidate)) {
      input_file <- candidate
      break
    }
  }
  
  # If no file found, skip
  if (is.null(input_file)) {
    message(paste("Skipping", station, "- no matching input file found."))
    return(NULL)
  }
  
  # Read the data
  data <- read_csv(input_file)
  
  # Replace -9999 with NA
  data[data == -9999] <- NA
  
  # Select relevant columns and compute ETa
  processed_data <- data %>%
    select(TIMESTAMP, LE_CORR, TA_F) %>%
    mutate(
      TIMESTAMP = ymd(as.character(TIMESTAMP)),
      lambda = latent_heat_vaporization(TA_F),
      ETa = (LE_CORR / lambda) * 86400  # mm/day
    )
  
  # Ensure output directory exists
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  # Write the processed file
  output_file <- paste0(output_dir, station, "_data.csv")
  write_csv(processed_data, output_file)
  message(paste("Processed:", station))
}

# List of stations (update this with your real station codes)
stations <- c("Geb", "Gri", "Kli", "RuS", "RuR")  

# Run the processing function for each station
for (station in stations) {
  process_icos_station(station)
}

############Combine ICOS Reference and ECOSTRESS data#################

# Define function to merge and process data for one station
process_station_data <- function(station) {
  # Construct file paths
  ref_path <- paste0("Processed/Reference/ICOSETC/Output/", station, "_data.csv")
  eco_path <- paste0("Processed/RS_Products/ECOSTRESS/DE-", station, "_ETdaily_2018-2022.csv")
  output_path <- paste0("Processed/Reference/Merged/", station, "_merged.csv")
  
  # Check if both files exist
  if (!file.exists(ref_path) || !file.exists(eco_path)) {
    message(paste("Skipping", station, "- one or both files not found."))
    return(NULL)
  }
  
  # Load datasets
  ref_data <- read_csv(ref_path)
  eco_data <- read_csv(eco_path)
  
  # Replace -9999 with NA
  ref_data[ref_data == -9999] <- NA

  # Format dates
  ref_data <- ref_data %>%
    mutate(TIMESTAMP = ymd(as.character(TIMESTAMP)))
  
  eco_data <- eco_data %>%
    mutate(Date = as.Date(Date))
  
  # Merge datasets
  merged_data <- ref_data %>%
    left_join(eco_data %>% select(Date, ETdaily), by = c("TIMESTAMP" = "Date")) %>%
    mutate(
      ETa_ECO = (ETdaily / lambda) * 86400  # mm/s to mm/day
    ) %>%
    rename(LE_ECO = ETdaily)
  
  # Save merged data
  write_csv(merged_data, output_path)
  message(paste("Processed:", station))
}

# List of station codes (change/add your actual station names here)
stations <- c("Geb", "Gri", "Kli", "RuR", "RuS") 

# Process each station
for (station in stations) {
  process_station_data(station)
}

