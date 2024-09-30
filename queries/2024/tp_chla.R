# Request for TP, CHLA, and Phyco data
# Request made on 8/19/2024
# Request made by Sarah Rickard and Lew McCaffrey

# Load Dependencies -------------------------------------------------------
library(arrow)
library(dplyr)

# Directories -------------------------------------------------------------
obt_result_dir <- file.path(
  "L:",
  "DOW",
  "BWAM Share",
  "data",
  "parquet",
  "analytical_table_store",
  "obt_result.parquet"
)


export_dir <- file.path(
  "L:",
  "DOW",
  "BWAM Share",
  "data",
  "projects",
  "2024",
  "satellite_chla"
)
# Query -------------------------------------------------------------------


parameters <- open_dataset(obt_result_dir) |>
  select(PARAMETER_NAME, FRACTION, METHOD_SPECIATION, UNIT, SAMPLE_SOURCE) |>
  distinct() |>
  collect()

final_df <- open_dataset(obt_result_dir) |>
  filter(
    WATERBODY_TYPE %in% "lake",
    EVENT_DATETIME > as.Date("2019-01-01"),
    (PARAMETER_NAME %in% "phosphorus" & FRACTION %in% "total") |
    (PARAMETER_NAME %in% c("chlorophyll-a", "chlorophyll_a") & UNIT %in% "ug/L") |
      PARAMETER_NAME %in% "chlorophyll-a-concentration_cyanobacteria_bluegreen"
  ) |>
  select(
    WIPWL, WATERBODY_NAME, WATERBODY_TYPE, EVENT_ID, EVENT_DATETIME,
    LATITUDE, LONGITUDE,
    SAMPLE_LOCATION,
    SAMPLE_TYPE, SAMPLE_ORGANIZATION, SAMPLE_DEPTH_METERS,
    FRACTION, PARAMETER_NAME, METHOD_SPECIATION, RESULT_VALUE, UNIT,
    RESULT_QUALIFIER, RESULT_QUALIFIER_DESCRIPTION,
    METHOD_DETECTION_LIMIT,
    QUANTITATION_LIMIT,
    REPORTING_DETECTION_LIMIT
  ) |>
  distinct() |>
  collect()


# Write XLSX --------------------------------------------------------------
openxlsx2::write_xlsx(
  x = final_df,
  file = file.path(export_dir,
                   "tp_chla_phyco.xlsx"),
  as_table = TRUE,
  overwrite = TRUE
)

