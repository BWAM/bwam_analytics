# Request for TP and CHLA data
# Request made on 8/19/2024
# Request made by Sarah Rickard

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


parameters <- open_dataset(obt_result_dir) |>
  select(PARAMETER_NAME, FRACTION, METHOD_SPECIATION, UNIT, SAMPLE_SOURCE) |>
  distinct() |>
  collect()

final_df <- open_dataset(obt_result_dir) |>
  filter(
    WATERBODY_TYPE %in% "lake",
    EVENT_DATETIME > as.Date("1900-01-01"),
    PARAMETER_NAME %in% c("site_sound_depth", "max_sound_depth"),
    is.na(RESULT_VALUE)) |>
  select(
    WIPWL, WATERBODY_TYPE, EVENT_ID, EVENT_DATETIME,
    LATITUDE, LONGITUDE,
    SAMPLE_LOCATION,
    SAMPLE_TYPE, SAMPLE_ORGANIZATION, SAMPLE_DEPTH_METERS,
    FRACTION, PARAMETER_NAME, METHOD_SPECIATION, RESULT_VALUE, UNIT
  ) |>
  distinct() |>
  collect()


event_id_vec <- unique(final_df$EVENT_ID)

