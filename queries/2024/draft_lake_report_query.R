#Load Dependencies -------------------------------------------------------
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

# Query -------------------------------------------------------------------

final_df <- open_dataset(obt_result_dir) |>
  dplyr::filter(
    WATERBODY_TYPE %in% "lake"
  ) |>
  dplyr::select(
    LAKE_HISTORY_ID = WATERBODY_CODE,
    LAKE_WATERBODY_NAME = WATERBODY_NAME,
    PUBLIC_WATER_SUPPLY,
    WATERBODY_TYPE,
    SITE_ID,
    EVENT_ID,
    SAMPLE_DATETIME = EVENT_DATETIME,
    LAKE_PROJ_CODE = PROJECT,
    PARAMETER_ID,
    RSLT_RESULT_SAMPLE_FRACTION = FRACTION,
    CHARACTERISTIC_NAME = PARAMETER_NAME,
    RSLT_RESULT_UNIT = UNIT,
    LAKE_HISTORY_ID = WATERBODY_CODE,
    LOCATION_PWL_ID = WIPWL,
    LOCATION_HISTORY_ID = SITE_CODE,
    MUNICIPALITY,
    LOCATION_COUNTY = COUNTY,
    LOCATION_NAME = LOCATION,
    LOCATION_X_COORDINATE = LATITUDE,
    LOCATION_Y_COORDINATE = LONGITUDE,
    LAKE_CLASSIFICATION = WATERBODY_CLASS,
    MAJOR_BASIN_ID = BASIN,
    SAMPLE_TYPE,
    SAMPLE_NAME,
    INFORMATION_TYPE = SAMPLE_LOCATION,
    DATA_PROVIDER = SAMPLE_ORGANIZATION,
    RSLT_PROFIE_DEPTH = SAMPLE_DEPTH_METERS,
    HFD_SAMPLE_LAT = SAMPLE_LATITUDE,
    HFD_SAMPLE_LONG = SAMPLE_LONGITUDE,
    ANALYSIS_DATETIME,
    RSLT_RESULT_VALUE = RESULT_VALUE,
    RSLT_VALIDATOR_QUALIFIER = RESULT_QUALIFIER
  ) |>
  dplyr::collect()
