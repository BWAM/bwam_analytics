
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

# Query -------------------------------------------------------------------

parameters <- open_dataset(obt_result_dir) |>
  select(PARAMETER_NAME, FRACTION, METHOD_SPECIATION, UNIT, SAMPLE_SOURCE) |>
  distinct() |>
  collect()

final_df <- open_dataset(obt_result_dir) |>
  filter(
    WATERBODY_CODE %in% c("UHUD", "LHUD", "MOHK"),
    PARAMETER_NAME %in% "bioassessment_profile"
  ) |>
  select(
    WIPWL, WATERBODY_NAME, WATERBODY_TYPE, EVENT_ID, EVENT_DATETIME,
    LATITUDE, LONGITUDE,
    SAMPLE_LOCATION,
    SAMPLE_TYPE, SAMPLE_ORGANIZATION,
    SAMPLE_METHOD,
    PARAMETER_NAME, RESULT_VALUE, UNIT,
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

