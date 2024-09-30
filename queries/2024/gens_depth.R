library(dplyr)
library(arrow)

# One Big Table Directory -------------------------------------------------
obt_dir <- file.path("L:",
                     "DOW",
                     "BWAM Share",
                     "data",
                     "dev",
                     "parquet",
                     "obt_result.parquet")

params <- open_dataset(obt_dir) |>
  distinct(PARAMETER_NAME) |>
  collect()

all_df <- open_dataset(obt_dir) |>
  filter(WATERBODY_CODE %in% c("GENS", "GENS_BE", "GENS_BW", "UGNS"),
         PARAMETER_NAME %in% "stream_depth") |>
  distinct(WATERBODY_CODE, WATERBODY_NAME, SITE_CODE, LATITUDE, LONGITUDE, EVENT_ID, RESULT_ID, PARAMETER_NAME, RESULT_VALUE, UNIT) |>
  mutate(
    depth = RESULT_VALUE * 3.2808399,
    depth_units = "Feet"
  ) |>
  collect()


all_df <- open_dataset(obt_dir) |>
  distinct(REPLICATE) |>
  collect()
