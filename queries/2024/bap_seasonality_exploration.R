
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
  summarize(RESULT_VALUE = mean(RESULT_VALUE),
            .by = c(WATERBODY_TYPE, EVENT_ID, EVENT_DATETIME, SAMPLE_METHOD, PARAMETER_NAME)) |>
  collect() |>
  mutate(MONTH = lubridate::month(EVENT_DATETIME, label = TRUE))

library(ggplot2)

ggplot(final_df, aes(MONTH, RESULT_VALUE)) +
  geom_boxplot() +
  facet_wrap(~ SAMPLE_METHOD)
