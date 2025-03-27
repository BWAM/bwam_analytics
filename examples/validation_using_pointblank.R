#Load Dependencies -------------------------------------------------------
library(arrow)
library(dplyr)
library(pointblank)
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

dummy_df <- open_dataset(obt_result_dir) |>
  distinct(SITE_CODE, BASIN, WATERBODY_CODE, LOCATION) |>
  head() |>
  collect()


my_agent <- create_agent(
  tbl = dummy_df
) |>
  # Check values in a set. The set specifices the expected values.
  col_vals_in_set(
    columns = BASIN,
    set = c("01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
            "11", "12", "13", "14", "15", "16", "17")
  ) |>
  # Check string lenghts are less than or equal to max allowed in DB.
  col_vals_lte(
    columns = vars(nchar(SITE_CODE)),
    value = 1
  ) |>
  interrogate()


dummy_df |>
  tt_string_info()


my_agent


