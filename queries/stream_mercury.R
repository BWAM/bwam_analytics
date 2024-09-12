# Request for Stream Mercury Data
# Request made on 9/11/2024
# Request made by Ethan Sullivan

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
  "stream_mercury"
)
# Query -------------------------------------------------------------------


parameters <- open_dataset(obt_result_dir) |>
  select(PARAMETER_NAME, FRACTION, METHOD_SPECIATION, UNIT, SAMPLE_SOURCE) |>
  distinct() |>
  collect()

final_df <- open_dataset(obt_result_dir) |>
  filter(
    WATERBODY_TYPE %in% "river_stream",
    EVENT_DATETIME > as.Date("2000-01-01"),
    PARAMETER_NAME %in% "mercury"
  ) |>
  select(
    WIPWL, WATERBODY_NAME, WATERBODY_TYPE, EVENT_ID, EVENT_DATETIME,
    LATITUDE, LONGITUDE,
    BASIN, BASIN_NAME,
    COUNTY,
    SAMPLE_LOCATION,
    SAMPLE_TYPE, SAMPLE_ORGANIZATION,
    FRACTION, PARAMETER_NAME, METHOD_SPECIATION, RESULT_VALUE, UNIT,
    RESULT_QUALIFIER, RESULT_QUALIFIER_DESCRIPTION,
    METHOD_DETECTION_LIMIT,
    QUANTITATION_LIMIT,
    REPORTING_DETECTION_LIMIT
  ) |>
  distinct() |>
  collect()

# Data Dictionary ---------------------------------------------------------
dictionary <- nexus:::admin_columns_dictionary |>
  dplyr::filter(column_name %in% names(final_df),
         !grepl("surrogate", definition),
         parent_table %in% "NONE",
         !is.na(definition)) |>
  dplyr::select(column_name, definition) |>
  dplyr::distinct() |>
  dplyr::arrange(column_name)



# Create a workbook -------------------------------------------------------

wb <- openxlsx2::wb_workbook() |>
  openxlsx2::wb_add_worksheet(
    sheet = "Data"
  ) |>
  openxlsx2::wb_add_data_table(
    x = final_df,
    table_style = "TableStyleMedium2"
  ) |>
  openxlsx2::wb_add_worksheet(
    sheet = "Dictionary"
  ) |>
  openxlsx2::wb_add_data_table(
    x = dictionary,
    table_style = "TableStyleMedium2"
  )

# Export to XLSX ----------------------------------------------------------
openxlsx2::wb_save(wb = wb,
                   file = file.path(export_dir,
                                    "stream_mercury.xlsx"))



