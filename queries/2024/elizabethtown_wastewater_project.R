# Request for monitoring data related to the Elizabethtown Wastewater Project
# Request made on 9/13/2024
# Request made by Thorsland, Derek via Brian Duffy

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
  "elizabethtown_wastewater_project"
)
# Query -------------------------------------------------------------------


parameters <- open_dataset(obt_result_dir) |>
  select(PARAMETER_NAME, FRACTION, METHOD_SPECIATION, UNIT, SAMPLE_SOURCE) |>
  distinct() |>
  collect()

final_df <- open_dataset(obt_result_dir) |>
  filter(
    WATERBODY_TYPE %in% "river_stream",
    SITE_CODE %in% c("10-BRNC-0.4",
                     "10-BOQT-28.5",
                     "10-BOQT-28.6")
  ) |>
  select(
    WIPWL, WATERBODY_NAME, WATERBODY_TYPE, EVENT_ID, EVENT_DATETIME,
    SITE_CODE,
    LATITUDE, LONGITUDE,
    BASIN, BASIN_NAME,
    COUNTY,
    SAMPLE_LOCATION,
    SAMPLE_TYPE, SAMPLE_ORGANIZATION,
    FRACTION, PARAMETER_NAME, METHOD_SPECIATION, RESULT_CATEGORY,
    RESULT_VALUE, UNIT,
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
                                    "elizabethtown_wastewater_project.xlsx"))




# plots -------------------------------------------------------------------

# library(ggplot2)
#
# sites_vec <- unique(final_df$SITE_CODE)
# param_vec <- final_df |>
#   filter(SAMPLE_TYPE %in% "chemistry") |>
#   pull(PARAMETER_NAME) |>
#   unique()
#
# site_params <- tidyr::expand_grid(sites_vec, param_vec) |>
#   mutate(id = paste0(sites_vec, "_", param_vec))
#
# test <- site_params$id |>
#   # head() |>
#   purrr::set_names() |>
#   purrr::map(
#   function(.x) {
#     site_params_sub <- site_params |>
#       dplyr::filter(id == .x)
#
#     sub_df <- final_df |>
#       dplyr::filter(
#         SITE_CODE %in% site_params_sub$sites_vec,
#         PARAMETER_NAME %in% site_params_sub$param_vec
#       )
#
#     if (nrow(sub_df) == 0) return(NULL)
#
#     ggplot(sub_df, aes(x = EVENT_DATETIME,
#                          y = RESULT_VALUE,
#                          color = SITE_CODE)) +
#       geom_point() +
#       ylab(site_params_sub$param_vec) +
#       theme_classic()
#   }
# ) |>
#   purrr::discard(~is.null(.x))
#
# test$`10-BRNC-0.4_nitrate`
