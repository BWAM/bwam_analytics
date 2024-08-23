# Request for Mayfly and Caddisfly Species Occurances
# Request made on 7/30/2024
# Request made by Erin White (New York Natural Heritage Program)
# Email Subject: Northeastern mayfly and caddisfly conservation status ranking

# Load Dependencies -------------------------------------------------------
library(arrow)
library(dplyr)

# Directories -------------------------------------------------------------
obt_taxa_dir <- file.path(
  "L:",
  "DOW",
  "BWAM Share",
  "data",
  "parquet",
  "analytical_table_store",
  "obt_taxa_abundance.parquet"
)


export_dir <- file.path(
  "L:",
  "DOW",
  "BWAM Share",
  "data",
  "projects",
  "2024",
  "mayfly_caddisfly_occurance"
)
# Query -------------------------------------------------------------------

final_df <- open_dataset(obt_taxa_dir) |>
  filter(
    T_ORDER %in% c("ephemeroptera", "trichoptera"),
    !T_SPECIES %in% "not_applicable"
  ) |>
  select(
    EVENT_ID,EVENT_DATETIME,
    # REPLICATE,
    WATERBODY_TYPE, WATERBODY_NAME,
    LATITUDE, LONGITUDE,
    SAMPLE_ORGANIZATION,
    SAMPLE_METHOD,SAMPLE_METHOD_DESCRIPTION,
    T_ORDER, T_SPECIES,
    # RESULT_VALUE, SUBSAMPLE_AMOUNT, SUBSAMPLE_AMOUNT_UNIT
  ) |>
  distinct() |>
  collect()


# Data Dictionary ---------------------------------------------------------
dictionary <- nexus::columns_dictionary |>
  filter(column_name %in% names(final_df),
         !grepl("surrogate", definition)) |>
  select(column_name, definition) |>
  distinct() |>
  arrange(names(final_df))


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
                                    "nysdec_ephem-trichop_spp-occurance.xlsx"))


