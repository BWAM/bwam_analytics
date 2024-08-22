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

# Write XLSX --------------------------------------------------------------
openxlsx2::write_xlsx(
  x = final_df,
  file = file.path(export_dir,
                   "nysdec_ephem-trichop_spp-occurance.xlsx"),
  as_table = TRUE,
  overwrite = TRUE,
  table_style = "TableStyleMedium2"
)

