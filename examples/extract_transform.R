
# Packages ----------------------------------------------------------------
library(arrow)
library(tidyverse)

# Data Directory ----------------------------------------------------------
stream_data_dir <- file.path(
  "L:",
  "DOW",
  "BWAM Share",
  "data",
  "data_warehouse_staging",
  "2024_V1",
  "raw_data",
  "stream"
)

# Extract -----------------------------------------------------------------

sei_raw <- read_csv(
  file = file.path(
    stream_data_dir,
    "master_S_SAMPLE_EVENT_INFO_2023_12_26.csv"
  ),
  na = c("", "NA", "N/A", "n/a", "-9999"),
  col_types = cols(
    event_id = col_character(),
    site_id = col_character(),
    # sample_date = col_date(),
    sample_date = col_date(format = "%m/%d/%Y"),
    arrive_time = col_time(),
    sampling_crew = col_character(),
    sampling_type_department_interest = col_logical(),
    sampling_type_external_suggestion = col_logical(),
    sampling_type_long_term_trend = col_logical(),
    sampling_type_multiplate = col_logical(),
    sampling_type_probability_survey = col_logical(),
    sampling_type_rapid_assessment_survey = col_logical(),
    sampling_type_reference = col_logical(),
    sampling_type_rmn = col_logical(),
    sampling_type_special_survey = col_logical(),
    sampling_type_unassessed_wipwl = col_logical(),
    flow_condition_sampling_type = col_character(),
    flow_condition_category = col_character(),
    flow_condition_numeric = col_double(),
    discharge_collected = col_logical(),
    discharge_csf = col_double(),
    biosample_collected = col_logical(),
    biosample_type = col_character(),
    chemistry_collected = col_logical(),
    chemistry_qc_collected = col_logical(),
    chemistry_qc_type = col_character(),
    chemistry_sampling_location = col_character(),
    chemistry_sampling_equipment = col_character(),
    chemistry_suite = col_character(),
    chemistry_bottle_label = col_character(),
    chemistry_bottle_label_msmsd = col_character(),
    chemistry_bottle_label_seq = col_character(),
    chemistry_bottle_label_eb = col_character(),
    cec_suite = col_character(),
    microtox_collected = col_logical(),
    organics_for_toxics_collected = col_logical(),
    depth_meters = col_double(),
    depth_qualifer = col_character(),
    width_meters = col_double(),
    width_qualifer = col_character(),
    velocity_cm_per_second = col_double(),
    velocity_qualifier = col_character(),
    secchi_meters = col_double(),
    secchi_qualifier = col_character(),
    diatom_percent = col_double(),
    diatom_thickness = col_double(),
    algae_suspended_present = col_logical(),
    algae_filamentous_present = col_logical(),
    macrophyte_percent = col_double(),
    canopy_percent = col_double(),
    embeddedness_percent = col_double(),
    embeddedness_qualifier = col_character(),
    substrate_rock_percent = col_double(),
    substrate_rubble_percent = col_double(),
    substrate_gravel_percent = col_double(),
    substrate_sand_percent = col_double(),
    substrate_silt_percent = col_double(),
    substrate_clay_percent = col_double(),
    dominant_observable_land_use = col_character(),
    event_comment = col_character()
  )
)

# Transform ---------------------------------------------------------------

sei_final <- sei_raw |>
  select(-correct_event_id,
         -correct_site_id,
         -site_id) |>
  rename(
    width_qualifier = width_qualifer,
    depth_qualifier = depth_qualifer
  ) |>

  rename(discharge_cfs = discharge_csf,
         sampling_type_special_study = sampling_type_special_survey,
         sample_crew = sampling_crew
  ) |>
  dplyr::mutate(
    arrive_time = as.character(glue::glue("{sample_date} {arrive_time}"))
  ) |>
  distinct()

sei_prep <- S_SAMPLE_EVENT_INFO_final |>
  dplyr::select(event_id,
                sample_crew,
                flow_condition_sampling_type:flow_condition_numeric,
                discharge_cfs,
                chemistry_sampling_equipment,
                depth_meters:event_comment)

sei_cat <- sei_prep |>
  select(event_id,
         chemistry_sampling_equipment,
         ends_with("present"),
         tidyselect::where(is.character)) |>
  select(-sample_crew) |>
  mutate(across(everything(), as.character)) |>
  tidyr::pivot_longer(
    algae_suspended_present:dominant_observable_land_use,
    names_to = "parameter_name",
    values_to = "result_category"
  ) |>
  filter(!is.na(result_category)) |>
  mutate(
    qualifier = ifelse(grepl("qualifier", parameter_name),
                       result_category,
                       NA_character_),
    result_category = ifelse(grepl("qualifier", parameter_name),
                             NA_character_,
                             result_category
    ),
    parameter_name = case_match(
      parameter_name,
      "flow_condition_category" ~ "flow_condition",
      "depth_qualifier" ~ "stream_depth",
      "width_qualifier" ~ "stream_width",
      "velocity_qualifier" ~ "velocity",
      "secchi_qualifier" ~ "secchi",
      "embeddedness_qualifier" ~ "estimated_embeddedness",
      "macrophyte" ~ "estimated_macrophyte_coverage",
      .default = parameter_name
    )
  )

test <- data.frame(
  match = c("a", "b", "c"),
  QAPP = c("QAPP 1", "QAPP 2", "QAPP 3")
)

example <- data.frame(
  match = c(rep("a", 4), rep("b", 5))
)

result <- left_join(
  x = example,
  y = test,
  by = "match"
)


example <- option1 |>
  pivot_longer(
    -c(a, x, y, z),
    names_to = "parameter",
    values_to = "result_value"
  )

sei_num <- sei_prep |>
  select(event_id,
         tidyselect::where(is.numeric)) |>
  tidyr::pivot_longer(
    !c(event_id),
    names_to = "parameter_name",
    values_to = "result_value"
  ) |>
  filter(!is.na(result_value)) |>
  mutate(
    unit = case_when(
      grepl("meters", parameter_name) ~ "meters",
      grepl("percent", parameter_name) ~ "percent",
      grepl("cfs", parameter_name) ~ "cfs",
      grepl("cm_per_second", parameter_name) ~ "cm_per_second",
      parameter_name %in% c("flow_condition_numeric",
                            "diatom_thickness") ~ "score",
      .default = "ERROR"
    ),
    parameter_name = stringr::str_remove_all(
      parameter_name,
      pattern = paste(c(
        "_meters",
        "_cm_per_second",
        "_percent",
        "_cfs",
        "_numeric",
        "_thickness"
      ),
      collapse = "|")
    ),
    parameter_name = case_match(
      parameter_name,
      "depth" ~ "stream_depth",
      "width" ~ "stream_width",
      "canopy" ~ "canopy_cover",
      "embeddedness" ~ "estimated_embeddedness",
      "macrophyte" ~ "estimated_macrophyte_coverage",
      "diatom" ~ "estimated_diatom_coverage",
      .default = parameter_name
    )
  )

sei_chem_equipment <- sei_prep |>
  distinct(event_id,
           sample_crew,
           sampling_equipment = chemistry_sampling_equipment) |>
  filter(!is.na(sampling_equipment)) |>
  dplyr::rowwise() |>
  dplyr::mutate(
    sample_crew = tidyr::replace_na(sample_crew, "unknown"),
    sample_crew = dplyr::if_else(
      is.na(sample_crew),
      NA_character_,
      paste(
        stringr::str_split_1(
          string = sample_crew,
          pattern = ",") |>
          trimws() |>
          sort(),
        sep = "; ",
        collapse = "; "
      )
    )
  ) |>
  dplyr::distinct()

sei_standard <- full_join(
  x = sei_num,
  y = sei_cat,
  by = join_by(event_id, parameter_name)
) |>
  left_join(sei_chem_equipment,
            by = "event_id",
            # relationship = "many-to-many"
  ) |>
  rename(
    activity_comment = event_comment,
    result_qualifier = qualifier
  ) |>
  mutate(
    activity_type = "habitat_descriptions",
    lab_name = "not_applicable",
    organization_name = "NYSDEC",
    collection_method = "sample_event_info_survey",
    sampling_location = "unknown",
    sample_type = "observation",
    sample_source = "field"
  ) |>
  distinct() |>
  group_by(event_id,
           activity_type,
           collection_method,
           sample_source,
           sampling_location,
           sample_type,
           parameter_name,
           unit) |>
  mutate(
    replicate = as.character(seq_len(n())),
    .after = "event_id"
  ) |>
  ungroup() |>
  dplyr::mutate(
    parameter_description = assign_s_sei_parameter_description(
      x = parameter_name
    )
  )


# Parquet Directory -------------------------------------------------------

build_dir <- file.path(
  "L:",
  "DOW",
  "BWAM Share",
  "data",
  "parquet",
  "build_tables"
)

