
# Request -----------------------------------------------------------------
# We are asking for your organization or agency’s recent macroinvertebrate
# counts, habitat scores, and field water quality data for Chesapeake watershed
# streams and rivers. Our immediate objective is to enter data from multiple
# stream biological monitoring programs into the Chesapeake Environmental Data
# Repository (CEDR) database and update the Chesapeake Bay Program (CBP) Stream
# Health Indicator.  Listed below are the Minimum Data Elements, or fields, we
# need from you.  Any additional data fields, relevant reports, data
# documentation and Quality Assurance Project Plans (QAPPs) you can send with
# your dataset would be very welcome.
# Load Dependencies-------------------------------------------------------
library(arrow)
library(dplyr)
library(ggplot2)
library(lubridate)
library(openxlsx2)
library(stringr)

# Directories -------------------------------------------------------------
base_dir <- file.path("L:",
                      "DOW",
                      "BWAM Share",
                      "data",
                      "parquet",
                      "analytical_table_store")

obt_result_dir <- file.path(base_dir, "obt_result.parquet")

obt_taxa_dir <- file.path(base_dir, "obt_taxa_abundance.parquet")


# Queries -----------------------------------------------------------------

## Taxa Query -------------------------------------------------------------

taxa_raw <- open_dataset(obt_taxa_dir) |>
  filter(between(EVENT_DATETIME, as.Date("2015-01-01"), as.Date("2024-01-01")),
         WATERBODY_TYPE %in% "river_stream",
         BASIN %in% c("05", "06"),
         !grepl("multiplate", SAMPLE_METHOD)) |>
  distinct() |>
  collect()

taxa_prepped <- taxa_raw |>
  rename(
    # Event -------------------------------------------------------------------
    MonitoringLocationName = SITE_CODE,
    FieldActivityLatitudeMeasure = LATITUDE,
    FieldActivityLongitudeMeasure = LONGITUDE,
    # taxon -------------------------------------------------------------------
    SampleNumber = REPLICATE,
    TSN = ITIS_TSN,

  ) |>
  mutate(
    # Event -------------------------------------------------------------------
    FieldActivityStartDate = date(EVENT_DATETIME),
    FieldActivityStartTime = format(EVENT_DATETIME, "%H:%M:%S"),
    ProviderCode = "NYSDEC",
    CollectorCode = "NYSDEC/SBM",
    HorizontalCorrdinateRefrenceSystemDatumName = HORIZONTAL_DATUM,
    FieldActivitySiteTypeCode = "U",
    SampleTypeCode = case_match(
      .x = SAMPLE_METHOD,
      "kick_standard" ~ "CS",
      "low_gradient" ~ "CS",
      "kick_sandy-stream" ~ "CS"
    ),
    EquipmentCode = case_match(
      .x = SAMPLE_METHOD,
      "kick_standard" ~ 86,
      "low_gradient" ~ 86,
      "kick_sandy-stream" ~ 86
    ),
    LatinName = TAXON_ID |>
      str_to_title() |>
      str_replace_all(
      pattern = "_",
      replacement = " "
    ),
    SPECCode = TAXON_ID,
    BiologicalFrequencyClass = "Not Spcified",
    BiologicalAnalyticalMethodCode = case_match(
      .x = SAMPLE_METHOD,
      "kick_standard" ~ "BE102",
      "low_gradient" ~ "NYSDEC_low_gradient_protocol",
      "kick_sandy-stream" ~ "BE120"
    ),
    ReportingValue = RESULT_VALUE


    ) |>
  distinct(
    MonitoringLocationName,
    FieldActivityStartDate,
    FieldActivityStartTime,
    ProviderCode,
    CollectorCode,
    FieldActivityLatitudeMeasure,
    FieldActivityLongitudeMeasure,
    HorizontalCorrdinateRefrenceSystemDatumName,
    FieldActivitySiteTypeCode,
    SampleTypeCode,
    SampleNumber,
    EquipmentCode,
    TSN,
    LatinName,
    SPECCode,
    BiologicalFrequencyClass,
    BiologicalAnalyticalMethodCode,
    ReportingValue
  )


event_final <- taxa_prepped |>
  distinct(
    MonitoringLocationName,
    FieldActivityStartDate,
    FieldActivityStartTime,
    ProviderCode,
    CollectorCode,
    FieldActivityLatitudeMeasure,
    FieldActivityLongitudeMeasure,
    HorizontalCorrdinateRefrenceSystemDatumName,
    FieldActivitySiteTypeCode
  )

taxon_final <- taxa_prepped |>
  distinct(
    MonitoringLocationName,
    FieldActivityStartDate,
    SampleTypeCode,
    SampleNumber,
    EquipmentCode,
    TSN,
    LatinName,
    SPECCode,
    BiologicalFrequencyClass,
    BiologicalAnalyticalMethodCode,
    ReportingValue
  )

## WQ Query -----------------------------------------------------------

wq_raw <- open_dataset(obt_result_dir) |>
  filter(EVENT_ID %in% unique(taxa_raw$EVENT_ID),
         PARAMETER_NAME %in% c(
           "dissolved_oxygen",
           "dissolved_oxygen_saturation",
           "ph",
           "salinity",
           "specific_conductance",
           "temperature",
           "stream_depth",
           "stream_width",
           "velocity"
         )) |>
  distinct() |>
  collect()

wq_final <- wq_raw |>
  transmute(
    MonitoringLocationName = SITE_CODE,
    FieldActivityStartDate = date(EVENT_DATETIME),
    SampleType = "D",
    SampleReplicateTypeCode = REPLICATE,
    SampleDepth = SAMPLE_DEPTH_METERS,
    SubstanceIdentificationName = case_match(
      .x = PARAMETER_NAME,
      "dissolved_oxygen" ~ "DO",
      "dissolved_oxygen_saturation" ~ "DO_SAT_P",
      "ph" ~ "PH",
      "salinity" ~ "SALINITY",
      "specific_conductance" ~ "SPCOND",
      "temperature" ~ "WTEMP",
      "stream_depth" ~ "TOTAL_DEPTH",
      "stream_width" ~ "WIDTH",
      "velocity" ~ "VELOCITY"
    ),
    MeasureValue = RESULT_VALUE,
    MeasureUnit = UNIT,
    MeasureQualfierCode = "A",
    SampleAnalyticalMethodIdentifier = case_match(
      .x = PARAMETER_NAME,
      "dissolved_oxygen" ~ "F04",
      "dissolved_oxygen_saturation" ~ "F01",
      "ph" ~ "F01",
      "salinity" ~ "F01",
      "specific_conductance" ~ "F01",
      "temperature" ~ "F01",
      "stream_depth" ~ "F03",
      "stream_width" ~ "F01",
      "velocity" ~ "F01"
    )
  ) |>
  distinct()

## Habitat Query ----------------------------------------------------------

habitat_raw <- open_dataset(obt_result_dir) |>
  filter(EVENT_ID %in% unique(taxa_raw$EVENT_ID),
         SAMPLE_TYPE %in% c("habitat_assessment_high_gradient",
                            "habitat_assessment_low_gradient")) |>
  distinct() |>
  collect()


lr_habitat_prep <- habitat_raw |>
  filter(PARAMETER_NAME %in% c("left_bank_riparian_zone_width",
                                "left_bank_stability",
                                "left_bank_vegetative_protection",
                                "right_bank_riparian_zone_width",
                                "right_bank_stability",
                                "right_bank_vegetative_protection")) |>
  transmute(
    MonitoringLocationName = SITE_CODE,
    FieldActivityStartDate = date(EVENT_DATETIME),
    HabitatReportingCharacteristicCode = case_match(
      .x = PARAMETER_NAME,
      "left_bank_riparian_zone_width" ~ "RIP_W",
      "left_bank_stability" ~ "BANKS",
      "left_bank_vegetative_protection" ~ "BANKV",
      "right_bank_riparian_zone_width" ~ "RIP_W",
      "right_bank_stability" ~ "BANKS",
      "right_bank_vegetative_protection"~ "BANKV",
    ),
    HabitatReportingCharactersiticValue = RESULT_VALUE,
    BiologicalAnalyticalMethodCode = "HAB101"
  ) |>
  summarize(
    HabitatReportingCharactersiticValue = sum(HabitatReportingCharactersiticValue),
    .by = c("MonitoringLocationName",
            "FieldActivityStartDate",
            "HabitatReportingCharacteristicCode",
            "BiologicalAnalyticalMethodCode")
  )


habitat_prep <- habitat_raw |>
  filter(!PARAMETER_NAME %in% c("habitat_assessment_score",
                               "habitat_model_affinity_score",
                               "left_bank_riparian_zone_width",
                               "left_bank_stability",
                               "left_bank_vegetative_protection",
                               "right_bank_riparian_zone_width",
                               "right_bank_stability",
                               "right_bank_vegetative_protection")) |>
  transmute(
    MonitoringLocationName = SITE_CODE,
    FieldActivityStartDate = date(EVENT_DATETIME),
    HabitatReportingCharacteristicCode = case_match(
      .x = PARAMETER_NAME,
      "channel_alteration" ~ "CH_ALT",
      "channel_sinuosity" ~ "SINU",
      "embeddedness" ~ "EMBED",
      "epifaunal_cover" ~ "EPI_SUB",
      "flow_status" ~ "FLOW",
      "pool_substrate" ~ "P_SUB",
      "pool_variability" ~ "POOL",
      "riffle_frequency" ~ "RIFF",
      "sediment_deposition" ~ "SED",
      "velocity_depth_regime" ~ "VEL_D"
    ),
    HabitatReportingCharactersiticValue = RESULT_VALUE,
    BiologicalAnalyticalMethodCode = "HAB101"
  ) |>
  distinct()

habitat_final <- bind_rows(lr_habitat_prep,
                           habitat_prep)


# Export ------------------------------------------------------------------


wb <- wb_workbook()
wb$add_worksheet("Event")
wb$add_data_table("Event", event_final, table_style = "TableStyleMedium2")
wb$add_worksheet("Taxon")
wb$add_data_table("Taxon", taxon_final, table_style = "TableStyleMedium2")
wb$add_worksheet("WQ")
wb$add_data_table("WQ", wq_final, table_style = "TableStyleMedium2")
wb$add_worksheet("Habitat")
wb$add_data_table("Habitat", habitat_final, table_style = "TableStyleMedium2")

# open it in your default spreadsheet software
if (interactive()) wb$open()

wb_save(wb,
        file = file.path("L:/DOW/BWAM Share/data/projects/2025/CHESSIE_BIBI",
                         "NYSDEC_2024-CHESSIE-BIBI_Data-Request.xlsx"),
        overwrite = TRUE)
