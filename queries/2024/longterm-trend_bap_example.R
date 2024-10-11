
# Load Dependencies -------------------------------------------------------
library(arrow)
library(dplyr)
library(ggplot2)

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
    SITE_CODE,
    LATITUDE, LONGITUDE,
    SAMPLE_LOCATION,
    SAMPLE_TYPE, SAMPLE_ORGANIZATION,
    SAMPLE_METHOD,
    PARAMETER_NAME, RESULT_VALUE, UNIT,
  ) |>
  distinct() |>
  summarize(RESULT_VALUE = mean(RESULT_VALUE),
            .by = c(WATERBODY_TYPE, SITE_CODE, EVENT_ID,
                    EVENT_DATETIME, SAMPLE_METHOD, PARAMETER_NAME)) |>
  collect() |>
  dplyr::mutate(
    EVENT_DATETIME = lubridate::ymd_hms(EVENT_DATETIME),
    DECADE = case_when(
      EVENT_DATETIME < "1983-01-01" ~ "1973-1982",
      dplyr::between(
        EVENT_DATETIME,
        as.Date("1983-01-01"),
        as.Date("1993-01-01")
      ) ~ "1983-1992",
      dplyr::between(
        EVENT_DATETIME,
        as.Date("1993-01-01"),
        as.Date("2003-01-01")
      ) ~ "1993-2002",
      dplyr::between(
        EVENT_DATETIME,
        as.Date("2003-01-01"),
        as.Date("2013-01-01")
      ) ~ "2003-2012",
      dplyr::between(
        EVENT_DATETIME,
        as.Date("2013-01-01"),
        as.Date("2023-01-01")
      ) ~ "2013-2022",
      .default = "ERROR"
    ),
    .before = everything(),
    DECADE = factor(DECADE),
  ) |>
  summarize(MEAN_VALUE = mean(RESULT_VALUE),
            .by = c(WATERBODY_TYPE, SITE_CODE, DECADE,
                    SAMPLE_METHOD, PARAMETER_NAME)) |>
  add_count(SITE_CODE, SAMPLE_METHOD) |>
  # Decide how many decades the site has to be present to be retained.
  filter(n > 3)



ggplot(final_df, aes(DECADE, MEAN_VALUE)) +
  geom_point(alpha = 0.1) +
  geom_line(aes(group = SITE_CODE),
            alpha = 0.1) +
  geom_hline(yintercept = 5,
             color = "red2") +
  geom_boxplot() +
  geom_smooth(aes(group = SAMPLE_METHOD),
              color = "#3186B1",
              fill = "#3186B1",
              formula = y ~ x,
              method = "lm") +
  xlab("10-Year Period") +
  ylab("Biological Assessment Profile Score") +
  labs(color = "Sample Method",
       fill = "Sample Method") +
  theme_bw() +
  facet_wrap(~SAMPLE_METHOD, ncol = 2) +
  theme(strip.background = element_rect(fill = "white"))

