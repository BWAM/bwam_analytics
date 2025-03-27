# Load the necessary packages.
library(dplyr)
library(tidyr)
# A Dummy data frame.
df <- data.frame(
  a = 1:10,
  b = c(NA, "example"),
  c = c("example", NA)
)

# Option 1 (Recommended)
option1 <- df |>
  replace_na(
    list(
      b = "missing",
      c = "not-applicable"
    )
  )

# Option 2
option2 <- df |>
  mutate(
    b = replace_na(b, "missing"),
    c = replace_na(c, "not-applicable")
  )

# Check that both options produce the same results.
identical(option1, option2)
