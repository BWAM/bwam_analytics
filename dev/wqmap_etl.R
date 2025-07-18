
# Load Dependencies -------------------------------------------------------
library(DBI)
library(dm)
library(duckdb)
library(glue)
library(nexus) # Custom R-package available here: https://github.com/BWAM/nexus

base_dir <- file.path("L:",
                      "DOW",
                      "BWAM Share",
                      "data",
                      "parquet")

build_dir <- "C:/Users/ZMSMITH/OneDrive - New York State Office of Information Technology Services/Downloads/wqmad_parquets"
analytical_dir <- file.path(base_dir, "analytical_table_store")


# Connect to the WQMA Oracle Data Warehouse -------------------------------

wqma_con <- get_connected(username = "ZSMITH",
                          dsn = "wqma_dev"
                          # keyring = "wqmad"
                          )
#
data_model <- get_data_model(con = wqma_con)

# con <- DBI::dbConnect(
#   drv = duckdb::duckdb(),
#   dbdir = "L:/DOW/BWAM Share/data/data_warehouses/BWAM_ETL/BWAM_ETL.duckdb "
# )

# DBI::dbExecute(con, "CREATE SCHEMA WQMAP;")

purrr::walk(names(data_model),
            .f = function(.x) {
              print(paste("Querying:",.x))
              tictoc::tic()
              df <- get_big_table(con = wqma_con,
                                  table = .x,
                                  n = 10000,
                                  type = "all")

              arrow::write_parquet(x = df,
                                   sink = file.path(build_dir,
                                                    paste0(.x, ".parquet")))
              print(tictoc::toc()$callback_msg)
              gc()
            })

DBI::dbDisconnect(wqma_con)