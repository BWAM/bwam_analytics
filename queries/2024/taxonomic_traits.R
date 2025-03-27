dbExecute(
  conn = duckdb_con,
  statement = "CREATE OR REPLACE TABLE TAXONOMIC_TRAIT_WIDE AS
          SELECT * TAXONOMIC_TRAIT

  "
)

traits <- dbGetQuery(
  conn = duckdb_con,
  statement = "FROM TAXONOMIC_TRAIT"
)
dbExecute(
  conn = duckdb_con,
  statement = "CREATE OR REPLACE TABLE TAXA_CAT AS
  WITH TRAIT AS (
  SELECT TAXON_ID, TRAIT_NAME, TRAIT_CATEGORY
  FROM TAXONOMIC_TRAIT
  )
  SELECT TAXON_ID, FUNCTIONAL_FEEDING_GROUP, TOLERANCE_HBI AS TOLERANCE_HBI_CATEGORY
  FROM (
  PIVOT TRAIT ON TRAIT_NAME USING FIRST(TRAIT_CATEGORY)
  )
  "
)
test <- dbGetQuery(conn = duckdb_con,
                   "FROM TAXA_CAT")

dbExecute(
  conn = duckdb_con,
  statement = "CREATE OR REPLACE TABLE TAXA_VAL AS
  WITH TRAIT AS (
  SELECT TAXON_ID, TRAIT_NAME, TRAIT_VALUE
  FROM TAXONOMIC_TRAIT
  )
  SELECT TAXON_ID, COLUMNS('tolerance_*')
  FROM (
  PIVOT TRAIT ON TRAIT_NAME USING FIRST(TRAIT_VALUE)
  )
  "
)

test2 <- dbGetQuery(conn = duckdb_con,
                   "FROM TAXA_VAL")

dbExecute(
  conn = duckdb_con,
  statement = "CREATE OR REPLACE TABLE TAXA AS
  with t1 AS (
  SELECT *
  FROM TAXONOMY
  LEFT JOIN TAXA_CAT USING (TAXON_ID)
  LEFT JOIN TAXA_VAL USING (TAXON_ID)
  )
  ALTER TABLE t1RENAME functional_feeding_group to FUNCTIONAL_FEEDING_GROUP

  "
)


test3 <- dbGetQuery(conn = duckdb_con,
                    "SELECT UPPER(*) FROM TAXA")
