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
  SELECT TAXON_ID, FUNCTIONAL_FEEDING_GROUP, TOLERANCE_HBI
  FROM (
  PIVOT TRAIT ON TRAIT_NAME USING FIRST(TRAIT_CATEGORY)
  )
  "
)
test <- dbGetQuery(conn = duckdb_con,
                   "FROM TAXA_CAT")
