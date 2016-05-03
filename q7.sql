CREATE COLUMN TABLE bigbench_tmp_table1 AS
(SELECT "k"."i_item_sk"
FROM "SYSTEM"."SPARK_cp_item" "k",
(
  SELECT
    AVG("j"."i_current_price") * 1.2 AS "avg_price"
  FROM "SYSTEM"."SPARK_cp_item" "j"
  GROUP BY "j"."i_category"
) "avgCategoryPrice"
WHERE "k"."i_current_price" > "avgCategoryPrice"."avg_price")
;

SELECT
  "ca_state",
  COUNT(*) AS cnt
FROM
  "SYSTEM"."SPARK_cp_customer_address" "a",
  "SYSTEM"."SPARK_customer" "c",
  "SYSTEM"."SPARK_cp_store_sales" "s",
  "SYSTEM"."BIGBENCH_TMP_TABLE1" "highPriceItems"
WHERE "a"."ca_address_sk" = "c"."c_current_addr_sk"
AND "c"."c_customer_sk" = "s"."ss_customer_sk"
AND "ca_state" IS NOT NULL
AND "highPriceItems"."i_item_sk" = "s"."ss_item_sk"
AND "s"."ss_sold_date_sk" IN
( 
  SELECT "d_date_sk"
  FROM "SYSTEM"."SPARK_date_dim"
  WHERE "d_year" = 2004
  AND "d_moy" = 7
)
GROUP BY "ca_state"
HAVING COUNT(*) >= 10 
ORDER BY cnt DESC, "ca_state"
LIMIT 10
;

DROP TABLE bigbench_tmp_table1;