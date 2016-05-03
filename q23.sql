CREATE COLUMN TABLE bigbench_tmp_table1 AS
(SELECT
  "w"."w_warehouse_name",
  "w"."w_warehouse_sk",
  "i"."i_item_sk",
  "d"."d_moy",
  stdev,
  mean,
  cast( (CASE mean WHEN 0.0 THEN NULL ELSE stdev/mean END) as decimal(15,5)) cov
FROM "SYSTEM"."SPARK_date_dim" "d", "SYSTEM"."SPARK_cp_item" "i", "SYSTEM"."SPARK_cp_warehouse" "w", (
  SELECT
    "w"."w_warehouse_name",
    "w"."w_warehouse_sk",
    "i"."i_item_sk",
    "d"."d_moy",
    cast(stddev_samp("inv"."inv_quantity_on_hand") as decimal(15,5)) stdev,
    cast(avg("inv"."inv_quantity_on_hand") as decimal(15,5)) mean
  FROM "SYSTEM"."SPARK_inventory" "inv"
  JOIN "SYSTEM"."SPARK_date_dim" "d" ON ("inv"."inv_date_sk" = "d"."d_date_sk" AND "d"."d_year" = 2001)
  JOIN "SYSTEM"."SPARK_cp_item" "i" ON "inv"."inv_item_sk" = "i"."i_item_sk"
  JOIN "SYSTEM"."SPARK_cp_warehouse" "w" ON "inv"."inv_warehouse_sk" = "w"."w_warehouse_sk"
  GROUP BY
    "w"."w_warehouse_name",
    "w"."w_warehouse_sk",
    "i"."i_item_sk",
    "d"."d_moy"
) q23_tmp_inv_part
WHERE stdev > mean AND mean > 0
);

SELECT
  inv1.w_warehouse_sk AS inv1_w_warehouse_sk,
  inv1.i_item_sk AS inv1_i_item_sk,
  inv1.d_moy AS inv1_d_moy,
  inv1.mean AS inv1_mean,
  inv1.cov AS inv1_cov,
  inv2.w_warehouse_sk AS inv2_w_warehouse_sk,
  inv2.i_item_sk AS inv2_i_item_sk,
  inv2.d_moy AS inv2_d_moy,
  inv2.mean AS inv2_mean,
  inv2.cov AS inv2_cov
FROM bigbench_tmp_table1 inv1
JOIN bigbench_tmp_table1 inv2 ON (
  inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1
  AND inv2.d_moy = 1 + 1
)
ORDER BY
  inv1_w_warehouse_sk,
  inv1_i_item_sk,
  inv1_d_moy,
  inv1_mean,
  inv1_cov,
  inv2_d_moy,
  inv2_mean,
  inv2_cov
;

SELECT
  inv1.w_warehouse_sk AS inv1_w_warehouse_sk,
  inv1.i_item_sk AS inv1_i_item_sk,
  inv1.d_moy AS inv1_d_moy,
  inv1.mean AS inv1_mean,
  inv1.cov AS inv1_cov,
  inv2.w_warehouse_sk AS inv2_w_warehouse_sk,
  inv2.i_item_sk AS inv2_i_item_sk,
  inv2.d_moy AS inv2_d_moy,
  inv2.mean AS inv2_mean,
  inv2.cov AS inv2_cov
FROM bigbench_tmp_table1 inv1
JOIN bigbench_tmp_table1 inv2 ON (
  inv1.i_item_sk = inv2.i_item_sk
  AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
  AND inv1.d_moy = 1
  AND inv2.d_moy = 1 + 1
  AND inv1.cov > 1.5
)
ORDER BY
  inv1_w_warehouse_sk,
  inv1_i_item_sk,
  inv1_d_moy,
  inv1_mean,
  inv1_cov,
  inv2_d_moy,
  inv2_mean,
  inv2_cov
;

DROP TABLE bigbench_tmp_table1;