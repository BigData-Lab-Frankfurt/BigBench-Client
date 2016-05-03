
SELECT "w"."w_state", "i"."i_item_id",
  SUM(
    CASE WHEN (TO_DATE("d"."d_date") < TO_DATE('2001-03-16'))
    THEN "a1"."ws_sales_price" - COALESCE("a1"."wr_refunded_cash",0)
    ELSE 0.0 END
  ) AS sales_before,
  SUM(
    CASE WHEN (TO_DATE("d"."d_date") >= TO_DATE('2001-03-16'))
    THEN "a1"."ws_sales_price" - COALESCE("a1"."wr_refunded_cash",0)
    ELSE 0.0 END
  ) AS sales_after
FROM (
  SELECT *
  FROM "SYSTEM"."SPARK_cp_web_sales" "ws"
  LEFT OUTER JOIN "SYSTEM"."SPARK_cp_web_returns" "wr" ON ("ws"."ws_order_number" = "wr"."wr_order_number"
    AND "ws"."ws_item_sk" = "wr"."wr_item_sk")
) "a1"
JOIN "SYSTEM"."SPARK_cp_item" "i" ON "a1"."ws_item_sk" = "i"."i_item_sk"
JOIN "SYSTEM"."SPARK_cp_warehouse" "w" ON "a1"."ws_warehouse_sk" = "w"."w_warehouse_sk"
JOIN "SYSTEM"."SPARK_date_dim" "d" ON "a1"."ws_sold_date_sk" = "d"."d_date_sk"
AND TO_DATE("d"."d_date") >= ADD_DAYS(TO_DATE('2001-03-16'), -30) 
AND TO_DATE("d"."d_date") <= ADD_DAYS(TO_DATE('2001-03-16'), 30) 
GROUP BY "w"."w_state","i"."i_item_id"
ORDER BY "w"."w_state","i"."i_item_id"
LIMIT 100
;