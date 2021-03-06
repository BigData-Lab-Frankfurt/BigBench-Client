
SELECT
  "i"."i_item_id",
  "i"."i_item_desc",
  "s"."s_store_id",
  "s"."s_store_name",
  SUM("ss"."ss_quantity") AS store_sales_quantity,
  SUM("sr"."sr_return_quantity") AS store_returns_quantity,
  SUM("ws"."ws_quantity") AS web_sales_quantity
FROM
  "SYSTEM"."SPARK_cp_store_sales" "ss",
  "SYSTEM"."SPARK_cp_store_returns" "sr",
  "SYSTEM"."SPARK_cp_web_sales" "ws",
  "SYSTEM"."SPARK_date_dim" "d1",
  "SYSTEM"."SPARK_date_dim" "d2",
  "SYSTEM"."SPARK_date_dim" "d3",
  "SYSTEM"."SPARK_cp_store" "s",
  "SYSTEM"."SPARK_cp_item" "i"
WHERE "d1"."d_year"         = 2003 
AND "d1"."d_moy"            = 1
AND "d1"."d_date_sk"        = "ss"."ss_sold_date_sk"
AND "i"."i_item_sk"          = "ss"."ss_item_sk"
AND "s"."s_store_sk"          = "ss"."ss_store_sk"
AND "ss"."ss_customer_sk"      = "sr"."sr_customer_sk"
AND "ss"."ss_item_sk"          = "sr"."sr_item_sk"
AND "ss"."ss_ticket_number"    = "sr"."sr_ticket_number"
AND "sr"."sr_returned_date_sk" = "d2"."d_date_sk"
AND "d2"."d_moy" BETWEEN 1 AND 1 + 6 
AND "d2"."d_year"           = 2003
AND "sr"."sr_customer_sk"      = "ws"."ws_bill_customer_sk" 
AND "sr"."sr_item_sk"          = "ws"."ws_item_sk"
AND "ws"."ws_sold_date_sk"     = "d3"."d_date_sk"
AND "d3"."d_year" BETWEEN 2003 AND 2003 + 2 
GROUP BY
  "i"."i_item_id",
  "i"."i_item_desc",
  "s"."s_store_id",
  "s"."s_store_name"
ORDER BY
  "i"."i_item_id",
  "i"."i_item_desc",
  "s"."s_store_id",
  "s"."s_store_name"
LIMIT 100;