
CREATE VIEW bigbench_tmp_view1 AS
SELECT
  "ss_customer_sk" AS customer_sk,
  SUM( CASE WHEN ("d_year" = 2001)     THEN ((("ss_ext_list_price" - "ss_ext_wholesale_cost" - "ss_ext_discount_amt") + "ss_ext_sales_price") / 2) ELSE 0 END) first_year_total,
  SUM( CASE WHEN ("d_year" = 2001 + 1) THEN ((("ss_ext_list_price" - "ss_ext_wholesale_cost" - "ss_ext_discount_amt") + "ss_ext_sales_price") / 2) ELSE 0 END) second_year_total
FROM
  "SYSTEM"."SPARK_cp_store_sales",
  "SYSTEM"."SPARK_date_dim"
WHERE "ss_sold_date_sk" = "d_date_sk"
AND "d_year" BETWEEN 2001 AND 2001 + 1
GROUP BY "ss_customer_sk"
;

CREATE VIEW bigbench_tmp_view2 AS
SELECT
  "ws_bill_customer_sk" AS customer_sk,
  SUM( CASE WHEN ("d_year" = 2001)     THEN ((("ws_ext_list_price" - "ws_ext_wholesale_cost" - "ws_ext_discount_amt") + "ws_ext_sales_price") / 2) ELSE 0 END) first_year_total,
  SUM( CASE WHEN ("d_year" = 2001 + 1) THEN ((("ws_ext_list_price" - "ws_ext_wholesale_cost" - "ws_ext_discount_amt") + "ws_ext_sales_price") / 2) ELSE 0 END) second_year_total
FROM
  "SYSTEM"."SPARK_cp_web_sales",
  "SYSTEM"."SPARK_date_dim"
WHERE "ws_sold_date_sk" = "d_date_sk"
AND "d_year" BETWEEN 2001 AND 2001 + 1
GROUP BY "ws_bill_customer_sk"
;

SELECT
  "c_customer_sk",
  "c_first_name",
  "c_last_name",
  "c_preferred_cust_flag",
  "c_birth_country",
  "c_login",
  "c_email_address"
FROM
  bigbench_tmp_view1 store,
  bigbench_tmp_view2 web,
  "SYSTEM"."SPARK_customer" "c"
WHERE store.customer_sk = web.customer_sk
AND web.customer_sk = "c"."c_customer_sk"
AND CASE WHEN web.first_year_total > 0 THEN web.second_year_total / web.first_year_total ELSE NULL END
  > CASE WHEN store.first_year_total > 0 THEN store.second_year_total / store.first_year_total ELSE NULL END
ORDER BY
  "c"."c_customer_sk",
  "c"."c_first_name",
  "c"."c_last_name",
  "c"."c_preferred_cust_flag",
  "c"."c_birth_country",
  "c"."c_login"
LIMIT 100;

DROP VIEW bigbench_tmp_view1;
DROP VIEW bigbench_tmp_view2;
