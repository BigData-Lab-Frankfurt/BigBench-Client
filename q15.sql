
SELECT *
FROM (
  SELECT
    cat,
    ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x) * SUM(x)) ) AS slope,
    (SUM(y) - ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x)*SUM(x)) ) * SUM(x)) / count(x) AS intercept
  FROM (
    SELECT
      "i"."i_category_id" AS cat, 
      "s"."ss_sold_date_sk" AS x,
      SUM("s"."ss_net_paid") AS y,
      "s"."ss_sold_date_sk" * SUM("s"."ss_net_paid") AS xy,
      "s"."ss_sold_date_sk" * "s"."ss_sold_date_sk" AS xx
    FROM "SYSTEM"."SPARK_cp_store_sales" "s"
    INNER JOIN "SYSTEM"."SPARK_cp_item" "i" ON "s"."ss_item_sk" = "i"."i_item_sk"
	WHERE EXISTS(SELECT * FROM (
	    SELECT "d"."d_date_sk"
        FROM "SYSTEM"."SPARK_date_dim" "d"
        WHERE "d"."d_date" >= '2001-09-02'
        AND   "d"."d_date" <= '2002-09-02') AS "dd"
	  WHERE ("s"."ss_sold_date_sk" = "dd"."d_date_sk")) 
    AND "i"."i_category_id" IS NOT NULL
    AND "s"."ss_store_sk" = 10 
    GROUP BY "i"."i_category_id", "s"."ss_sold_date_sk"
  ) temp
  GROUP BY cat
) regression
WHERE slope <= 0
ORDER BY cat
;
