
CREATE COLUMN TABLE bigbench_tmp_table1 AS
(SELECT
    "p"."pr_item_sk" AS "pid",
    "p"."r_count" AS "reviews_count",
    "p"."avg_rating" AS "avg_rating",
    "s"."revenue" AS "m_revenue"
  FROM (
    SELECT
      "pr"."pr_item_sk",
      count(*) AS "r_count",
      avg("pr_review_rating") AS "avg_rating"
    FROM "SYSTEM"."SPARK_product_reviews" "pr"
    WHERE "pr"."pr_item_sk" IS NOT NULL
    GROUP BY "pr"."pr_item_sk"
  ) "p"
  INNER JOIN (
    SELECT
      "ws"."ws_item_sk",
      SUM("ws"."ws_net_paid") AS "revenue"
    FROM "SYSTEM"."SPARK_cp_web_sales" "ws"
	WHERE EXISTS(SELECT * FROM (
	    SELECT "d_date_sk"
        FROM "SYSTEM"."SPARK_date_dim" "d"
        WHERE "d"."d_date" >= '2003-01-02'
        AND   "d"."d_date" <= '2003-02-02') AS "dd"
		WHERE "ws"."ws_sold_date_sk" = "dd"."d_date_sk")
	  AND "ws"."ws_item_sk" IS NOT null
    GROUP BY "ws"."ws_item_sk"
  ) "s"
  ON "p"."pr_item_sk" = "s"."ws_item_sk"
);

SELECT corr("reviews_count","avg_rating")
FROM bigbench_tmp_table1
;

DROP TABLE bigbench_tmp_table1;