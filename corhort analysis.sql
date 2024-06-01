
WITH table_frist_month AS (
   SELECT customer_id, transaction_id, transaction_time
       , MIN ( MONTH (transaction_time) ) OVER ( PARTITION BY customer_id) AS first_month
   From fact_transaction_2019 As fact19
   LEFT JOIN dim_scenario as sce
           ON sce.scenario_id = fact19.scenario_id
   Where sub_category = 'Telco card' and status_id = 1
)
, table_sub_month AS (
   SELECT *
       , MONTH (transaction_time) - first_month AS subsequent_month
   FROM table_frist_month
)
, table_retained AS (
   SELECT first_month AS acquisition_month
       , subsequent_month
       , COUNT (DISTINCT customer_id) AS retained_customers
   FROM table_sub_month
   GROUP BY first_month, subsequent_month
   -- ORDER BY first_month, subsequent_month
)
SELECT *
   , FIRST_VALUE (retained_customers) OVER ( PARTITION BY acquisition_month ORDER BY subsequent_month ASC ) AS original_customer
   , CAST (retained_customers AS FLOAT ) / FIRST_VALUE (retained_customers) OVER ( PARTITION BY acquisition_month ORDER BY subsequent_month ASC ) AS pct
INTO #table_rentention
FROM table_retained

-- 1.2 B Pivot kết quả ra thành từng cột để biểu diễn heatmap


SELECT acquisition_month, original_customer
   , "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"


FROM ( 
   SELECT acquisition_month, subsequent_month, original_customer, CAST ( pct AS DECIMAL (10,2) ) AS pct
   FROM #table_rentention
) AS source_table


PIVOT (
   SUM ( pct )
   FOR subsequent_month IN ( "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11")
) AS pivot_logic 
ORDER BY acquisition_month