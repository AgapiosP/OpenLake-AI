-- After uploading examples/sample_sales.csv as sample_sales:
SELECT category, ROUND(SUM(revenue),2) AS total_revenue, SUM(quantity) AS units
FROM sample_sales
GROUP BY category
ORDER BY total_revenue DESC;
