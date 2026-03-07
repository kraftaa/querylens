SELECT
  SUM(o.amount) AS revenue,
  o.customer_id
FROM orders o
