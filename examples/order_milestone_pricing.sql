SELECT
  m.*,
  (m.base_unit_amount / fx.conversion_rate) AS base_unit_amount_usd,
  (m.base_total_amount / fx.conversion_rate) AS base_total_amount_usd,
  (m.list_unit_amount / fx.conversion_rate) AS list_unit_amount_usd,
  (m.list_total_amount / fx.conversion_rate) AS list_total_amount_usd,
  fx.conversion_rate
FROM raw.order_milestones AS m
LEFT JOIN raw.orders AS o
  ON m.order_id = o.order_id
LEFT JOIN raw.fx_rates AS fx
  ON fx.rate_set_id = o.rate_set_id
  AND fx.target_currency = 'USD';
