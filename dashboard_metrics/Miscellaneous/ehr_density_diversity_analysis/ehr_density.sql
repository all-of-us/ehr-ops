WITH g_val_n AS
(
SELECT DISTINCT person_id, VAR_POP(g_val) AS g_val_var,
CAST(count(event_datetime) AS INT64) as n
FROM `{EHR_OPS_PROJECT_ID}.{EHR_DENSITY_DATASET_ID}.g_vals`
GROUP BY 1
)
SELECT DISTINCT person_id, g_val_var, n as total_events,
IFNULL(2/n + (n-2)/n*(1-SQRT((n-1)*g_val_var)), 0) AS density
FROM g_val_n
