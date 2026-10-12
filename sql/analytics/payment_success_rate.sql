-- Business question:
-- What percentage of payment records have a successful status?

SELECT
    COUNT(*) AS total_payments,
    COUNT(*) FILTER (
        WHERE status = 'success'
    ) AS successful_payments,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE status = 'success'
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS success_rate_pct
FROM payments;
