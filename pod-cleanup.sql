WITH max_keys AS (
    SELECT k.id as key_id, max(t.added) as latest
    FROM keys k, tsd t
    WHERE k.id = t.key_id
        AND k.key LIKE 'k8s.pods.%'
        AND t.added > CURRENT_TIMESTAMP + INTERVAL '-30 day'
    GROUP BY k.id
)
SELECT key_id
FROM max_keys
WHERE latest < CURRENT_TIMESTAMP + INTERVAL '-7 day';
