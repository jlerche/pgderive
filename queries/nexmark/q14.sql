-- Tuple-local PostgreSQL q14. Instants use UTC; no implicit clock or expiry.
SELECT auction,bidder,0.908 * price AS price,
       CASE WHEN extract(hour FROM date_time AT TIME ZONE 'UTC') >= 8
                  AND extract(hour FROM date_time AT TIME ZONE 'UTC') <= 18
            THEN 'dayTime'
            WHEN extract(hour FROM date_time AT TIME ZONE 'UTC') <= 6
                  OR extract(hour FROM date_time AT TIME ZONE 'UTC') >= 20
            THEN 'nightTime'
            ELSE 'otherTime' END AS "bidTimeType",
       date_time,extra,length(extra)-length(replace(extra,'c','')) AS c_counts
FROM nexmark.bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;
