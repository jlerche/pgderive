-- Explicit PostgreSQL sessions: adjacent gap <10 seconds connects occurrences.
-- Recorded event time; NULL times excluded, no implicit expiry or watermark.
SELECT g.bidder,g.starttime,g.lasttime+INTERVAL '10 seconds' AS endtime,g.bid_count
FROM (
    SELECT s.bidder,MIN(s.time) AS starttime,MAX(s.time) AS lasttime,COUNT(*) AS bid_count
    FROM (
        SELECT z.bidder,z.time,
               SUM(z.flag) OVER (
                   PARTITION BY z.bidder ORDER BY z.time,z.id ROWS UNBOUNDED PRECEDING
               ) AS session
        FROM (
            SELECT q.id,q.bidder,q.time,
                   CASE WHEN q.previous IS NULL THEN 1
                        WHEN q.time=q.previous THEN 0
                        WHEN q.time-q.previous>=INTERVAL '10 seconds' THEN 1
                        ELSE 0 END AS flag
            FROM (
                SELECT b.id,b.bidder,b.date_time AS time,
                       lag(b.date_time) OVER (
                           PARTITION BY b.bidder ORDER BY b.date_time,b.id
                       ) AS previous
                FROM nexmark.bid b WHERE b.date_time IS NOT NULL
            ) q
        ) z
    ) s
    GROUP BY s.bidder,s.session
) g;
