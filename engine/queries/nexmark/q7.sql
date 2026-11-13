-- Highest bid in each explicit half-open ten-second PostgreSQL event bucket.
-- Preserve all maximum ties; NULL prices do not satisfy price=maximum.
SELECT w.auction,w.price,w.bidder,w.date_time,w.extra
FROM (
    SELECT s.auction,s.price,s.bidder,s.date_time,s.extra,
           MAX(s.price) OVER(PARTITION BY s.starttime) AS maximum
    FROM (
        SELECT b.auction,b.price,b.bidder,b.date_time,b.extra,
               date_bin(INTERVAL '10 seconds',b.date_time,
                        TIMESTAMPTZ '2000-01-01 00:00:00+00') AS starttime
        FROM nexmark.bid b WHERE b.date_time IS NOT NULL
    ) s
) w
WHERE w.price=w.maximum;
