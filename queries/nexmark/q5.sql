-- Ten-second hopping windows, two-second slide; retain every auction/count tie.
-- Final projection omits window identity but preserves its bag multiplicity.
SELECT m.auction,m.num
FROM (
    SELECT g.auction,g.starttime,g.num,
           MAX(g.num) OVER(PARTITION BY g.starttime) AS maximum
    FROM (
        SELECT b.auction,
               date_bin(INTERVAL '2 seconds',b.date_time,
                        TIMESTAMPTZ '2000-01-01 00:00:00+00') - w.n * INTERVAL '2 seconds' AS starttime,
               COUNT(*) AS num
        FROM nexmark.bid b CROSS JOIN generate_series(0,4) w(n)
        WHERE b.date_time IS NOT NULL
        GROUP BY b.auction,date_bin(INTERVAL '2 seconds',b.date_time,
                                    TIMESTAMPTZ '2000-01-01 00:00:00+00') - w.n * INTERVAL '2 seconds'
    ) g
) m
WHERE m.num=m.maximum;
