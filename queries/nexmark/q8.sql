-- Distinct person/seller ten-second memberships, joined on identity and start.
-- Fixed width makes end determined by start; SQL equality rejects NULL keys.
SELECT p.id,p.name,p.starttime
FROM (
    SELECT p.id,p.name,
           date_bin(INTERVAL '10 seconds',p.date_time,
                    TIMESTAMPTZ '2000-01-01 00:00:00+00') AS starttime
    FROM nexmark.person p
    GROUP BY p.id,p.name,date_bin(INTERVAL '10 seconds',p.date_time,
                                 TIMESTAMPTZ '2000-01-01 00:00:00+00')
) p
JOIN (
    SELECT a.seller,
           date_bin(INTERVAL '10 seconds',a.date_time,
                    TIMESTAMPTZ '2000-01-01 00:00:00+00') AS starttime
    FROM nexmark.auction a
    GROUP BY a.seller,date_bin(INTERVAL '10 seconds',a.date_time,
                              TIMESTAMPTZ '2000-01-01 00:00:00+00')
) a ON p.id=a.seller AND p.starttime=a.starttime;
