-- Partition labels are UTC calendar fields of recorded event time.
SELECT auction,bidder,price,date_time,extra,
       pg_catalog.to_char(date_time AT TIME ZONE 'UTC','YYYY-MM-DD') AS dt,
       pg_catalog.to_char(date_time AT TIME ZONE 'UTC','HH24:MI') AS hm
FROM nexmark.bid;
