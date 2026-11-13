-- Explicit UTC midnight day_start key; native integral price, exact AVG/SUM.
SELECT b.auction,
       date_bin(INTERVAL '1 day',b.date_time,TIMESTAMPTZ '2000-01-01 00:00:00+00') AS day_start,
       COUNT(*) AS total_bids,
       COUNT(*) FILTER(WHERE b.price<10000) AS rank1_bids,
       COUNT(*) FILTER(WHERE b.price>=10000 AND b.price<1000000) AS rank2_bids,
       COUNT(*) FILTER(WHERE b.price>=1000000) AS rank3_bids,
       MIN(b.price) AS min_price,MAX(b.price) AS max_price,
       AVG(b.price) AS avg_price,SUM(b.price) AS sum_price
FROM nexmark.bid b
GROUP BY b.auction,date_bin(INTERVAL '1 day',b.date_time,TIMESTAMPTZ '2000-01-01 00:00:00+00');
