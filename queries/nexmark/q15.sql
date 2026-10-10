-- PostgreSQL adaptation: day_start is the UTC midnight timestamptz key.
-- date_time is recorded event time; NULL and infinite times retain their SQL groups.
SELECT
    date_bin(INTERVAL '1 day',b.date_time,TIMESTAMPTZ '2000-01-01 00:00:00+00') AS day_start,
    COUNT(*) AS total_bids,
    COUNT(*) FILTER(WHERE b.price<10000) AS rank1_bids,
    COUNT(*) FILTER(WHERE b.price>=10000 AND b.price<1000000) AS rank2_bids,
    COUNT(*) FILTER(WHERE b.price>=1000000) AS rank3_bids,
    COUNT(DISTINCT b.bidder) AS total_bidders,
    COUNT(DISTINCT b.bidder) FILTER(WHERE b.price<10000) AS rank1_bidders,
    COUNT(DISTINCT b.bidder) FILTER(WHERE b.price>=10000 AND b.price<1000000) AS rank2_bidders,
    COUNT(DISTINCT b.bidder) FILTER(WHERE b.price>=1000000) AS rank3_bidders,
    COUNT(DISTINCT b.auction) AS total_auctions,
    COUNT(DISTINCT b.auction) FILTER(WHERE b.price<10000) AS rank1_auctions,
    COUNT(DISTINCT b.auction) FILTER(WHERE b.price>=10000 AND b.price<1000000) AS rank2_auctions,
    COUNT(DISTINCT b.auction) FILTER(WHERE b.price>=1000000) AS rank3_auctions
FROM nexmark.bid b
GROUP BY date_bin(INTERVAL '1 day',b.date_time,TIMESTAMPTZ '2000-01-01 00:00:00+00');
