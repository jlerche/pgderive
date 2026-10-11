-- PostgreSQL adaptation: day_start uses the q15 UTC midnight key ABI.
-- Latest minute is formatted only after native retractable MAX.
SELECT q.channel,q.day_start,pg_catalog.to_char(q.latest AT TIME ZONE 'UTC','HH24:MI') AS minute,
       q.total_bids,q.rank1_bids,q.rank2_bids,q.rank3_bids,q.total_bidders,q.rank1_bidders,q.rank2_bidders,q.rank3_bidders,q.total_auctions,q.rank1_auctions,q.rank2_auctions,q.rank3_auctions
FROM(
SELECT
    b.channel AS channel,
    MAX(b.date_time) AS latest,
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
GROUP BY b.channel,date_bin(INTERVAL '1 day',b.date_time,TIMESTAMPTZ '2000-01-01 00:00:00+00')
) q;
