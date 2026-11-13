-- PostgreSQL q18: latest occurrence per bidder/auction.
-- DESC NULLS FIRST is PostgreSQL's default; stable bid ID resolves equal times.
SELECT w.auction,w.bidder,w.price,w.channel,w.url,w.date_time,w.extra
FROM (
    SELECT b.id,b.auction,b.bidder,b.price,b.channel,b.url,b.date_time,b.extra,
           row_number() OVER (
               PARTITION BY b.bidder,b.auction
               ORDER BY b.date_time DESC NULLS FIRST,b.id ASC
           ) AS rank_number
    FROM nexmark.bid b
) w
WHERE w.rank_number<=1;
