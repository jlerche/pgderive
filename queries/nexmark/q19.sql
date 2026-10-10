-- PostgreSQL q19: top ten occurrences per auction, including their row numbers.
-- Explicit time/ID tie-breakers settle otherwise unspecified equal-price order.
-- The source-only ID is omitted from the visible Nexmark payload.
SELECT w.auction,w.bidder,w.price,w.channel,w.url,w.date_time,w.extra,w.rank_number
FROM (
    SELECT b.id,b.auction,b.bidder,b.price,b.channel,b.url,b.date_time,b.extra,
           row_number() OVER (
               PARTITION BY b.auction
               ORDER BY b.price DESC NULLS FIRST,b.date_time DESC NULLS FIRST,b.id ASC
           ) AS rank_number
    FROM nexmark.bid b
) w
WHERE w.rank_number<=10;
