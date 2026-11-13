-- PostgreSQL q9: inclusive recorded bid-time eligibility.
-- PostgreSQL DESC puts NULL prices first; ascending bid times put NULL last.
-- Stable native bid/auction primary keys explicitly resolve otherwise equal ties.
-- This revisable MV has no implicit wall clock or auction-closure condition.
SELECT w.auction_id,w.item_name,w.description,w.initial_bid,w.reserve,
       w.date_time,w.expires,w.seller,w.category,w.auction_extra,
       w.auction,w.bidder,w.price,w.bid_date_time,w.bid_extra
FROM (
    SELECT j.auction_id,j.item_name,j.description,j.initial_bid,j.reserve,
           j.date_time,j.expires,j.seller,j.category,j.auction_extra,
           j.auction,j.bidder,j.price,j.bid_date_time,j.bid_extra,
           row_number() OVER (
               PARTITION BY j.auction_id
               ORDER BY j.price DESC NULLS FIRST,j.bid_date_time ASC NULLS LAST,
                        j.bid_id,j.auction_id
           ) AS rownum
    FROM (
        SELECT a.id AS auction_id,a.item_name,a.description,a.initial_bid,a.reserve,
               a.date_time,a.expires,a.seller,a.category,a.extra AS auction_extra,
               b.id AS bid_id,b.auction,b.bidder,b.price,
               b.date_time AS bid_date_time,b.extra AS bid_extra
        FROM nexmark.auction a JOIN nexmark.bid b ON a.id=b.auction
        WHERE b.date_time>=a.date_time AND b.date_time<=a.expires
    ) j
) w
WHERE w.rownum<=1;
