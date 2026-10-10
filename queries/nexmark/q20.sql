-- PostgreSQL Nexmark q20; native snake_case names and explicit output labels.
SELECT b.auction,b.bidder,b.price,b.channel,b.url,
       b.date_time AS bid_date_time,b.extra AS bid_extra,
       a.item_name,a.description,a.initial_bid,a.reserve,
       a.date_time AS auction_date_time,a.expires,a.seller,a.category,
       a.extra AS auction_extra
FROM nexmark.bid b INNER JOIN nexmark.auction a ON b.auction=a.id
WHERE a.category=10;
