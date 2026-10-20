SELECT a.id AS auction_id, COUNT(*) AS bid_count, SUM(b.price) AS total
FROM demo.auction a JOIN demo.bid b ON a.id=b.auction
GROUP BY a.id
