-- PostgreSQL q4: inclusive recorded bid-time eligibility, per-auction MAX,
-- then exact PostgreSQL numeric AVG of non-NULL winning prices by category.
SELECT q.category,AVG(q.final_price) AS average_price
FROM (
    SELECT j.auction_id,j.category,MAX(j.price) AS final_price
    FROM (
        SELECT a.id AS auction_id,a.category,b.price
        FROM nexmark.auction a JOIN nexmark.bid b ON a.id=b.auction
        WHERE b.date_time>=a.date_time AND b.date_time<=a.expires
    ) j
    GROUP BY j.auction_id,j.category
) q
GROUP BY q.category;
