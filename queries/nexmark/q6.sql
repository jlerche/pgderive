SELECT recent.seller,AVG(recent.final_price) AS avg_price
FROM (
  SELECT ranked.seller,ranked.final_price
  FROM (
    SELECT closed.auction_id,closed.seller,closed.expires,closed.final_price,
      ROW_NUMBER() OVER (PARTITION BY closed.seller
        ORDER BY closed.expires DESC,closed.auction_id DESC) AS position
    FROM (
      SELECT eligible.auction_id,eligible.seller,eligible.expires,
        MAX(eligible.price) AS final_price
      FROM (
        SELECT e.auction_id,e.seller,e.expires,e.price
        FROM (
          SELECT a.id AS auction_id,a.seller,a.expires,b.price
          FROM nexmark.auction a JOIN nexmark.bid b ON a.id=b.auction
          WHERE b.date_time>=a.date_time AND b.date_time<=a.expires
        ) e
        CROSS JOIN (
          SELECT recorded_time FROM nexmark.global_clock WHERE id=0
        ) c
        WHERE e.expires<=c.recorded_time
      ) eligible
      GROUP BY eligible.auction_id,eligible.seller,eligible.expires
    ) closed
  ) ranked
  WHERE ranked.position<=10
) recent
GROUP BY recent.seller;
