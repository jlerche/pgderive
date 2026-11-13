-- Recorded processing-time predecessor; same-time ties use stable side identity.
-- Event date_time remains payload and never substitutes for processing_time.
SELECT b.auction,b.bidder,b.price,b.date_time,p.value
FROM nexmark.bid b
LEFT JOIN LATERAL (
    SELECT s.value
    FROM nexmark.side_input s
    WHERE s.key=mod(b.auction,10000) AND s.date_time<=b.processing_time
    ORDER BY s.date_time DESC,s.id DESC
    LIMIT 1
) p ON true;
