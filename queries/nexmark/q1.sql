-- Exact PostgreSQL numeric conversion, with an integral native price input.
SELECT auction,bidder,0.908 * price AS price,date_time,extra
FROM nexmark.bid;
