-- PostgreSQL adaptation: explicit C case/equality/regex semantics.
-- URL is an input operand only; empty captures qualify, absent matches do not.
SELECT auction,bidder,price,channel,
       CASE WHEN lower(channel COLLATE pg_catalog."C")='apple' THEN '0'
            WHEN lower(channel COLLATE pg_catalog."C")='google' THEN '1'
            WHEN lower(channel COLLATE pg_catalog."C")='facebook' THEN '2'
            WHEN lower(channel COLLATE pg_catalog."C")='baidu' THEN '3'
            ELSE (regexp_match(url COLLATE pg_catalog."C",'(&|^)channel_id=([^&]*)'))[2]
       END AS channel_id
FROM nexmark.bid
WHERE lower(channel COLLATE pg_catalog."C") IN ('apple','google','facebook','baidu')
   OR (regexp_match(url COLLATE pg_catalog."C",'(&|^)channel_id=([^&]*)'))[2] IS NOT NULL;
