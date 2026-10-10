-- PostgreSQL q22: zero-based SPLIT_INDEX(url,'/',3..5) becomes array[4..6].
-- Absent components yield NULL; present empty components remain empty strings.
SELECT b.auction,b.bidder,b.price,b.channel,
       (pg_catalog.string_to_array(b.url COLLATE pg_catalog."C",'/'))[4] AS dir1,
       (pg_catalog.string_to_array(b.url COLLATE pg_catalog."C",'/'))[5] AS dir2,
       (pg_catalog.string_to_array(b.url COLLATE pg_catalog."C",'/'))[6] AS dir3
FROM nexmark.bid b;
