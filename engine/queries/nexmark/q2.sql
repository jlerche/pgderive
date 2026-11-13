-- PostgreSQL Nexmark q2: NULL auction does not qualify; signed remainder is native.
SELECT auction,price FROM nexmark.bid WHERE pg_catalog.mod(auction,123)=0;
