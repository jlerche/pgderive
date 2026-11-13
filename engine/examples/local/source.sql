CREATE SCHEMA demo;
CREATE SCHEMA demo_engine;
CREATE TABLE demo.auction(id integer PRIMARY KEY, category text);
CREATE TABLE demo.bid(id integer PRIMARY KEY, auction integer, price bigint);
ALTER TABLE demo.auction REPLICA IDENTITY FULL;
ALTER TABLE demo.bid REPLICA IDENTITY FULL;
CREATE PUBLICATION demo_source FOR TABLE demo.auction,demo.bid;
INSERT INTO demo.auction VALUES(1,'books'),(2,'music');
INSERT INTO demo.bid VALUES(1,1,7),(2,1,7),(3,2,NULL);
