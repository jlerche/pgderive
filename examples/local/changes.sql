BEGIN;
UPDATE demo.auction SET category='updated' WHERE id=1;
UPDATE demo.bid SET auction=2,price=9 WHERE id=2;
INSERT INTO demo.bid VALUES(4,1,7);
DELETE FROM demo.bid WHERE id=3;
COMMIT;
