-- Run only inside this explicitly owned synthetic demonstration database.
CREATE SCHEMA demo_ivm;
CREATE SCHEMA demo_topk;
REVOKE ALL ON SCHEMA demo_ivm,demo_topk FROM PUBLIC;
CREATE TABLE public.demo_products(product_id bigint PRIMARY KEY,name text COLLATE pg_catalog."C" NOT NULL,category text COLLATE pg_catalog."C" NOT NULL);
CREATE TABLE public.demo_order_lines(line_id bigint PRIMARY KEY,product_id bigint NOT NULL,quantity bigint NOT NULL,unit_price_cents bigint NOT NULL);
ALTER TABLE public.demo_products REPLICA IDENTITY FULL;
ALTER TABLE public.demo_order_lines REPLICA IDENTITY FULL;
CREATE PUBLICATION pgderive_demo_sources FOR TABLE public.demo_products,public.demo_order_lines;
