-- Run only in a newly owned disposable demonstration database.
CREATE SCHEMA demo_ivm;
CREATE SCHEMA demo_topk;
REVOKE ALL ON SCHEMA demo_ivm,demo_topk FROM PUBLIC;
CREATE TABLE public.demo_positions(id bigint PRIMARY KEY,book_id bigint NOT NULL,quantity bigint);
ALTER TABLE public.demo_positions REPLICA IDENTITY FULL;
CREATE PUBLICATION pgderive_demo_sources FOR TABLE public.demo_positions;
