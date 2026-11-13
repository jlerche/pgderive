-- PostgreSQL formulation of Nexmark q3: local item suggestion.
SELECT p.name,p.city,p.state,a.id
FROM nexmark.auction a INNER JOIN nexmark.person p ON a.seller=p.id
WHERE a.category=10 AND (p.state='OR' OR p.state='ID' OR p.state='CA');
