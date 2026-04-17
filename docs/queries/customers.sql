SELECT 
    c.name,
    a.city,
    a.state,
    ct.email
FROM customers.customers c
JOIN customers.addresses a ON c.customer_id = a.customer_id
JOIN customers.contacts ct ON c.customer_id = ct.customer_id
LIMIT 20