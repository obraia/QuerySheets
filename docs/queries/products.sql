SELECT 
    p.product_name,
    c.category_name,
    p.price
FROM products.products p
JOIN products.categories c ON p.category_id = c.category_id
LIMIT 20