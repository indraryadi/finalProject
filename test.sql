SELECT  DISTINCT products .name, SUM(orders .num_of_item) as total_item_buyed
from raw_orders as orders 
join raw_order_items as order_items
on orders.user_id = order_items.user_id 
JOIN raw_prodicts as products 
on order_items.product_id = products.id
GROUP BY products.name 
ORDER BY total_item_buyed DESC 
limit 10

-- 
SELECT  DISTINCT products .name, SUM(orders .num_of_item) as total_item_buyed
from raw_orders as orders 
join raw_order_items as order_items
on orders.user_id = order_items.user_id 
JOIN raw_prodicts as products 
on order_items.product_id = products.id
GROUP BY products.name 
ORDER BY total_item_buyed DESC 
limit 10

select raw_orders.user_id,raw_orders.order_id, raw_prodicts.name, raw_orders.num_of_item  
from raw_orders
join raw_order_items
on raw_orders.user_id = raw_order_items.user_id 
join raw_prodicts
on raw_order_items.product_id = raw_prodicts.id
WHERE raw_prodicts.name = "Wrangler Men's Premium Performance Cowboy Cut Jean"