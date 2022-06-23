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


select roi.product_id ,roi.created_at ,count(roi.product_id) as tot_item 
from rawdataorder roi
where roi.status ='Complete'
GROUP by (roi.created_at,roi.product_id)
ORDER by roi.product_id 
--
--select roi.product_id ,ds.id as status_id, roi.created_at  
--from rawdataorder roi
--join dim_status ds 
--on roi.status = ds.status_name 
--where roi.status ='Complete'
--ORDER by roi.product_id 

select roi.product_id ,substring(roi.created_at,1,7) as month ,roi.status  ,count(roi.product_id) as tot_item 
from rawdataorder roi
where roi.status ='Complete'
GROUP by (month,roi.product_id,roi.status)
ORDER by roi.product_id 
--
--select roi.product_id ,ds.id as status_id, roi.created_at  
--from rawdataorder roi
--join dim_status ds 
--on roi.status = ds.status_name 
--where roi.status ='Complete'
--ORDER by roi.product_id 

select roi.product_id ,substring(roi.created_at,1,7) as month ,ds.id as status_id ,count(roi.product_id) as tot_item 
from rawdataorder roi
join dim_status ds 
on roi.status = ds.status_name 
where roi.status ='Complete'
GROUP by (month,roi.product_id,status_id)
ORDER by roi.product_id 
--
--select roi.product_id ,ds.id as status_id, roi.created_at  
--from rawdataorder roi
--join dim_status ds 
--on roi.status = ds.status_name 
--where roi.status ='Complete'
--ORDER by roi.product_id 



-- +++++++++++++++++++++++++
select roi.product_id ,
	   substring(roi.created_at,1,7) as month ,
	   ds.id as status_id ,
	   count(roi.product_id) as tot_item,
	   sum(roi.sale_price) as tot_value
from rawdataorder roi
join dim_status ds 
on roi.status = ds.status_name 
where roi.status ='Complete'
GROUP by (month,roi.product_id,status_id)
ORDER by roi.product_id,month
--

	select * from rawdataorder r where product_id =4
--select roi.product_id ,ds.id as status_id, roi.created_at  
--from rawdataorder roi
--join dim_status ds 
--on roi.status = ds.status_name 
--where roi.status ='Complete'
--ORDER by roi.product_id 