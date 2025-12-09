/* Query 3
 * 
 * 3. Вывести количество уникальных онлайн-заказов для всех брендов
      в рамках подтвержденных заказов клиентов из сферы IT.
      Включить бренды, у которых нет онлайн-заказов от IT-клиентов,
      — для них должно быть указано количество 0.
*/

with unique_approved_orders_IT as (
	select
		o.order_id,
		oi.product_id,
		o.online_order,
		o.order_status
	from
		orders o
	join customer c on
		o.customer_id = c.customer_id
	join order_items oi on
		oi.order_id = o.order_id
	where
		c.job_industry_category = 'IT'
		and o.order_status = 'Approved'
)
select
	brand,
	count(order_id) num_orders
from
	product_cor pc
left join unique_approved_orders_IT uao on
	uao.product_id = pc.product_id
	and uao.online_order = true
group by
	brand
order by
	num_orders desc
;
