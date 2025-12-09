/* Query 4
 * 
 * 4. Найти по всем клиентам: сумму всех заказов (общего дохода), максимум, минимум
      и количество заказов, а также среднюю сумму заказа по каждому клиенту.
      Отсортировать результат по убыванию суммы всех заказов и количества заказов. 
      Выполнить двумя способами: используя только GROUP BY 
      и используя только оконные функции. Сравнить результат.
*/

with revenue_items as (
	select
		c.customer_id,
		o.order_id,
		trunc(oi.revenue::numeric, 2) as revenue
	from
		customer c
	left join (
		select
			customer_id,
			order_id
		from
			orders
	) o on c.customer_id = o.customer_id
	left join (
		select
			order_id,
			(quantity * item_list_price_at_sale) as revenue
		from
			order_items
	) oi on oi.order_id = o.order_id
),
cte_with_group_by as (
	select
		customer_id,
		sum(revenue) sum_revenue,
		max(revenue) max_revenue,
		min(revenue) min_revenue,
		count(order_id) num_orders,
		trunc(avg(revenue)::numeric, 2) avg_revenue
	from
		revenue_items
	group by
		customer_id
),
cte_with_over as (
	select
		--distinct
		customer_id,
		sum(revenue) over(partition by customer_id) sum_revenue,
		max(revenue) over(partition by customer_id) max_revenue,
		min(revenue) over(partition by customer_id) min_revenue,
		count(order_id) over(partition by customer_id) num_orders,
		trunc(avg(revenue) over(partition by customer_id)::numeric, 2) avg_revenue
	from
		revenue_items
)
select
	*
from
--	cte_with_group_by
	cte_with_over
order by
	sum_revenue desc,
	num_orders desc
;
