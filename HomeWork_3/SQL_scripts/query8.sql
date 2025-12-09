/* Query 8
 * 
 * 8. Найти топ-5 клиентов (по общему доходу) в каждом сегменте благосостояния
      (wealth_segment). Вывести имя, фамилию, сегмент и общий доход.
      Если в сегменте менее 5 клиентов, вывести всех.
*/

with transaction_amount_per_customer as (
	select
		o.customer_id,
		trunc(coalesce(sum(oi.quantity * oi.item_list_price_at_sale), 0)::numeric, 2) as amount
	from
		orders as o
	join order_items as oi on
		oi.order_id = o.order_id
	group by
		o.customer_id
)
select
	*
from
	(
	select
		c.first_name,
		coalesce(c.last_name, 'n\a') as last_name,
		c.wealth_segment,
		coalesce(t.amount, 0) as amount,
		row_number() over(partition by wealth_segment order by amount desc) as rn
	from
		customer c
	left join transaction_amount_per_customer as t on
		t.customer_id = c.customer_id
	where
		amount > 0
) as ranks
where
	rn <= 5
;
