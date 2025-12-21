/* Query 5
 * 
 * 5. Найти имена и фамилии клиентов с топ-3 минимальной
      и топ-3 максимальной суммой транзакций за весь период
      (учесть клиентов, у которых нет заказов, приняв их сумму транзакций за 0).
*/

with transaction_amount_per_customer as (
	select
		o.customer_id,
		trunc(coalesce(sum(oi.quantity * oi.item_list_price_at_sale), 0)::numeric, 2) as amount
	from
		orders as o
	join order_items as oi on oi.order_id = o.order_id
	group by
		o.customer_id
),
transactions_rank as (
	select
		customer_id,
		amount,
		rank() over(order by amount asc) as top3_min_amount,
		rank() over(order by amount desc) as top3_max_amount
	from
		transaction_amount_per_customer
)
select
	c.first_name,
	c.last_name,
	coalesce(tr.amount, 0) as amount,
	coalesce(top3_min_amount, 0) as top3_min_amount,
	coalesce(top3_max_amount, 0) as top3_max_amount
from
	transactions_rank tr
right join (	select customer_id, first_name, coalesce(last_name, 'n\a') as last_name
			from customer) c on c.customer_id = tr.customer_id
where tr.top3_min_amount <=3 or tr.top3_max_amount <=3
;
