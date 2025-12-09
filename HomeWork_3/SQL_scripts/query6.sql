/* Query 6
 * 
 * 6. Вывести только вторые транзакции клиентов (если они есть)
      с помощью оконных функций. Если у клиента меньше двух транзакций,
      он не должен попасть в результат.
*/

with only_second_customer_orders as (
	select
		o.order_id,
		c.customer_id,
		lead(o.order_id) over(partition by c.customer_id) as second_order_id,
		row_number() over(partition by c.customer_id) as rn
	from
		customer c
	left join orders as o
			using(customer_id)
)
select
	o.*
from
	orders o
join only_second_customer_orders as osco on
	osco.order_id = o.order_id
where
	second_order_id is not null
	and rn = 1
	-- не забываем, что сделали сдвиг через lead()
order by
	o.order_id
;
