/* Query 7
 * 
 * 7. Вывести имена, фамилии и профессии клиентов,
      а также длительность максимального интервала (в днях)
      между двумя последовательными заказами. Исключить клиентов,
      у которых только один или меньше заказов.
*/

with calc_intervals as (
	select
		o.order_id,
		c.first_name,
		c.last_name,
		c.job_title,
		count(o.order_id) over(partition by o.customer_id) as num_orders,
		lead(order_date) over(partition by o.customer_id order by order_date) - order_date as days
	from
		customer c
	join orders o on
		c.customer_id = o.customer_id
)
select
	first_name,
	coalesce(last_name, 'n\a') as last_name,
	coalesce(job_title, 'n\a') as job_title,
	max(days) as max_interval_days
	--	,max(num_orders) as num_orders
from
	calc_intervals
where
	num_orders > 1
group by
	first_name,
	last_name,
	job_title
order by
	max(days) desc 
;
