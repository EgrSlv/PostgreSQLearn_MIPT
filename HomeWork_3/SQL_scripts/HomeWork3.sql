/* Query 1
 * 
 * 1. Вывести распределение (количество) клиентов по сферам деятельности,
      отсортировав результат по убыванию количества.
*/
select
	coalesce(job_industry_category, 'n\a') as job_industry_category,
	COUNT(customer_id) as num_customers
from
	customer
group by
	job_industry_category
order by
	num_customers desc
;

/* Query 2
 * 
 * 2. Найти общую сумму дохода (list_price*quantity) по всем подтвержденным заказам
      за каждый месяц по сферам деятельности клиентов. Отсортировать результат по году,
      месяцу и сфере деятельности.
*/
with approved_order_revenues as(
	select
		customer_id,
		o.order_id,
		order_date,
		revenues.revenue
	from
		orders o
	left join (
		select
			order_id,
			quantity * item_list_price_at_sale revenue
		from
			order_items) revenues on
		o.order_id = revenues.order_id
	where
		order_status = 'Approved'
)
select
	coalesce(c.job_industry_category, 'n\a') as job_industry_category,
	extract(year from aor.order_date) as year,
	extract(month from aor.order_date) as month,
	TRUNC(sum(aor.revenue)::numeric, 2) as revenue
from
	approved_order_revenues aor
left join customer c on
	c.customer_id = aor.customer_id
group by
	year,
	month,
	c.job_industry_category
order by
	year,
	month,
	c.job_industry_category
;

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
