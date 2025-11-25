/* Query 1
 * 
 * 1. Вывести распределение (количество) клиентов по сферам деятельности,
      отсортировав результат по убыванию количества.
*/
select
	job_industry_category,
	COUNT(customer_id) as num_customers
from
	customer
where
	job_industry_category is not null
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
	select customer_id, o.order_id, order_date, revenues.revenue 
	from orders o
	left join (
		select order_id, quantity * item_list_price_at_sale revenue
		from order_items) as revenues on o.order_id = revenues.order_id 
	where order_status = 'Approved'
)
select
	c.job_industry_category,
	extract(year from aor.order_date) as year,
	extract(month from aor.order_date) as month,	
	TRUNC(sum(aor.revenue)::numeric, 2) revenue
from
	approved_order_revenues aor
left join customer c on
	c.customer_id = aor.customer_id
where
	c.job_industry_category is not null
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
	distinct brand,
	count(order_id) over (partition by brand) num_orders
from
	product_cor pc
left join unique_approved_orders_IT uao on
	uao.product_id = pc.product_id
	and uao.online_order = true
order by num_orders desc
;

/* Query 4
 * 
 * 4. Найти по всем клиентам: сумму всех заказов (общего дохода), максимум, минимум
      и количество заказов, а также среднюю сумму заказа по каждому клиенту.
      Отсортироватьрезультат по убыванию суммы всех заказов и количества заказов. 
      Выполнить двумя способами: используя только GROUP BY 
      и используя только оконные функции. Сравнитьрезультат.
 */


/* Query 5
 * 
 * 5. Найти имена и фамилии клиентов с топ-3 минимальной
      и топ-3 максимальной суммой транзакций за весь период
      (учесть клиентов, у которых нет заказов, приняв их суммутранзакций за 0).
 */

/* Query 6
 * 
 * 6. Вывести только вторые транзакции клиентов (если они есть)
      с помощью оконных функций. Если у клиента меньше двух транзакций,
      он не должен попасть в результат.
 */

/* Query 7
 * 
 * 7. Вывести имена, фамилии и профессии клиентов,
      а также длительность максимального интервала (в днях)
      между двумя последовательными заказами. Исключить клиентов,
      у которых только один или меньше заказов.
 */

/* Query 8
 * 
 * 8. Найти топ-5 клиентов (по общему доходу) в каждом сегменте благосостояния
      (wealth_segment). Вывести имя, фамилию, сегмент и общий доход.
      Если в сегменте менее 5клиентов, вывести всех.
 */