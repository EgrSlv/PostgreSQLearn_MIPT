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
