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
