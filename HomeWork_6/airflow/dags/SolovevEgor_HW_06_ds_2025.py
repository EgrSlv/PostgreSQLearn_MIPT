from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import sys


def get_sql_query(query, conn):
    with open(f"/opt/airflow/SQL_scripts/{query}") as f:
        sql = f.read()
    cur_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    print(f"Выполняется запрос {query} в {cur_time}")

    hook = PostgresHook(postgres_conn_id=conn)
    df = hook.get_pandas_df(sql)

    df.to_csv(f"/opt/airflow/results/{cur_time}_{query.replace('.sql', '')}.csv",
              index=False,
              encoding="UTF-8")
    if not len(df):
        print("[ERROR] Неверный запрос!", file=sys.stderr)
        return 1
    return 0

postgres_conn_id = "postgresql"
query1 = "query5.sql"
query2 = "query8.sql"

with DAG(
    dag_id="SolovevEgor_HW_05_ds_2025",
    default_args={
        "owner": "Solovev Egor",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Home work 6 tasks",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 12, 19),
    catchup=False,
    tags=["docker", "HomeWork6"],
    on_success_callback=(
        lambda c: print(f"DAG выполнен Успешно!!!")
    ),
    on_failure_callback=(
        lambda c: print(f"Ошибка выполнения DAG!!!", file=sys.stderr)
    )
) as dag:
    create_tables = PostgresOperator(
        task_id="Create_Tables_customer__order_items__orders__product_",
        postgres_conn_id=postgres_conn_id,
        sql="""
                create table if not exists customer (
                    customer_id INT4 primary key
                    ,first_name VARCHAR(128) not null
                    ,last_name VARCHAR(128)
                    ,gender VARCHAR(128) not null
                    ,DOB DATE
                    ,job_title VARCHAR(128)
                    ,job_industry_category VARCHAR(128)
                    ,wealth_segment VARCHAR(128) not null
                    ,deceased_indicator VARCHAR(128) not null
                    ,owns_car VARCHAR(128) not null
                    ,address VARCHAR(128) not null
                    ,postcode VARCHAR(128) not null
                    ,state VARCHAR(128) not null
                    ,country VARCHAR(128) not null
                    ,property_valuation INT2 not null
                );

                create table if not exists order_items (
                    order_item_id INT4 primary key
                    ,order_id INT4 not null
                    ,product_id INT4 not null
                    ,quantity NUMERIC not null
                    ,item_list_price_at_sale FLOAT4 not null
                    ,item_standard_cost_at_sale FLOAT4
                );

                create table if not exists orders (
                    order_id INT4 primary key
                    ,customer_id INT4 not null
                    ,order_date DATE not null
                    ,online_order TEXT
                    ,order_status VARCHAR(128) not null
                );

                create table if not exists product (
                    product_id INT4 not null
                    ,brand VARCHAR(128)
                    ,product_line VARCHAR(128)
                    ,product_class VARCHAR(128)
                    ,product_size VARCHAR(128)
                    ,list_price FLOAT4 not null
                    ,standard_cost FLOAT4
                );
            """
    )
    truncate_tables = PostgresOperator(
        task_id="truncate_tables",
        postgres_conn_id=postgres_conn_id,
        sql="""truncate table customer, order_items, orders, product;"""
    )
    fill_table__customer = PostgresOperator(
        task_id="fill_table__customer",
        postgres_conn_id=postgres_conn_id,
        sql="""
                COPY customer FROM '/opt/airflow/data_files/customer.csv'
                DELIMITER ';' CSV HEADER;
        """
    )
    fill_table__order_items = PostgresOperator(
        task_id="fill_table__order_items",
        postgres_conn_id=postgres_conn_id,
        sql="""
                COPY order_items FROM '/opt/airflow/data_files/order_items.csv'
                DELIMITER ',' CSV HEADER;
        """
    )
    fill_table__orders = PostgresOperator(
        task_id="fill_table__orders",
        postgres_conn_id=postgres_conn_id,
        sql="""
                COPY orders FROM '/opt/airflow/data_files/orders.csv'
                DELIMITER ',' CSV HEADER;
        """
    )
    fill_table__product = PostgresOperator(
        task_id="fill_table__product",
        postgres_conn_id=postgres_conn_id,
        sql="""
                COPY product FROM '/opt/airflow/data_files/product.csv'
                DELIMITER ',' CSV HEADER;
        """
    )
    run_query1 = PythonOperator(
        task_id="run_query1",
        python_callable=get_sql_query,
        op_kwargs={'query': 'query5.sql', 'conn': postgres_conn_id},
    )
    run_query2 = PythonOperator(
        task_id="run_query2",
        python_callable=get_sql_query,
        op_kwargs={'query': 'query8.sql', 'conn': postgres_conn_id},
    )

    fill_tasks = [fill_table__customer, fill_table__order_items,
                  fill_table__orders, fill_table__product]

    create_tables >> truncate_tables >> fill_tasks
    fill_tasks >> run_query1
    fill_tasks >> run_query2
