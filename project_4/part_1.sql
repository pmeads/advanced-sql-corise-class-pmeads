with 
/* Get a set of all automobile customers with urgent orders */
customers_with_urgent_orders as 
    (

        select 
            orders.o_custkey as customer_key, 
            o_orderdate as order_date,
            o_orderkey as order_key,
            o_totalprice as total_price
        from snowflake_sample_data.TPCH_SF1.customer as customers
        join snowflake_sample_data.TPCH_SF1.orders as orders on orders.o_custkey = customers.c_custkey
        where customers.c_mktsegment = 'AUTOMOBILE'
        and orders.o_orderpriority = '1-URGENT'

    ),
/* From the results above, get the 3 hightest dollar urgent orders */
three_highest_urgent_orders as 
    (
	
        select 
           cust.customer_key,
           cust.total_price,
           cust.order_key
        from customers_with_urgent_orders as cust
        qualify row_number() over (
            partition by cust.customer_key 
            order by cust.total_price desc
        ) <= 3
    
    ),

/* For the report, get the sum of the total spent and put the top 3 
   order numbers in a comma delimited list */
three_highest_urgent_orders_agg as 
    (
        select 
            customer_key,
            sum(total_price) as total_spent,
            listagg(order_key,',') as order_numbers
        from 
            three_highest_urgent_orders
        group by customer_key
    ),
    
/* get the date of the latest urgent order */
customer_last_urgent_order_date as 
    (

        select 
          customer_key,
          max(order_date) as last_order_date
        from customers_with_urgent_orders
        group by customer_key
  
    ),

/* get a base of parts for the customer in order 
   to later retrieve the part key, quantity ordered, and total spent
   for the parts with the top 3 dollar amount spent */
parts_base as 
    (

        select 
            cust.customer_key,
            lineitem.l_partkey as part_key,
            lineitem.l_quantity as quantity,
            lineitem.l_extendedprice as total_price,
            row_number() over (
                partition by cust.customer_key 
                order by lineitem.l_extendedprice desc) 
            as rownum
        from customers_with_urgent_orders as cust
        join snowflake_sample_data.TPCH_SF1.lineitem as lineitem 
            on lineitem.l_orderkey = cust.order_key
     
    ),

/* use the parts_base to extract the part keys and pivot so results 
   can be on a single line per customer */
parts_keys as 
    (

        select *           
        from (
            select 
                customer_key,
                part_key,
                rownum 
            from parts_base
        ) as p
        pivot ( 
            min(p.part_key) for p.rownum in (1,2,3) 
        ) as pivot_values (
            customer_key, 
            part_1_key, 
            part_2_key, 
            part_3_key 
        )
    
    ),

/* now use the parts_base to extract the part quantities and pivot so results 
   can be on a single line per customer */
parts_quantities as 
    (

        select *           
        from (
            select 
                customer_key,
                quantity,
                rownum 
            from parts_base
        ) as p
        pivot ( 
            min(p.quantity) for p.rownum in (1,2,3) 
        ) as pivot_values (
            customer_key, 
            part_1_quantity, 
            part_2_quantity, 
            part_3_quantity 
        )
    
    ),

/* finally use the parts_base to extract the part total_spent and pivot so results 
   can be on a single line per customer */
parts_total_spent as (

        select *           
        from (
            select 
                customer_key,
                total_price as total_spent,
                rownum 
            from parts_base
        ) as p
        pivot ( 
            min(p.total_spent) for p.rownum in (1,2,3) 
        ) as pivot_values (
            customer_key, 
            part_1_total_spent, 
            part_2_total_spent, 
            part_3_total_spent 
        )
    
),

/* put it all together */
final as 
   (

        select 
            customers.customer_key,
            last_urgent_order_date.last_order_date,
            customers.order_numbers,
            customers.total_spent,
            parts_keys.part_1_key,
            parts_quantities.part_1_quantity,
            parts_total_spent.part_1_total_spent,
            parts_keys.part_2_key,
            parts_quantities.part_2_quantity,
            parts_total_spent.part_2_total_spent,
            parts_keys.part_3_key,
            parts_quantities.part_3_quantity,
            parts_total_spent.part_3_total_spent    
        from three_highest_urgent_orders_agg as customers
        join customer_last_urgent_order_date as last_urgent_order_date 
            on last_urgent_order_date.customer_key = customers.customer_key
        join parts_keys 
            on parts_keys.customer_key = customers.customer_key
        join parts_quantities
             on parts_quantities.customer_key = customers.customer_key
        join parts_total_spent
             on parts_total_spent.customer_key = customers.customer_key
        order by last_order_date desc

   )
select * from final
limit 100
