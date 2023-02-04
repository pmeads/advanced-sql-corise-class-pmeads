/********

I wanted to weed out the invalid customers first so I compared the 
city/state the customers inputed to our us_cities table. This will 
reduce the dataset so the rest of the query has less records to deal 
with and doesn't have to work as hard.

While I was there, comparing the city/state, I might as well get the 
lat/long info for the city at the same time. This will produce some 
duplicates but as it's not that many and doesn't hurt us, we'll just 
ignore them. the duplicates will be eliminated later. 

next, i needed the lat/long data for the supplier 

lastly, i used the st_distance and st_makepoint functions supplied 
with snowflake to calculate the distance of a customer to all 10 suppliers. 
because there are only 10 suppliers, a cartesian join won't be too damaging
here and will let us go through all suppliers for each customer

to get the closest supplier to a customer, i used the row_number() window 
function to create a window partioned by a customer and ordered by the 
distance. The closest supplier will be the first row which i can get by 
limiting where the row number = 1

We are implementing DBT at work and CTEs are widely used in DBT. 
This SQL is influced by DBT best practices. 

*********/

with 

---------------
-- IMPORT CTEs
---------------

-- Load valid cities/state data. Use to later weed out user entered
-- address info that doesn't match a real city/state combo
us_cities as (
    
    select 
        city_name,
        state_abbr,
        lat,
        long    
    from vk_data.resources.us_cities
    
),

-- Determine which customer addresses are good by comparing them to 
-- our us_cities data. At the same time, we'll save the lat/long data for later
valid_customer_addresses as (

-- Determine which customer addresses are good by comparing them to 
-- our us_cities data. At the same time, we'll save the lat/long data for later
valid_customer_addresses as (

    select 
        a.customer_id,
         upper(a.customer_city) as customer_city_upper,
        a.customer_state as customer_state_abbr,
        ci.lat,
        ci.long
    from vk_data.customers.customer_address as a
    join us_cities as ci on (
             ci.city_name = upper(a.customer_city) 
         and ci.state_abbr = a.customer_state
    )
    qualify row_number() over (partition by a.customer_id order by ci.lat) = 1
    
),

-- now that we weeded out the bad customer data, get the rest of the customer info
-- and include the lat/long for their city/state. 
customers as (

    select 
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        ca.customer_city_upper,
        ca.customer_state_abbr,
        ca.lat,
        ca.long
    from vk_data.customers.customer_data as cd
    join valid_customer_addresses as ca on ca.customer_id = cd.customer_id
    
),

-- get the lat/long for our suppliers from the us_cities data
suppliers as (
    
    select 
        si.*, 
        usc.lat, 
        usc.long
    from vk_data.suppliers.supplier_info as si
    join  vk_data.resources.us_cities as usc on (
            usc.city_name = upper(si.supplier_city) and 
            usc.state_abbr = si.supplier_state
    )
),

---------------
-- LOGICAL CTEs
---------------

-- this is a rare intentional cartesian product.
-- for each customer, we want to run through all
-- ten suppliers to determine which would be closest
-- to that customer
customer_supplier_distances as (
    
    select 
        cust.*,
        supl.*,
        st_distance(
            st_makepoint(cust.long, cust.lat), 
            st_makepoint(supl.long,supl.lat)
        ) as distance_from_customer
    from customers as cust 
    join suppliers as supl 

),

-- Determine which supplier is closest to a customer
customer_supplier as (  

    select 
    
        -- limit to the fields requested
        customer_id,
        first_name,
        last_name,
        email,
        supplier_id,
        supplier_name,
        round(distance_from_customer / 1609) as miles_from_customer
    
    from (
        
        -- use an inline view and apply a windowing function.
        -- the window of the row_number() function in the view will be the customer_id
        -- and that window will contain all the supplier distances for a customer.
        -- we'll order the window by distance_from_customer so we can select the 
        -- first result, the closest supplier, in the parent query
        select 
        
            *,
            row_number() over (
                partition by customer_id 
                order by distance_from_customer 
            ) as rownum
        
        from  customer_supplier_distances
        
    )
    
    where rownum = 1 -- ordered the window by distance_from_customer 
                     -- so the first row will be the closest supplier
    
),

------------
-- FINAL CTE
------------

final as (
    
    select 
        *
    from customer_supplier as cs
    order by 
        cs.last_name, 
        cs.first_name

)

select * from final 
;

