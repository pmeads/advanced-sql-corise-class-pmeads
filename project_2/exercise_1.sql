/*
   Caputure Virtual Kitchen customers who ordered meal kits 
   that mistakenly did not contain frsh parsley, and how 
   far they are from the grocery stores that have agreed to 
   help us mitigate this formidable food fiasco. 
*/

with 

/* get the point coordinates for Chicago */
chicago as
    ( 
        
        select geo_location
        from vk_data.resources.us_cities 
        where city_name = 'CHICAGO' 
        and state_abbr = 'IL'
        
    ),

/* get the point coordinates for Gary, IN */
gary as 
    (
        
        select geo_location
        from vk_data.resources.us_cities 
        where city_name = 'GARY' 
        and state_abbr = 'IN'
        
    ),

/* Our customers are located in the following States/cities: 
   KY: Concord, Ashland, Georgetown
   CA: Oakland, Pleasant Hill
   TX: Arlington, Brownsville

   We need to get the distance between them and the two stores that 
   are helping us 
*/
customer_addresses as 
    (
        
        select
            customer_address.*,
            (st_distance(us.geo_location, chicago.geo_location) / 1609)::int as chicago_distance_miles,
            (st_distance(us.geo_location, gary.geo_location) / 1609)::int as gary_distance_miles
            
        from vk_data.customers.customer_address
        join vk_data.resources.us_cities us 
            on upper(rtrim(ltrim(customer_state))) = upper(trim(us.state_abbr))
            and trim(lower(customer_city)) = trim(lower(us.city_name))
        cross join chicago 
        cross join gary 
        where 
        (
            ( customer_state = 'KY' )
            and 
            (
                trim(lower(customer_city)) ilike '%concord%' 
                or trim(lower(customer_city)) ilike '%geogetown%'
                or trim(lower(customer_city)) ilike '%ashland%'
            )

        )
        or
        (
            ( customer_state = 'CA' ) 
                and
            (
                trim(lower(customer_city)) ilike '%oakland%'
                or trim(lower(customer_city)) ilike '%pleasant hill%'
            )
        )
        or
        (
            ( customer_state = 'TX' ) 
                and
            (
                trim(lower(customer_city)) ilike '%arlington%'
                or trim(lower(customer_city)) ilike '%brownsville%'
            )
        )        
    ),

/* just need the customer name from the customer_data table */
customers as 
   (

        select 
            c.customer_id,
            first_name || ' ' || last_name as customer_name
        from vk_data.customers.customer_data as c
        join customer_addresses as ca on c.customer_id = ca.customer_id
    
    ),

customer_food_pref_count as 
    (
        
        select 
            customer_id,
            count(*) as food_pref_count
        from vk_data.customers.customer_survey
        where is_active = true
        group by customer_id
        
    ),

    
final as 
    (
        
        select 
           c.customer_name,
           ca.customer_city,
           ca.customer_state,
           cfpc.food_pref_count,
           chicago_distance_miles,
           gary_distance_miles        
        from customer_addresses as ca
        join customers as c on ca.customer_id = c.customer_id
        join customer_food_pref_count as cfpc on ca.customer_id = cfpc.customer_id
        order by customer_name
        
    )
    
select * from final
