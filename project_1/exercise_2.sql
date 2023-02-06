/*

    The challenge query from project 1 ended up being quite challenging!! 
    I think I was able to provide an answer for the first part of the second 
    question, but I am really stumped at the moment on how to get a good recipe, 
    just one, for the first food pref. I want it to be a random sample rather 
    than just returning the min or such for something like “eggs” and have not 
    figured out how to do that just yet.  I haven't used any hints yet or asked 
    the TA for help just yet. I was hoping to spend a little more time monday on it

*/

with 

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
valid_customers as (

    select distinct 
        c.customer_id, 
        c.email,
        c.first_name
    
    from vk_data.customers.customer_data as c
    join vk_data.customers.customer_address as a
        on c.customer_id = a.customer_id
    join us_cities as ci on (
         ci.city_name = upper(a.customer_city) 
         and ci.state_abbr = a.customer_state
    )
    
),

customer_food_preferences as (
    
    select 
        *
    from (
        
        select 
            c.customer_id,
            c.email,
            c.first_name,
            cs.tag_id,
            replace(rt.tag_property,' ','') as food_preference,
            row_number() over (partition by c.customer_id order by rt.tag_property) as rownum
        from valid_customers as c
        join vk_data.customers.customer_survey as cs 
            on cs.customer_id = c.customer_id
        join vk_data.resources.recipe_tags as rt
            on cs.tag_id = rt.tag_id
        where cs.is_active = TRUE
        order by c.customer_id, food_preference
        
    
    ) 
    where rownum <= 3

),

recipe_ingredients as (

    select 
        recipe_id, 
        recipe_name,
        ltrim(rtrim(flat_ingredients.value)) as ingredient
    from vk_data.chefs.recipe,
    table(flatten(ingredients)) as flat_ingredients
    --limit 1

),

t1 as (

    select 
        ingredient, 
        count(distinct recipe_id) number_distinct_recipies
    from recipe_ingredients
    group by ingredient

),

test1 as (
    
    select 
        cfp.customer_id,
        cfp.email,
        cfp.first_name,
        cfp.food_preference,
        cfp.rownum,
        ri.recipe_name
    from customer_food_preferences as cfp
    left join recipe_ingredients as ri 
        on (cfp.food_preference = ri.ingredient 
            and cfp.rownum = 1         
           )
    order by cfp.customer_id, cfp.food_preference


),

final as (
    
    select * from test1

    
)

select * from final
