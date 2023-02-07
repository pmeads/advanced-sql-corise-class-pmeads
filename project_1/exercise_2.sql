/*
   approach: 
   1) get valid customers using address matched against us_cities table
   2) get the customers food preferences and limit to first 3 using row_Number() window function
   3) pivot results to create a column for each food preference
   4) use min to get recipe ingredients by flattening recipe.tag_list column (first tried flattening ingredients column but that didn't work)
   5) join back to customer food preference results , use any_value to get once matching recipe
   6) order results by email
*/

with 

-- Load valid cities/state data. Use to later weed out user entered
-- address info that doesn't match a real city/state combo
us_cities as (
    
    select 
        upper(trim(city_name)) as city_name,
        upper(trim(state_abbr)) as state_abbr,
        lat,
        long    
    from vk_data.resources.us_cities
    
),

-- Determine which customer id are good by comparing addresses to 
-- our us_cities data. 
valid_customer_ids as (

    select distinct
        a.customer_id
    from vk_data.customers.customer_address as a
    join us_cities as ci on (
         ci.city_name = upper(trim(a.customer_city))
         and ci.state_abbr = upper(trim(a.customer_state))
    )

),

-- get the rest of the customer data we were asked to return
customers as (
    
     select 
         c.customer_id,
         c.first_name,
         c.email
     from vk_data.customers.customer_data as c
     join valid_customer_ids as vc
        on vc.customer_id = c.customer_id
    
),

-- from the survey, get the first 3 food prefs per customer
customer_food_preferences as (
 
    select
        customer_id,
        food_preference,
        rownum
    from (

        select
            c.customer_id,
            trim(replace(rt.tag_property, '"', '')) as food_preference,   
            row_number() over (partition by c.customer_id order by rt.tag_property) as rownum
        from valid_customer_ids as c
        join vk_data.customers.customer_survey as cs
            on cs.customer_id = c.customer_id
        join vk_data.resources.recipe_tags as rt
            on cs.tag_id = rt.tag_id
        where cs.is_active = TRUE
        order by c.customer_id, food_preference

    )
    where rownum <= 3

),

-- rotate the results so the 3 food prefs are columns
customer_food_preferences_pivot as (

    select *
    from customer_food_preferences
    pivot ( min(food_preference) for rownum in (1,2,3) ) 
           as pivot_values (
               customer_id, 
               food_preference1, 
               food_preference2, 
               food_preference3
           )

),

-- it seems like this should be where we match ingredients but it is in the tag_list instead
recipe_ingredients_does_not_work as (

    select
        recipe_id,
        recipe_name,
        trim(replace(flat_ingredients.value, '"', '')) as ingredient
    from vk_data.chefs.recipe,
    table(flatten(ingredients)) as flat_ingredients

),

-- get all ingredients per recipe
recipe_ingredients as (

    select 
        recipe_id,
        recipe_name,
        trim(replace(flat_tag.value, '"', '')) as ingredient
    from vk_data.chefs.recipe
    , table(flatten(tag_list)) as flat_tag

),

-- find a recipe for food_preference1
customer_food_preferences_with_recipe as (
    
    select
        cfpp.customer_id,
        cfpp.food_preference1,
        cfpp.food_preference2,
        cfpp.food_preference3,
        any_value(ri.recipe_name) as recipe_name  
    from customer_food_preferences_pivot as cfpp
    left outer join recipe_ingredients as ri
        on cfpp.food_preference1 = ri.ingredient
    group by 
        cfpp.customer_id,
        cfpp.food_preference1,
        cfpp.food_preference2,
        cfpp.food_preference3

),

-- put it all together and sort
final as (
    
    select
        c.customer_id,
        c.first_name,
        c.email,
        cfpr.food_preference1,
        cfpr.food_preference2,
        cfpr.food_preference3,
        cfpr.recipe_name     
    from customers as c
    join customer_food_preferences_with_recipe as cfpr
        on cfpr.customer_id = c.customer_id
    order by
        c.email
    
)

select * from final
