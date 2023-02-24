-- 178 unique sessions
ALTER SESSION SET USE_CACHED_RESULT = FALSE; 
with 

/*
    Import the data we'll need. Get rid of duplicate records. 
    Extract out the event name and recipe for later use
*/
event_data as 
    (
        select distinct
            event_id,
            session_id,
            user_id,
            event_timestamp,
            json_extract_path_text(event_details,'event') as event_name,
            json_extract_path_text(event_details, 'recipe_id') as recipe_id
        from vk_data.events.website_activity
    ),

/* need to know which sessions have recipe event */
recipe_events as 
    (
        select session_id, recipe_id
        from event_data
        where recipe_id is not null
    ),

/* 
  of the sessions that actually got to a recipe, how many searches preceded the recipe? 
  This correlated subquery was the most expensive operation causing 33% of the time. 
  I changed it to join to a previous CTE which seemed to remove the expensive operation
  and improve the performance. the original version is in the commented out CTE
  "search_events1". The new version is "search_events2"
*/

/*
search_events1 as
    (
        select
           event_data.session_id,
           sum(
               case 
                   when event_data.event_name = 'search' 
                       then 1
                       else 0
               end ) as num_search_events
        from event_data
        where exists (
            select 1 from event_data as ed 
            where ed.session_id = event_data.session_id
            and recipe_id is not null
        )
        group by event_data.session_id
    ),
*/

search_events2 as 
    (
        select 
           event_data.session_id,
           sum(
               case 
                   when event_data.event_name = 'search' 
                       then 1
                       else 0
               end ) as num_search_events
        from event_data
        join recipe_events on recipe_events.session_id = event_data.session_id
        group by event_data.session_id

    ),

recipe_most_viewed as 
    (
        select 
            recipe_id,
            count(*) recipe_count
        from recipe_events 
        where recipe_id is not null
        group by recipe_id
        order by recipe_count desc
        limit 1
    ),
/*
  had an uncecessary order by 
*/
avg_session_length as
    (
        
        select 
            activity.session_id,
            count(1) as session_count,
            min(activity.event_timestamp) as session_start,
            max(activity.event_timestamp) as session_end,
            timestampdiff(second,min(activity.event_timestamp),max(activity.event_timestamp  ) ) as session_length
            
        from event_data as activity
        group by activity.session_id
        --order by activity.session_id
        
    ),

final as 
    (
        select 
            'unique_sessions' as result_name,
            count(avg_session_length.session_id)::varchar as result_value 
        from avg_session_length
        
        union all
        
        select
            'avg_session_length_in_seconds' as result_name,
            avg(session_length)::varchar as result_value
        from avg_session_length

        union all

        select 
            'avg_number_of_search_events' as result_name, 
            avg(num_search_events)::varchar as result_value
        --from search_events1
        from search_events2

        union all
        
        select 
            'most_viewed_recipe' as result_name,
            min(recipe_id) as result_value
        from recipe_most_viewed
        
    )

select * from final
--select * from search_events1
--select * from search_events2
--select * from recipe_most_viewed

