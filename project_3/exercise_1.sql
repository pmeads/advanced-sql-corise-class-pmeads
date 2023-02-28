--ALTER SESSION SET USE_CACHED_RESULT = FALSE; 
--explain
with 

/*
    Import the data we'll need. Get rid of duplicate records. 
    Extract out the event name and recipe for later use
*/
event_data as 
    (
        select --distinct
            event_id,
            session_id,
            event_timestamp,
            json_extract_path_text(event_details,'event') as event_name,
            json_extract_path_text(event_details, 'recipe_id') as recipe_id
        from vk_data.events.website_activity
        group by 1,2,3,4,5
    ),

grouped_sessions as 
    (
        select date(event_timestamp) as event_day,
        	   session_id,
               timestampdiff(second,min(event_timestamp),max(event_timestamp  ) ) as session_length
               --count(session_id) as unique_sessions--,
               --timestampdiff(second,min(event_timestamp),max(event_timestamp  ) ) as session_length
        from event_data
        group by event_day, session_id
        order by 1,2
    ),

agg_results as
    (
        select event_day,
               count(session_id) as unique_sessions,
               avg(session_length) as avg_session_length
        from grouped_sessions
        group by event_day
    ),

/* need to know which sessions have recipe event */
recipe_events as 
    (
        select session_id, recipe_id
        from event_data
        where recipe_id is not null
    ),

/* find avg number of searches completed before displaying a recipe */
search_events2 as 
    (
        select 
           date(event_timestamp) as event_day, 
           event_data.session_id,
           sum(
               case 
                   when event_data.event_name = 'search' 
                       then 1
                       else 0
               end ) as num_search_events
        from event_data
        join recipe_events on recipe_events.session_id = event_data.session_id
        group by 
            event_day,
            event_data.session_id

    ),

recipe_most_viewed as 
    (
        select 
            date(event_timestamp) as event_day,
            recipe_id,
            count(*) recipe_count
        from event_data 
        where recipe_id is not null
        group by 1,2
        qualify row_number() over (partition by event_day order by recipe_count desc) = 1
    ),

final as 
    (
        --select * from agg_results
        select  
            agg_results.event_day,
            agg_results.unique_sessions,
            agg_results.avg_session_length,
            recipe_most_viewed.recipe_id as most_popular_recipe_id,
            avg(num_search_events) as avg_num_search_events
        from search_events2
        join agg_results on agg_results.event_day = search_events2.event_day
        join recipe_most_viewed on recipe_most_viewed.event_day = search_events2.event_day
        group by 
            agg_results.event_day,
            agg_results.unique_sessions,
            agg_results.avg_session_length,
            recipe_most_viewed.recipe_id
    )

select * from final
