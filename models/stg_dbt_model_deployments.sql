with events as (

    select * from {{ref('stg_dbt_audit_log')}}

),

aggregated as (

    select 
    
        {{dbt_utils_sqlserver.surrogate_key(
            'event_model', 
            'invocation_id'
            )}} as model_deployment_id,
    
        invocation_id,
        event_model as model,
    
        min(case 
            when event_name = 'model deployment started' then event_timestamp 
            end) as deployment_started_at,
    
        min(case 
            when event_name = 'model deployment completed' then event_timestamp 
            end) as deployment_completed_at
    
    from events
    where lower(event_name) like lower('%model%')
    group by {{dbt_utils_sqlserver.surrogate_key(
            'event_model', 
            'invocation_id'
            )}},
            invocation_id,
            event_model

)

select * from aggregated