with audit as (

    select * from {{this.schema}}.dbt_audit_log

),

with_id as (

    select 
    
        *,
    
        {{dbt_utils_sqlserver.surrogate_key(
            'event_name', 
            'event_model', 
            'invocation_id'
            )}} as event_id
    
    from audit

)

select * from with_id