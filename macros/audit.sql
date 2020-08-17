{% macro get_audit_relation() %}
    {%- set audit_table = 
        api.Relation.create(
            identifier='dbt_audit_log', 
            schema=target.schema~'_meta', 
            type='table'
        ) -%}
    {{ return(audit_table) }}
{% endmacro %}


{% macro get_audit_schema() %}
    {% set audit_table = logging.get_audit_relation() %}
    {{ return(audit_table.include(schema=True, identifier=False)) }}    
{% endmacro %}


{% macro log_audit_event(event_name, schema, relation) %}

    insert into {{ logging.get_audit_relation() }} (
        event_name, 
        event_timestamp_utc, 
        event_timestamp, 
        event_schema, 
        event_model,
        invocation_id
        ) 
    
    values (
        '{{ event_name }}', 
        {{dbt_utils_sqlserver.current_timestamp_in_utc()}}, 
        {{dbt_utils_sqlserver.current_timestamp()}}, 
        {% if variable != None %}'{{ schema }}'{% else %}null::varchar(512){% endif %}, 
        {% if variable != None %}'{{ relation }}'{% else %}null::varchar(512){% endif %}, 
        '{{ invocation_id }}'
        )

{% endmacro %}


{% macro create_audit_schema() %}
    IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name =  {{ logging.get_audit_schema() | replace('"', "'") }} )
    EXEC('CREATE SCHEMA {{ logging.get_audit_schema() | replace('"', "") }}');
{% endmacro %}


{% macro create_audit_log_table() %}
    IF NOT EXISTS (
                SELECT 1
           FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
           WHERE s.[name] = '{{ logging.get_audit_schema() | replace('"', "") }}'
             AND t.name = '{{ logging.get_audit_relation().identifier }}'
             AND t.type = 'U')
    EXEC('CREATE TABLE {{ logging.get_audit_relation() | replace('"', "") }} 
            (
       event_name       varchar(512),
       event_timestamp_utc  {{dbt_utils_sqlserver.type_timestamp()}},
       event_timestamp  {{dbt_utils_sqlserver.type_timestamp()}},
       event_schema     varchar(512),
       event_model      varchar(512),
       invocation_id    varchar(512)
    )
    ');

{% endmacro %}


{% macro log_run_start_event() %}
    {{logging.log_audit_event('run started')}}
{% endmacro %}


{% macro log_run_end_event() %}
    {{logging.log_audit_event('run completed')}}
{% endmacro %}


{% macro log_model_start_event() %}
    {{logging.log_audit_event(
        'model deployment started', this.schema, this.name
        )}}
{% endmacro %}


{% macro log_model_end_event() %}
    {{logging.log_audit_event(
        'model deployment completed', this.schema, this.name
        )}}
{% endmacro %}