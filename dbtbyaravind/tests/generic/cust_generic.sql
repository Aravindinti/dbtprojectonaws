{% test is_pk_negative (model, column_name) %}

with validation as (

    select
        {{ column_name }} as pk_field

    from {{ model }}

),

validation_errors as (

    select
        pk_field

    from validation
    -- if this is true, then even_field is actually odd!
    where pk <0

)

select *
from validation_errors

{% endtest %}