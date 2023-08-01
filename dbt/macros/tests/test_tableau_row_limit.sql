{% test tableau_row_limit( model, column_name ) %}

with
validation as (

    select {{ column_name }} from {{ model }}

),

validation_errors as (

    select count(*)
    from validation
    having count(*)>15000000
)


select * from validation_errors


{% endtest %}