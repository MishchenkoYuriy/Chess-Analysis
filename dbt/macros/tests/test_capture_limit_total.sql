{% test capture_limit_total(model) %}


with
validation as (

    select * from {{ model }}

),

validation_errors as (

    select game_id, count(capture)
    from validation
    where capture is true
    group by game_id
    having count(capture) > 30
)


select * from validation_errors


{% endtest %}