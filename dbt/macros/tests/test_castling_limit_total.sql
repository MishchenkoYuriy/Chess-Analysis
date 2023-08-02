{% test castling_limit_total(model) %}


with
validation as (

    select * from {{ model }}

),

validation_errors as (

    select game_id, count(castling)
    from validation
    where castling <> 0
    group by game_id
    having count(castling) > 2
)


select * from validation_errors


{% endtest %}