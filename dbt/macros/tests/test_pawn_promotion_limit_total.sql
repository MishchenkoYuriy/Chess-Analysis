{% test pawn_promotion_limit_total(model) %}

with
validation as (

    select * from {{ model }}

),

validation_errors as (

    select game_id, count(pawn_promotion)
    from validation
    where pawn_promotion <> 0
    group by game_id
    having count(pawn_promotion) > 16
)


select * from validation_errors


{% endtest %}