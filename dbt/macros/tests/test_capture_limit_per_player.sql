{% test capture_limit_per_player(model) %}


with
validation as (

    select * from {{ model }}

),

validation_errors as (

    select game_id, player, count(capture)
    from validation
    where capture is true
    group by game_id, player
    having count(capture) > 15
)


select * from validation_errors


{% endtest %}