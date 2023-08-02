{% test capture_limit_per_player(model, column_name) %}


with
validation as (

    select * from {{ model }}

),

validation_errors as (

    select game_id, player, count( {{ column_name }} )
    from validation
    where {{ column_name }} is true
    group by game_id, player
    having count( {{ column_name }} ) > 15
)


select * from validation_errors


{% endtest %}