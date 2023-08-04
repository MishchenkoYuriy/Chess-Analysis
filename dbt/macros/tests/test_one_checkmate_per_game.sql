{% test one_checkmate_per_game(model, column_name) %}


with
validation as (

    select * from {{ model }}

),

validation_errors as (

    select game_id, count( {{column_name}} )
    from validation
    where {{ column_name }} = 2
    group by game_id
    having count( {{ column_name }} ) > 1
)


select * from validation_errors


{% endtest %}