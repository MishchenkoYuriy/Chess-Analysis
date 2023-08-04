{% test checkmate_is_last_move(model) %}


with
validation as (

    select * from {{ model }}

),

prepared_last_moves as (

    select
        game_id,
        max(move_num) as last_move,
        (select move_num from validation where game_id = v.game_id and position = 2) as last_position_move,
        (select move_num from validation where game_id = v.game_id and move like '%#') as last_hashtag_move

    from validation v
    group by game_id, last_position_move, last_hashtag_move

),

validation_errors as (

    select * from prepared_last_moves
    where (last_position_move is not null and last_move <> last_position_move)
    or (last_hashtag_move is not null and last_move <> last_hashtag_move)

)


select * from validation_errors


{% endtest %}