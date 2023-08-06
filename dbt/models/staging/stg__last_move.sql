{{ config(materialized='view') }}

with

source_chess_moves as (

    select * from {{ ref('stg__chess_moves') }}

),

max_move_num as (

    select
        game_id,
        max(move_num) as game_length

    from source_chess_moves
    group by game_id

),

join_games as (

    select
        cg.*,
        cm.game_length
    
    from max_move_num cm
    inner join {{ ref('stg__chess_games') }} cg
    on cm.game_id = cg.game_id

),

remove_draws_and_surrenders as (

    select * from join_games
    where (mod(game_length, 2) = 1 and result = '1-0') -- white made a move and win after
    or (mod(game_length, 2) = 0 and result = '0-1') -- black made a move and win after

),

final as (

    select
        game_id,
        game_length,
        (select move       from {{ ref('stg__chess_moves') }} where game_id = m.game_id and move_num = m.game_length) as move,
        (select piece_name from {{ ref('stg__chess_moves') }} where game_id = m.game_id and move_num = m.game_length) as piece_name
    
    from remove_draws_and_surrenders m

)

select * from final