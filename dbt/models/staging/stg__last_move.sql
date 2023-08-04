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

final as (

    select *,
        (select move       from {{ ref('stg__chess_moves') }} where game_id = m.game_id and move_num = m.game_length) as move,
        (select piece_name from {{ ref('stg__chess_moves') }} where game_id = m.game_id and move_num = m.game_length) as piece_name
    
    from max_move_num m

)

select * from final