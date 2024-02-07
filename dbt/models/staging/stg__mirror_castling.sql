{{ config(materialized='view') }}

with

source as (

    select *
    from {{ ref('stg__chess_moves') }}

),

lead_move as (

    select
        game_id,

        move_num,
        player,
        move,
        castling,

        lead(move_num) over (order by game_id, move_num) as next_move_num,
        lead(player) over (order by game_id, move_num) as next_player,
        lead(move) over (order by game_id, move_num) as next_move,
        lead(castling) over (order by game_id, move_num) as next_castling

    from source

    order by game_id, move_num

),

lead_castling as (

    select *
    from lead_move
    where castling != 0 and next_castling != 0

)


select * from lead_castling
