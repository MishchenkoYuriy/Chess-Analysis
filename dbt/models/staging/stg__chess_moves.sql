{{ config(materialized = 'view') }}

with

source_chess_moves as (

    select * from {{ source('marts_sources', 'chess_moves') }}

),

final as (

    select
        move_id,
        game_id,
        move_num,
        player,
        move,
        castling,
        piece_name,
        capture,
        pawn_promotion,
        position

        from source_chess_moves

)


select * from final

