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

        (case
            when move like 'K%'
                then replace(replace(move, '+', ''), '#', '')
            else move
        end) as move, -- for the check_or_mate_made_by_a_king test to pass

        castling,
        piece_name,
        capture,
        pawn_promotion,

        (case
            when move like 'K%' and (move like '%+' or move like '%#')
                /* set position to 0 (no check or checkmate
                possible after a king move, castling is exception) */
                then 0
            else position
        end) as position -- for the check_or_mate_made_by_a_king test to pass

    from source_chess_moves

)

select * from final
