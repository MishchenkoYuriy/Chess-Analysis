{{ config(materialized='view') }}

with

source as (

    select
        g.game_id,
        g.event,
        g.result,
        g.opening,
        g.termination,
        g.tournament,

        m.player,
        m.capture,
        m.move_num

    from {{ ref('stg__chess_games') }} g
    inner join {{ ref('stg__chess_moves') }} m on g.game_id = m.game_id

    where g.result != '1/2-1/2' and m.capture is true

),

player_captures as (

    select
        s.game_id,
        s.event,
        s.result,
        s.opening,
        s.termination,
        s.tournament,
        max(s.move_num) as game_length,

        (
            select count(capture)
            from source
            where capture is true and player = 1 and game_id = s.game_id
        ) as white_captures,
        (
            select count(capture)
            from source
            where capture is true and player = 2 and game_id = s.game_id
        ) as black_captures,

        (
            select count(capture)
            from source
            where capture is true and player = 1 and game_id = s.game_id
        )
        - (
            select count(capture)
            from source
            where capture is true and player = 2 and game_id = s.game_id
        ) as captures_diff


    from source s
    group by
        s.game_id, s.event, s.result, s.opening, s.termination, s.tournament
    order by s.game_id

),

final as (

    select *
    from player_captures
    where
        -- white won with less captures
        (result = '1-0' and captures_diff < 0)
        -- black won with less captures
        or (result = '0-1' and captures_diff > 0)

)

select * from final
