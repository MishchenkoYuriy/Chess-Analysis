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
    inner join {{ ref('stg__chess_moves') }} m
    on g.game_id = m.game_id

    where g.result <> '1/2-1/2' and m.capture is true

),

player_captures as (

    select
        game_id,
        event,
        result,
        opening,
        termination,
        tournament,
        max(move_num) as game_length,

        (select count(capture) from source where capture is true and player = 1 and game_id = s.game_id) as white_captures,
        (select count(capture) from source where capture is true and player = 2 and game_id = s.game_id) as black_captures,

        (select count(capture) from source where capture is true and player = 1 and game_id = s.game_id) -
        (select count(capture) from source where capture is true and player = 2 and game_id = s.game_id) as captures_diff


        from source s
        group by game_id, event, result, opening, termination, tournament
        order by game_id

),

final as (

    select *
    from player_captures
    where (result = '1-0' and captures_diff < 0) -- white won with less captures
       or (result = '0-1' and captures_diff > 0) -- black won with less captures

)


select * from final

