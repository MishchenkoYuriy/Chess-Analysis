{{ config(materialized='view') }}

with

source_chess_games as (

    select * from {{ source('marts_sources', 'chess_games') }}

),

final as (

    select
        game_id,
        event,
        result,
        utcdate,
        opening,
        termination,
        tournament

    from source_chess_games

)


select * from final

