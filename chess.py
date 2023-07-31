import numpy as np
import pandas as pd
# from datetime import timedelta

from prefect import flow, task, get_run_logger
# from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_load_file


temp_moves_total = []


@task(log_prints=True) # cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data() -> pd.DataFrame:
    logger = get_run_logger()
    df = pd.read_csv('chess_games.csv', nrows=800_000,
                    usecols=['Event', 'Result', 'UTCDate', 'Opening', 'Termination', 'AN'])
    logger.info(f"{len(df)} rows was extracted")
    return df


@task(log_prints=True)
def extract_data_by_chunks() -> pd.DataFrame:
    logger = get_run_logger()
    chess_game_list = []
    chunksize = 50000
    for chunk in pd.read_csv('chess_games.csv', nrows=800_000, chunksize=chunksize, usecols=['Event', 'Result', 'UTCDate', 'Opening', 'Termination', 'AN']):
        chess_game_list.append(chunk)
    df = pd.concat(chess_game_list)
    logger.info(f"{len(chess_game_list)*chunksize} rows was extracted")
    return df


@task(log_prints=True)
def remove_ambiguous_results(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    filt = (df['Result'] != '*') & (df['Termination'] != 'Abandoned') & (df['Termination'] != 'Rules infraction')
    logger.info(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    logger.info(f"{len(df)} rows left")
    return df


@task(log_prints=True)
def remove_short_games(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    filt = df["AN"].apply(len) > 50
    logger.info(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    logger.info(f"{len(df)} rows left")
    return df


@task(log_prints=True)
def remove_an_values(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    filt = ~df['AN'].str.contains('\[%eval')
    logger.info(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    logger.info(f"{len(df)} rows left")
    return df


@task(log_prints=True)
def remove_rare_openings(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    vc = df['Opening'].value_counts()
    vals_to_remove = vc[vc < 1500].index.values
    df['Opening'].loc[df['Opening'].isin(vals_to_remove)] = 'REMOVE'
    filt = df['Opening'] != 'REMOVE'
    logger.info(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    logger.info(f"{len(df)} rows left")
    return df


@task(log_prints=True)
def remove_long_games(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    # filt = df['AN'].str.len() <= 540
    filt = df["AN"].apply(len) <= 540
    logger.info(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    logger.info(f"{len(df)} rows left")
    return df


@task(log_prints=True)
def reset_df_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    return df


@task(log_prints=True)
def column_names_to_lowercase(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    return df


@task(log_prints=True)
def add_tournament_column(df: pd.DataFrame) -> pd.DataFrame:
    df['tournament'] = df['event'].str.contains('tournament')
    return df


@task(log_prints=True)
def rename_event_values(df: pd.DataFrame) -> pd.DataFrame:
    df['event'] = df['event'].map({' Classical ': 'Classical',
                                    ' Blitz ': 'Blitz',
                                    ' Blitz tournament ': 'Blitz',
                                    ' Correspondence ': 'Classical',
                                    ' Classical tournament ': 'Classical',
                                    ' Bullet tournament ': 'Bullet',
                                    ' Bullet ': 'Bullet',
                                    'Blitz tournament ': 'Blitz',
                                    'Bullet ': 'Bullet',
                                    'Classical ': 'Classical',
                                    'Blitz ': 'Blitz'
                                    })
    return df


@task(log_prints=True)
def clean_an_to_space_separated(df: pd.DataFrame) -> pd.DataFrame:
    df['an'] = df['an'].str.replace('[0-9]+\. | 1-0| 0-1| 1/2-1/2| \*', '', regex=True)
    return df


@task(log_prints=True)
def change_df_column_datatypes(df: pd.DataFrame) -> pd.DataFrame:
    df['event'] = df['event'].astype('category')
    df['result'] = df['result'].astype('category')
    df['termination'] = df['termination'].astype('category')
    df['utcdate'] = pd.to_datetime(df['utcdate'])
    return df


def populate_moves_total(row) -> None:
    '''
    Split each move (string) into a tuple.
    Tuple format: (game_id, move_num, player, move)
    Game_id is foreign key of the main dataframe (index column).
    Player is categorical column with values 1 for 'white' or 2 for 'black'.
    '''
    moves_list = row['an'].split(' ')
    for i, move in enumerate(moves_list, start=1):
        player = 2 if i % 2 == 0 else 1
        tuple_move = tuple([row.name] + [i] + [player] + [move])
        temp_moves_total.append(tuple_move)


def create_df_moves(temp_moves_total: list[tuple]) -> pd.DataFrame:
    df_moves = pd.DataFrame(temp_moves_total, columns=['game_id', 'move_num', 'player', 'move'])
    return df_moves


@task(log_prints=True)
def create_moves_total(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    df.apply(populate_moves_total, axis = 1)
    logger.info(f"{len(df)} rows was parsed to {len(temp_moves_total)} moves")
    
    df_moves = create_df_moves(temp_moves_total)
    return df_moves


@task(log_prints=True)
def change_df_moves_column_datatypes(df_moves: pd.DataFrame) -> pd.DataFrame:
    df_moves['player'] = df_moves['player'].astype('int8')
    df_moves['move_num'] = df_moves['move_num'].astype('int16')
    return df_moves


@task(log_prints=True)
def drop_an_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop('an', axis=1)
    return df


@task(log_prints=True)
def add_castling_column(df_moves: pd.DataFrame) -> pd.DataFrame:
    castling_conditions = [
        df_moves['move'] == 'O-O',
        df_moves['move'] == 'O-O+',
        df_moves['move'] == 'O-O#',
        df_moves['move'] == 'O-O-O',
        df_moves['move'] == 'O-O-O+',
        df_moves['move'] == 'O-O-O#'
    ]

    castling_outputs = [1, 1, 1, 2, 2, 2] # 'kingside', 'kingside', 'kingside', 'queenside', 'queenside', 'queenside'

    castling = np.select(castling_conditions, castling_outputs, 0)
    df_moves['castling'] = pd.Series(castling.astype(np.int8))
    return df_moves


@task(log_prints=True)
def add_piece_name_column(df_moves: pd.DataFrame) -> pd.DataFrame:
    piece_conditions = [
    df_moves['move'].str.startswith('O-O') == True, # castling is considered as a king move
    df_moves['move'].str.startswith('K') == True,
    df_moves['move'].str.startswith('Q') == True,
    df_moves['move'].str.startswith('R') == True,
    df_moves['move'].str.startswith('B') == True,
    df_moves['move'].str.startswith('N') == True
    ]

    piece_outputs = [1, 1, 2, 3, 4, 5] # 'king', 'king', 'queen', 'rook', 'bishop', 'knight'

    piece_name = np.select(piece_conditions, piece_outputs, 6) # pawn don't have piece name in AN
    df_moves['piece_name'] = pd.Series(piece_name.astype(np.int8))
    return df_moves


@task(log_prints=True)
def add_capture_column(df_moves: pd.DataFrame) -> pd.DataFrame:
    df_moves['capture'] = df_moves['move'].str.contains('x') == True
    return df_moves


@task(log_prints=True)
def add_pawn_promotion_column(df_moves: pd.DataFrame) -> pd.DataFrame:
    pawn_promotion_conditions = [
    df_moves['move'].str.contains('=Q') == True,
    df_moves['move'].str.contains('=R') == True,
    df_moves['move'].str.contains('=B') == True,
    df_moves['move'].str.contains('=N') == True
    ]

    pawn_promotion_outputs = [2, 3, 4, 5] # 'queen', 'rook', 'bishop', 'knight'

    pawn_promotion = np.select(pawn_promotion_conditions, pawn_promotion_outputs, 0)
    df_moves['pawn_promotion'] = pd.Series(pawn_promotion.astype(np.int8))
    return df_moves


@task(log_prints=True)
def add_position_column(df_moves: pd.DataFrame) -> pd.DataFrame:
    position_conditions = [
        df_moves['move'].str.endswith('+') == True,
        df_moves['move'].str.endswith('#') == True,
    ]

    position_outputs = [1, 2] # 'check', 'checkmate'

    position = np.select(position_conditions, position_outputs, 0)
    df_moves['position'] = pd.Series(position.astype(np.int8))
    return df_moves


@task(log_prints=True)
def load_data_to_postgres(df: pd.DataFrame, table_name: str, primary_key_name: str) -> None:
    logger = get_run_logger()

    database_block = SqlAlchemyConnector.load("postgres-chess")
    with database_block.get_connection(begin=False) as engine:
        df.to_sql(table_name, engine, if_exists='replace', index_label=primary_key_name, chunksize=10_000)
    
    logger.info(f'{len(df)} rows was loaded into {table_name}')


@task(log_prints=True)
def load_data_to_csv(df: pd.DataFrame, file_name: str, primary_key_name: str) -> None:
    logger = get_run_logger()
    df.to_csv(file_name, index_label=primary_key_name)
    logger.info(f'{len(df)} rows was loaded into {file_name}')


@flow(name='ETL_chess', log_prints=True)
def main() -> None:
    # EXTRACT
    df = extract_data()

    # TRANSFORM
    # filter dataframe
    df = remove_ambiguous_results(df)
    df = remove_short_games(df)
    df = remove_an_values(df)
    df = remove_rare_openings(df)
    df = remove_long_games(df) # in order to meet 15 million rows limit of Tableau Public

    # expand and rearrange dataframe
    df = reset_df_index(df)
    df = column_names_to_lowercase(df)
    df = add_tournament_column(df)
    df = rename_event_values(df)
    df = clean_an_to_space_separated(df)
    df = change_df_column_datatypes(df)
    
    # create and prepare df_moves
    df_moves = create_moves_total(df)
    df_moves = change_df_moves_column_datatypes(df_moves)
    df = drop_an_column(df) # 'an' is no longer useful
    
    # populate df_moves
    df_moves = add_castling_column(df_moves)
    df_moves = add_piece_name_column(df_moves)
    df_moves = add_capture_column(df_moves)
    df_moves = add_pawn_promotion_column(df_moves)
    df_moves = add_position_column(df_moves)

    # LOAD
    # load_data_to_postgres(df, 'chess_games', 'game_id')
    # load_data_to_postgres(df_moves, 'chess_moves', 'move_id')

    load_data_to_csv(df, 'tableau_chess_games.csv', 'game_id')
    load_data_to_csv(df_moves, 'tableau_chess_moves.csv', 'move_id')

    '''
    https://prefecthq.github.io/prefect-gcp/bigquery/
    bigquery_load_file is build-in prefect task, so I decided to call it here
    alternatively, you can run it inside other tasks by using 'bigquery_load_file.fn'
    '''
    gcp_credentials = GcpCredentials.load('gcp-cred-chess')
    bigquery_load_file(dataset = 'marts', location = 'europe-west1', table = 'chess_games', path = 'tableau_chess_games.csv', gcp_credentials = gcp_credentials)
    bigquery_load_file(dataset = 'marts', location = 'europe-west1', table = 'chess_moves', path = 'tableau_chess_moves.csv', gcp_credentials = gcp_credentials)


if __name__ == '__main__':
    main()