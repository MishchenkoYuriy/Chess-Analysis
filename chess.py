import re
import pandas as pd
from datetime import timedelta

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash


@task(log_prints=True, timeout_seconds=30)
      # cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data() -> pd.DataFrame:
    logger = get_run_logger()
    df = pd.read_csv('chess_games.csv', nrows=50000,
                    usecols=['Event', 'Result', 'UTCDate', 'Opening', 'Termination', 'AN']) # chunksize=100000, index_col=''
    logger.info(f"{len(df)} rows was extracted")
    return df


@task(log_prints=True)
def remove_useless_results(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    filt = (df['Result'] != '*') & (df['Termination'] != 'Abandoned') & (df['Termination'] != 'Rules infraction')
    logger.info(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    logger.info(f"{len(df)} rows left")
    return df


@task(log_prints=True)
def remove_short_matches(df: pd.DataFrame) -> pd.DataFrame:
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
def reset_df_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    return df


@task(log_prints=True)
def create_tournament_column(df: pd.DataFrame) -> pd.DataFrame:
    df['Tournament'] = df['Event'].str.contains('tournament')
    return df


@task(log_prints=True)
def rename_event_values(df: pd.DataFrame) -> pd.DataFrame:
    df['Event'] = df['Event'].map({' Classical ': 'Classical',
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
def change_column_datatype(df: pd.DataFrame) -> pd.DataFrame:
    df['UTCDate'] = pd.to_datetime(df['UTCDate'])
    return df


def get_raw_list(row: str) -> list:
    '''Parse a string to list of moves.
       List elements are strings.'''
    raw_list = []
    raw_list.append(row)
    sep_num = 2
    while True:
        separator = str(sep_num)+'.'
        if separator in raw_list[-1]:
            raw_list = raw_list[0:-1] + raw_list[-1].split(' '+separator) # concatenate all previous moves and remainder
            raw_list[-1] = str(sep_num) + raw_list[-1]
            sep_num+=1
        else:
            raw_list[-1] = re.sub(' 1-0', '', raw_list[-1])
            raw_list[-1] = re.sub(' 0-1', '', raw_list[-1])
            raw_list[-1] = re.sub(' 1/2-1/2', '', raw_list[-1])
            raw_list[0] = re.sub('1.', '1', raw_list[0])
            break
    return raw_list


@task(log_prints=True)
def parse_moves_to_list(df: pd.DataFrame) -> pd.DataFrame:
    df["AN"] = df["AN"].apply(get_raw_list)
    return df


@task(log_prints=True)
def create_moves_table(df: pd.DataFrame) -> pd.DataFrame:
    df_moves_total = pd.DataFrame()
    
    for row in range(len(df)):
        df_moves = pd.DataFrame()
        
        for move in range(len(df['AN'][row])):
            tuple_move = tuple([row] + df['AN'][row][move].split(' ')) # ('0','1','e4','b6')
            
            if len(tuple_move) == 3:
                tuple_move = tuple_move + (None, ) # last move ('0', '38', 'Rfd1', None)
            elif len(tuple_move) < 3 or len(tuple_move) > 4:
                print(f'Unexpected number of elements after split: {len(tuple_move)} elements in {tuple_move} in {row} row')
            
            df_temp = pd.DataFrame([tuple_move], columns=['match_id', 'move_num', 'white_move', 'black_move']) # parse each list element
            df_moves = pd.concat([df_moves, df_temp]) # add to df for this row (match)
        
        df_moves_total = pd.concat([df_moves_total, df_moves])
    
    return df_moves_total


def create_moves_table_no_fk_1(row: list):
    #df_moves_total = pd.DataFrame()
    df_moves = pd.DataFrame()
    
    for move in range(len(row)):
        tuple_move = tuple([999] + row[move].split(' ')) # ('0','1','e4','b6')
        
        if len(tuple_move) == 3:
            tuple_move = tuple_move + (None, ) # last move ('0', '38', 'Rfd1', None)
        elif len(tuple_move) < 3 or len(tuple_move) > 4:
            print(f'Unexpected number of elements after split: {len(tuple_move)} elements in {tuple_move} in {row} row')
        
        df_temp = pd.DataFrame([tuple_move], columns=['match_id', 'move_num', 'white_move', 'black_move']) # parse each list element
        df_moves = pd.concat([df_moves, df_temp]) # add to df for this row (match)
    
    #df_moves_total = pd.concat([df_moves_total, df_moves])

    return df_moves


@task(log_prints=True)
def create_moves_table_no_fk(df: pd.DataFrame) -> pd.DataFrame:
    df_moves_total = df["AN"].apply(create_moves_table_no_fk_1)
    return df_moves_total


@task(log_prints=True)
def load_data() -> None:
    print('Finish')


@flow(name='ETL_chess', log_prints=True)
def main() -> None:
    # EXTRACT
    df = extract_data()

    # TRANSFORM
    # filter dataframe
    df = remove_useless_results(df)
    df = remove_short_matches(df)
    df = remove_an_values(df)
    # df = remove_rare_openings(df)

    # expand and rearrange dataframe
    df = reset_df_index(df)
    df = create_tournament_column(df)
    df = rename_event_values(df)
    
    df = change_column_datatype(df)
    df = parse_moves_to_list(df)
    # df_moves_total = create_moves_table(df)
    df_moves_total = create_moves_table_no_fk(df)
    
    # LOAD
    load_data()


if __name__ == '__main__':
    main()