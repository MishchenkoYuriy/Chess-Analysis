import re
import pandas as pd

from prefect import flow, task

pd.set_option('display.max_columns', None)


@task(log_prints=True)
def remove_useless_results(df: pd.DataFrame) -> pd.DataFrame:
    filt = (df['Result'] != '*') & (df['Termination'] != 'Abandoned') & (df['Termination'] != 'Rules infraction')
    df = df[filt]
    return df


@task(log_prints=True)
def change_column_datatype(df: pd.DataFrame) -> pd.DataFrame:
    df['UTCDate'] = pd.to_datetime(df['UTCDate'])
    return df


@task(log_prints=True) # change the order
def create_tournament_column(df: pd.DataFrame) -> pd.DataFrame:
    df['Tournament'] = df['Event'].str.contains('tournament')
    return df


@task(log_prints=True) # change the order
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
def remove_short_matches(df: pd.DataFrame) -> pd.DataFrame:
    filt = df["AN"].apply(len) > 50
    # print(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    return df

@task(log_prints=True)
def remove_an_values(df: pd.DataFrame) -> pd.DataFrame:
    filt = ~df['AN'].str.contains('\[%eval')
    # print(f"{len(df)-len(df[filt])} rows was removed")
    df = df[filt]
    return df


@task(log_prints=True)
def remove_rare_openings(df: pd.DataFrame) -> pd.DataFrame:
    vc = df['Opening'].value_counts()
    vals_to_remove = vc[vc < 1500].index.values
    df['Opening'].loc[df['Opening'].isin(vals_to_remove)] = 'REMOVE'
    filt = df['Opening'] != 'REMOVE'
    df = df[filt]
    return df


@task(log_prints=True)
def reset_df_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
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
    
    # print('Done')
    return df_moves_total


@task(name='extract-data', log_prints=True)
def extract_data() -> pd.DataFrame:
    df = pd.read_csv('chess_games.csv', nrows=100_000,
                    usecols=['Event', 'Result', 'UTCDate', 'Opening', 'Termination', 'AN']) # chunksize=100000, index_col=''
    return df


@flow(name='transform-data', log_prints=True)
def transform_data(df): #: pd.DataFrame

    df = remove_useless_results(df)
    df = change_column_datatype(df)

    df = create_tournament_column(df)
    df = rename_event_values(df)

    df = remove_short_matches(df)
    df = remove_an_values(df)
    df = remove_rare_openings(df)

    df = reset_df_index(df)
    df = parse_moves_to_list(df)
    #df_moves_total = create_moves_table(df)
    df_moves_total = pd.DataFrame()

    return df, df_moves_total


@flow(name='load-data', log_prints=True)
def load_data(df, df_moves_total) -> None: #: pd.DataFrame
    print('Finish')


@flow(name='main-flow', log_prints=True, timeout_seconds=30)
def main():
    extracted_df = extract_data()
    #df, df_moves_total = transform_data(extracted_df)
    #load_data(df, df_moves_total)


if __name__ == '__main__':
    main()