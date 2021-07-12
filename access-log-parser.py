import pandas as pd
from os import listdir
from os import path
import gzip
import shutil

from urlDict import calculator_paths

def printKeys(result, key, value):
    print('{} matches: {}'.format(key, result.path.str.count(value).sum()))


def main():
    print('*** Main ***')

    working_dir = path.dirname(__file__)
    data_dir = path.join(working_dir, 'log-files')

    calc_paths_DF = pd.DataFrame(list(calculator_paths.items()), columns = ['calc', 'path'])

    log_data_columns = [3, 5, 6]
    filepaths = [f for f in listdir(path.join(working_dir, data_dir)) if f.endswith(('.log', '.gz', '.zip'))]
    result = pd.DataFrame()
    #df = pd.DataFrame()
    #df = pd.DataFrame(columns= ['timestamp','httpMethod','token', 'status','responseLatency', 'responseLength'])
    print('*** creating filename list ***')
    for f in filepaths:
        filename = path.join(working_dir, data_dir, f)
        try:
            if f.endswith('.log'):
                df = pd.read_csv(path.join(working_dir, data_dir, f), delim_whitespace=True, header=None, usecols=log_data_columns)
            elif f.endswith('.gz'):
                df = pd.read_csv(path.join(working_dir, data_dir, f), compression='gzip', delim_whitespace=True, header=None, usecols=log_data_columns)
            elif f.endswith('.zip'):
                df = pd.read_csv(path.join(working_dir, data_dir, f), compression='zip', delim_whitespace=True, header=None, usecols=log_data_columns)
            else:
                print('Uanncounted for fileneme: {}'.format(f))
        except:
            pass
        result = pd.concat([result, df], ignore_index=True)
    print('*** reindexing ***')
    result.columns = ['timestamp', 'path', 'status']
    result.timestamp = [x.strip('[') for x in result.timestamp]
    result['timestamp'] = pd.to_datetime(result['timestamp'], errors='coerce')
    #print(result.info())
    
    #for key in calculator_paths:
    #    printKeys(result, key, calculator_paths[key])

    filter_result = result.loc[(result['status'] == 200)]
    filter_result = pd.merge(filter_result, calc_paths_DF, on ='path')
    filter_result.to_csv(path.join(working_dir, 'stats.csv'), sep=',')
    count = filter_result.groupby(['calc']).count()
    print(count)


if __name__ == '__main__':
    print('********* Starting **************')
    main()
    print('*** Finished ***')