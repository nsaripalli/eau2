# /usr/bin/python3

import math
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import inspect
import os
from statistics import mean


def print_output(num_columns: int, df_len: int, num_complex_iterations: int, is_complex: bool,
                 is_mulitthreaded: bool):
    return [f"./bench", str(num_columns), str(df_len), str(num_complex_iterations),
            str(int(is_complex)), str(int(is_mulitthreaded))]


total_number_of_runs_for_average = 5


def avg_of_multi_subruns(num_columns: int, df_len: int, num_complex_iterations: int, is_complex: bool,
                         is_mulitthreaded: bool) -> float:
    return mean([float(subprocess.check_output(
        print_output(num_columns, df_len, num_complex_iterations, is_complex, is_mulitthreaded)
    ).decode().strip('\n')) for i in range(total_number_of_runs_for_average)])


def make_graph_and_data_dump(data: list, test_var_name: str):
    columns = [test_var_name, "Single Threaded Time (seconds)", "Multi Threaded Time (seconds)"]
    df = pd.DataFrame(data, columns=columns)

    # gca stands for 'get current axis'
    ax = plt.gca()

    df.plot(kind='line', x=columns[0], y=columns[1], ax=ax)
    df.plot(kind='line', x=columns[0], y=columns[2], color='red', ax=ax)

    caller_function_name = inspect.stack()[1][3]

    plt.savefig(f'plots/{caller_function_name}.png')
    df.to_csv(f'data/{caller_function_name}.csv')
    plt.close('all')


def test_rower_complexity_effects():
    data = []
    max_iters = 30000
    num_paritions = 20
    num_columns = 10
    df_len = 4000

    for complexity in range(1, max_iters, math.floor(max_iters / num_paritions)):
        data.append((complexity,
                     avg_of_multi_subruns(num_columns, df_len, complexity, True, is_mulitthreaded=False),
                     avg_of_multi_subruns(num_columns, df_len, complexity, True, is_mulitthreaded=True)
                     ))
    test_var_name = "Rower complexity"
    make_graph_and_data_dump(data, test_var_name)


def test_simple_rower_perf_by_size():
    data = []
    max_iters = 10000
    num_paritions = 20
    num_columns = 10

    for df_len in range(1, max_iters, math.floor(max_iters / num_paritions)):
        data.append((df_len,
                     avg_of_multi_subruns(num_columns, df_len, 0, False, is_mulitthreaded=False),
                     avg_of_multi_subruns(num_columns, df_len, 0, False, is_mulitthreaded=True)
                     ))
    test_var_name = "DataFrame Length"
    make_graph_and_data_dump(data, test_var_name)


def test_complex_rower_perf_by_size():
    data = []
    max_iters = 100000
    num_paritions = 20
    num_columns = 10

    for df_len in range(1, max_iters, math.floor(max_iters / num_paritions)):
        data.append((df_len,
                     avg_of_multi_subruns(num_columns, df_len, 8000, True, is_mulitthreaded=False),
                     avg_of_multi_subruns(num_columns, df_len, 8000, True, is_mulitthreaded=True)
                     ))
    test_var_name = "DataFrame Length"
    make_graph_and_data_dump(data, test_var_name)


if __name__ == "__main__":
    os.makedirs('plots')
    os.makedirs('data')

    print("Running test test_rower_complexity_effects")
    test_rower_complexity_effects()
    print("Running test test_simple_rower_perf_by_size")
    test_simple_rower_perf_by_size()
    print("Running test test_complex_rower_perf_by_size")
    test_complex_rower_perf_by_size()
    print("Done!")
