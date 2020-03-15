import pandas

def genfile():
    num_columns = 900
    df_len = 3000
    overall_data = []
    for row in range(df_len):
        row_data = []
        for col in range(num_columns):
            row_data += [int(col), float(col), bool(col % 1), "testing a normal sentence"]

        overall_data.append(row_data)

    df = pandas.DataFrame(overall_data)
    df.to_csv("datafile.txt")

if __name__ == "__main__":
    genfile()