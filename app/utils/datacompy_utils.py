import datacompy

def compare_dataframes(df1, df2, key_columns):
    compare = datacompy.Compare(
        df1,
        df2,
        join_columns=key_columns,
        abs_tol=0.0001,
        rel_tol=0,
        df1_name='original',
        df2_name='new'
    )
    return compare
