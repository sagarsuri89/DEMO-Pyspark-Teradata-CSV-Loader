def write_csv_data(df,dir,w_mode='overwrite',w_header=True):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv(dir,mode=w_mode,header=w_header))
    return None

    