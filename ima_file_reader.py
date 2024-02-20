def td_load_data(spark,username,password,sql):
    driver = 'com.teradata.jdbc.TeraDriver'
    df = (spark.read \
        .format('jdbc') \
        .option('driver', driver) \
        .option('url', "jdbc:teradata://tdwd") \
        .option('dbtable', '({sql}) as src'.format(sql=sql)) \
        .option('user', username) \
        .option('password', password) \
        .load())
    return df  