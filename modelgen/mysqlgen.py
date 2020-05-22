from model_generator_pyramid import BaseModelGenerator, Params

params = Params()
params.dialect = 'mysql'

params.user = 'mmdev'
params.password = 'mmdev121'
params.host = '127.0.0.1:3307'
params.db_name = 'mmdev'

#schema for db / could be None for default
params.schema = None

#if true Raise exception if a PK is not present in the table
params.force_pk = False

#specify the tables you want to generate the model for.
params.table_list = params.fetch_all_tables_pg(params)
params.table_list = ["client_ad"]
#params.metadata_name = 'ev5_metadata'

bmg = BaseModelGenerator(params)

bmg.generate_model()
