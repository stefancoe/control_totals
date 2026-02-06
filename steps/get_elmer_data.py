from util.elmer_helpers import read_from_elmer_geo, read_from_elmer
from util import Pipeline


def copy_elmer_geo_to_hdf5(pipeline):
    # loop through ElmerGeo files specified in settings.yaml
    elmer_geo_list = pipeline.get_elmer_geo_list()
    if elmer_geo_list:
        for file in elmer_geo_list:
            gdf = read_from_elmer_geo(file['sql_table'],file['columns'])
            
            # convert id column to int64
            gdf = pipeline.convert_id_to_int64(file, gdf)

            # save to HDF5
            pipeline.save_geodataframe(file['name'], gdf)

def copy_elmer_to_hdf5(pipeline):
    # loop through Elmer tables specified in settings.yaml unless it's empty
    elmer_list = pipeline.get_elmer_list()
    if elmer_list:
        for table in elmer_list:
            df = read_from_elmer(table['sql_table'],['*'])
            
            # convert id column to int64
            df = pipeline.convert_id_to_int64(table, df)
            
            # save to HDF5
            pipeline.save_table(table['name'], df)


def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Getting ElmerGeo data and saving to HDF5...")
    copy_elmer_geo_to_hdf5(p)
    print("Getting Elmer data and saving to HDF5...")
    copy_elmer_to_hdf5(p)
    return context
