import pandas as pd
import geopandas as gpd
from util import Pipeline


def load_shapefiles_to_hdf5(pipeline):
    # load shapefiles in the Shapefiles list in settings.yaml
    p = pipeline
    shapefiles = p.settings.get('shapefiles', [])
    for file in shapefiles:
        shape_name = file['name']
        file_path = f"{p.get_data_dir()}/shapefiles/{file['file']}"
        print(f"Loading {file_path} into HDF5 as {shape_name}...")
        gdf = gpd.read_file(file_path)

        # save to HDF5
        p.save_geodataframe(shape_name, gdf)


def run_step(context):
    p = Pipeline(settings_path=context['configs_dir'])
    print("Loading shapefiles into HDF5 store...")
    load_shapefiles_to_hdf5(p)
    return context