import pandas as pd
import geopandas as gpd
from util import Pipeline


def create_block_control_area_xwalk(pipeline):
    p = pipeline
    
    # load blocks geodataframe from h5
    blk = p.get_geodataframe('blocks')
    blk_id = p.get_id_col('blocks')

    # load control areas geodataframe from h5
    control_areas = p.get_geodataframe('control_area')
    
    # convert blocks to centroids
    blk_pts = blk.copy()
    blk_pts['geometry'] = blk_pts.representative_point()

    # spatial join block centroids to get rgid for each block
    # uses sjoin_nearest to handle edge cases where centroids fall just outside control areas
    # this shouldn't be a big issue since the edge cases mostly fell on waterways
    blk_pts = blk_pts.sjoin_nearest(control_areas, how = 'left').drop(columns=['index_right'])

    # save block to control area crosswalk
    p.save_table('block_control_area_xwalk', blk_pts[[blk_id, 'control_id']])

def run_step(context):
    # pypyr step
    print("Creating block to control area crosswalk...")
    p = Pipeline(settings_path=context['configs_dir'])
    create_block_control_area_xwalk(p)
    return context
