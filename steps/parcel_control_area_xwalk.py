import pandas as pd
import geopandas as gpd
from util import Pipeline


def create_parcel_control_area_xwalk(pipeline):
    p = pipeline
    
    # load parcels geodataframe from h5
    parcel_pts = p.get_geodataframe('parcel_pts')
    parcel_id = p.get_id_col('parcel_pts')

    # load regional geographies geodataframe from h5
    regional_geography = p.get_geodataframe('regional_geography')
    # dissolve on control_id to get control areas
    control_areas = regional_geography.dissolve('control_id').reset_index()[['control_id', 'geometry']]

    # spatial join parcel centroids to get control_id for each parcel
    # uses sjoin_nearest to handle edge cases where centroids fall just outside control areas
    parcel_join = parcel_pts.sjoin(control_areas, how = 'left').drop(columns=['index_right'])
    # copy parcel points that didn't join to a control area
    missing_control_id = parcel_join.loc[parcel_join['control_id'].isna()].copy().drop(columns=['control_id'])
    # use spatial join nearest to assign control_id based on nearest control area
    missing_control_id = missing_control_id.sjoin_nearest(control_areas).drop(columns=['index_right'])
    # drop parcels that didn't join to a control area
    parcel_join = parcel_join.loc[~parcel_join['control_id'].isna()].copy()
    # combine parcels that joined to a control area with those that were assigned a control area based on nearest
    parcel_out = pd.concat([parcel_join[['parcel_id','control_id']], missing_control_id[['parcel_id','control_id']]], ignore_index=True)
    parcel_out['control_id'] = parcel_out['control_id'].astype(int)

    # save parcel to control area crosswalk
    p.save_table('parcel_control_area_xwalk', parcel_out)


def run_step(context):
    # pypyr step
    print("Creating parcel to control area crosswalk...")
    p = Pipeline(settings_path=context['configs_dir'])
    create_parcel_control_area_xwalk(p)
    return context