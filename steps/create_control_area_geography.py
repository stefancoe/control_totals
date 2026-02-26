import pandas as pd
import geopandas as gpd
import numpy as np
from util import Pipeline

def union_dissolve(primary, secondary, id_col):
    primary = primary.rename(columns={id_col:'primary_id'})
    secondary = secondary.rename(columns={id_col:'secondary_id'})
    primary = primary.overlay(secondary, how='union',keep_geom_type=True)
    primary.loc[primary['primary_id'].isna(), 'primary_id'] = primary.loc[primary['primary_id'].isna(), 'secondary_id']
    primary = primary.dissolve(by='primary_id', as_index=False, dropna=False)
    return primary.rename(columns={'primary_id':id_col})[[id_col, 'geometry']]

def spatial_join_dissolve(gdf, gdf_to_join, gdf_to_join_id):
    gdf = gdf.reset_index(drop=True)
    gdf['temp_id'] = gdf.index + 1
    gdf_pts = gdf.copy()
    gdf_pts['geometry'] = gdf_pts.representative_point()
    gdf_pts = gdf_pts.sjoin(gdf_to_join, how='left')
    gdf = gdf.merge(gdf_pts[['temp_id', gdf_to_join_id]], on='temp_id', how='left')
    gdf = gdf.dissolve(by=gdf_to_join_id, as_index=False)
    return gdf

def prepare_counties(pipeline):
    p = pipeline
    rural_control_id_map = {
        '033': 64,
        '035': 76,
        '053': 124,
        '061': 176
    }
    county = (
        p.get_geodataframe('county')
        .query("psrc == 1")
        .assign(control_id = lambda df: df['county_fip'].map(rural_control_id_map))
    )
    return county[['control_id','geometry']]


def prepare_military_bases(pipeline):
    p = pipeline
    county = p.get_geodataframe('county').query("psrc == 1")
    military_xwalk = p.get_table('military_bases_xwalk')
    military = (
        p.get_geodataframe('military_bases')
        .dissolve('milspn_id')
        .merge(military_xwalk, on='milspn_id', how='inner')
        .clip(county.dissolve())
    )
    return military[['control_id', 'geometry']]

def prepare_tribal_areas(pipeline):
    p = pipeline
    county = p.get_geodataframe('county')
    tribal = p.get_geodataframe('tribal_land').clip(county.dissolve())
    tribal = tribal.loc[tribal.tribal_land=='Tulalip Reservation'].dissolve()
    tribal['control_id'] = 210
    return tribal[['control_id','geometry']]

def prepare_regional_geographies(pipeline):
    p = pipeline
    reg = p.get_geodataframe('regional_geographies')
    reg_xwalk = p.get_table('regional_geographies_xwalk')
    reg['reg_id'] = reg['cnty_name'] + '_' + reg['juris']
    reg = reg.merge(reg_xwalk, on='reg_id', how='left')
    
    # split Renton PAA into the 3 seperate control areas (using old control areas for now)
    renton = reg.loc[reg['juris']=='Renton PAA'][['geometry']].copy()
    reg = reg.loc[reg['juris']!='Renton PAA'].copy()
    renton = renton.explode()
    old = p.get_geodataframe('old_control_areas')
    renton = spatial_join_dissolve(renton, old, 'control_id')
    reg = pd.concat([reg, renton], ignore_index=True)
    return reg[['control_id','geometry']]

def prepare_natural_resource_areas(pipeline):
    p = pipeline
    control_id_map = {
        '033': 301,
        '035': 302,
        '053': 303,
        '061': 304,
    }
    county = p.get_geodataframe('county').query("psrc == 1")
    buffer_size = 50
    nat_forest = p.get_geodataframe('national_forest')
    nat_forest['geometry'] = nat_forest.buffer(buffer_size)
    nat_park = p.get_geodataframe('national_park')
    nat_park['geometry'] = nat_park.buffer(buffer_size)
    nat_resource = p.get_geodataframe('natural_resource')
    nat_resource['geometry'] = nat_resource.buffer(buffer_size)
    gdf = pd.concat([nat_forest, nat_park, nat_resource], ignore_index=True)
    gdf = gdf.dissolve()
    gdf['geometry'] = gdf.buffer(-buffer_size)
    gdf = gdf.clip(county.dissolve())
    gdf = (
        gdf.overlay(county)
        .dissolve('county_fip',as_index=False)
        .assign(control_id = lambda df: df['county_fip'].map(control_id_map))
    )
    return gdf[['control_id','geometry']]

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Creating control area geography and saving to HDF5...")
    
    # prepare all layers for unioning
    counties = prepare_counties(p)
    reg = prepare_regional_geographies(p)
    military = prepare_military_bases(p)
    jblm_uga = reg.loc[reg['control_id'] == 405].copy()
    tribal = prepare_tribal_areas(p)
    nat_res = prepare_natural_resource_areas(p)

    # union all layers
    gdf = union_dissolve(reg,counties,'control_id')
    gdf = union_dissolve(military,gdf,'control_id')
    gdf = union_dissolve(jblm_uga,gdf,'control_id')
    gdf = union_dissolve(tribal,gdf,'control_id')
    gdf = union_dissolve(nat_res,gdf,'control_id')

    # add control names and target ids
    xwalk = p.get_table('control_target_xwalk')
    gdf = gdf.merge(xwalk,on='control_id',how='left')

    # save control areas to h5
    p.save_geodataframe('control_areas',gdf)
    return context