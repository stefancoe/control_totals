import pandas as pd
import geopandas as gpd
from util import Pipeline


def union_dissolve(primary_gdf, secondary_gdf, primary_id_col, secondary_id_col, name1, name2):
    '''
    Docstring for union_dissolve
    
    primary_gdf: primary geodataframe to preserve geometry of
    secondary_gdf: secondary geodataframe to overlay on primary
    primary_id_col: primary geodataframe id column
    secondary_id_col: secondary geodataframe id column
    name1: primary geodataframe name column
    name2: secondary geodataframe name column
    '''
    flags = ['reg','military','tribal','nr','jblm']
    flag_cols_1 = [col for col in flags if col in primary_gdf.columns]
    print(flag_cols_1)
    gdf1 = primary_gdf[[primary_id_col,name1, 'geometry'] + flag_cols_1].copy()
    gdf1[primary_id_col] = gdf1[primary_id_col].astype(str) + '_' + primary_id_col.split('_id')[0]
    geog_flag = primary_id_col.split('_id')[0]
    gdf1[geog_flag] = 1
    flag_cols_2 = [col for col in flags if col in secondary_gdf.columns]
    print(flag_cols_2)
    gdf2 = secondary_gdf[[secondary_id_col,name2, 'geometry'] + flag_cols_2].copy()
    gdf2[secondary_id_col] = gdf2[secondary_id_col].astype(str) + '_' + secondary_id_col.split('_id')[0]

    # perform union
    gdf = gpd.overlay(gdf1, gdf2, how='union')

    # for secondary polygons that do not overlap, bring id over to original id column
    # this will allow for bringing in new polygons from secondary layer while keeping original
    # polygons from primary layer intact. Secondary polygons will lose any overlapping area with primary polygons
    # to preserve the original geometry of the primary layer.
    gdf.loc[gdf[primary_id_col].isna(), primary_id_col] = gdf.loc[gdf[primary_id_col].isna(), secondary_id_col]

    # dissolve on primary id column
    gdf = gdf.dissolve(by=primary_id_col, as_index=False)

    combined_layers = primary_id_col.split('_id')[0] + '_' + secondary_id_col.split('_id')[0]
    combined_name = combined_layers + '_name'
    combined_id = combined_layers + '_id'

    gdf[combined_name] = np.where(gdf[name1].notna(), gdf[name1], gdf[name2])
    gdf = gdf.reset_index(drop=True)
    gdf[combined_id] = gdf.index + 1
    return gdf


def create_control_area_geography(pipeline):
    '''
    creates the control area geography by unioning and dissolving the regional geographies,
    military bases, tribal areas, and natural resource areas together. The order of unioning
    is important to preserve the original geometry of the primary layer. Finally, any 
    remaining areas are filled in with the county layer to create a complete control area geography.
    '''
    p = pipeline
    
    # get input layers and clip to county boundary
    # county
    county = p.get_geodataframe('county').query("psrc == 1")
    # regional geographies
    reg = p.get_geodataframe('regional_geographies').clip(county.dissolve()).reset_index(drop=True)
    reg['reg_id'] = reg.index + 1
    reg['reg'] = 1
    # military bases (does not include smaller fueling stations)
    included_milspn_ids = [12,13,19,20,21,22]
    military = p.get_geodataframe('military_bases').query("milspn_id in @included_milspn_ids").dissolve('milspn_id').reset_index(drop=True)
    military['military_id'] = military.index + 1
    military = military.clip(county.dissolve())
    military = military.rename(columns={'name':'military_name'})
    # tribal areas (only Tulalip for now)
    tribal = p.get_geodataframe('tribal_land').clip(county.dissolve())
    tribal = tribal.loc[tribal.tribal_land=='Tulalip Reservation'].dissolve()
    tribal['tribal_id'] = tribal.index + 1
    # natural resource areas (forest, mineral, agriculture)
    nat_resource = p.get_geodataframe('natural_resource').clip(county.dissolve()).dissolve('resource',as_index=False).reset_index(drop=True)
    nat_resource['nr_id'] = nat_resource.index + 1

    # begin unioning and dissolving layers together, the order is important to preserve the original geometry of the primary layer.
    gdf = union_dissolve(military, reg, 'military_id', 'reg_id','military_name','juris')
    
    # add jblm uga back in, this is the only area that overlaps with a military base but should be preserved as its own area
    jblm_uga = reg.loc[reg['juris'] == 'JBLM UGA'].copy()
    jblm_uga = jblm_uga[['juris','geometry']]
    jblm_uga['jblm_id'] = 1
    gdf = union_dissolve(jblm_uga, gdf, 'jblm_id','military_reg_id','juris','military_reg_name')
    jblm = gdf.loc[gdf['jblm_military_reg_name']=='Joint Base Lewis McChord'].copy()
    jblm = jblm.dissolve()
    gdf = gdf.loc[gdf['jblm_military_reg_name']!='Joint Base Lewis McChord'].copy()
    gdf = pd.concat([gdf, jblm], ignore_index=True)

    # add tribal areas
    gdf = union_dissolve(tribal,gdf,'tribal_id','jblm_military_reg_id','tribal_land','jblm_military_reg_name')

    # add natural resource areas
    gdf = union_dissolve(nat_resource, gdf,'nr_id','tribal_jblm_military_reg_id','resource','tribal_jblm_military_reg_name')

    # finally, fill any remaining areas in with the county layer to create a complete control area geography
    county['county_id'] = ('53' + county['county_fip']).astype(int)
    gdf = union_dissolve(gdf,county,'nr_tribal_jblm_military_reg_id','county_id','nr_tribal_jblm_military_reg_name','county_fip')
    return gdf

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Creating control area geography and saving to HDF5...")
    create_control_area_geography(p)
    return context