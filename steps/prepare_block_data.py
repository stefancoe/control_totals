import pandas as pd
from util import Pipeline


def sum_decennial_by_regional_geog(pipeline):
    p = pipeline
    dec = p.get_table('dec_block_data')
    blk = p.get_table('block_regional_geography_xwalk')
    block_id = p.get_id_col('blocks')
    rgid = p.get_id_col('regional_geography')

    # merge decennial data with block to control area crosswalk
    df = dec.merge(blk, left_on='geoid', right_on=block_id)

    # get list of decennial census columns
    dec_cols = list(p.settings['census_variables'].keys())

    # sum decennial data by control area
    dec_by_control = (
        df.groupby(rgid)
        .sum()[dec_cols]
        .astype(int)
        .reset_index()
    )

    # calculate hhpop
    dec_by_control['dec_hhpop'] = (
        dec_by_control['dec_total_pop'] - dec_by_control['dec_gq']
    )

    # save to HDF5
    p.save_table('decennial_by_regional_geography', dec_by_control)

def get_ofm_years(pipeline):
    p = pipeline

    years = []
    for table in p.settings['Elmer']:
        if 'ofm_estimates' in table['name']:
            # get year from end of table name
            year = table['name'].split('_')[-1]
            years.append(year)
    return years

def sum_ofm_by_regional_geog(pipeline):
    p = pipeline
    rgid = p.get_id_col('regional_geography')
    years = get_ofm_years(p)
    for year in years:
        ofm = p.get_table(f'ofm_estimates_{year}')
        ofm_block_id = p.get_id_col(f'ofm_estimates_{year}')
        blk = p.get_table('block_regional_geography_xwalk')
        block_id = p.get_id_col('blocks')

        # merge ofm data with block to control area crosswalk
        df = ofm.merge(blk, left_on=ofm_block_id, right_on=block_id)

        # sum ofm data by control area
        ofm_by_control = (
            df.groupby(rgid)
            .sum()[['housing_units', 'occupied_housing_units', 
                    'group_quarters_population', 'household_population']]
            .reset_index()
        )

        # rename columns
        ofm_col_map = {
            'housing_units': 'ofm_units',
            'occupied_housing_units': 'ofm_hh',
            'group_quarters_population': 'ofm_gq',
            'household_population': 'ofm_hhpop'
        }
        ofm_by_control = ofm_by_control.rename(columns=ofm_col_map)

        # calculate total population
        ofm_by_control['ofm_total_pop'] = (
            ofm_by_control['ofm_hhpop'] + ofm_by_control['ofm_gq']
        )

        # save to HDF5
        p.save_table(f'ofm_estimates_{year}_by_regional_geography', ofm_by_control)

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Aggregating Decennial Census data to regional geography level...")
    sum_decennial_by_regional_geog(p)
    print("Aggregating OFM estimates data to regional geography level...")
    sum_ofm_by_regional_geog(p)
    return context