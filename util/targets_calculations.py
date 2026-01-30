# shared calculations for units change and population change targets

def load_input_tables(pipeline,targets_type):
    # targets type: 'units' or 'total_pop'

    p = pipeline
    # load control to target lookup
    control_target_lookup = p.get_table('control_target_lookup')

    # sum decennial data to target areas
    dec = (
        p.get_table('decennial_by_control_area')
        .merge(control_target_lookup, on='control_id', how='left')
        .groupby(['target_id','RGID','county_id']).sum().reset_index()
        .drop(columns=['control_id','name'])
    )

    # merge decennial data with adjusted targets
    df = (
        p.get_table(f'adjusted_{targets_type}_change_targets')
        .merge(dec, on='target_id', how='left')
    )

    # calculate decennial household population, households and household size by rgid
    df['dec_hhpop_by_rgid'] = df.groupby('RGID')['dec_hhpop'].transform('sum')
    df['dec_hh_by_rgid'] = df.groupby('RGID')['dec_hh'].transform('sum')
    df['dec_hhsz_by_rgid'] = df['dec_hhpop_by_rgid'] / df['dec_hh_by_rgid']

    # calculate decennial hhsz
    df['dec_hhsz'] = df['dec_hhpop'] / df['dec_hh']

    return df, dec


def calc_gq(pipeline, df, dec, horizon_year):
    p = pipeline

    # load the Regional Economic Forecast table to get total GQ for the region in the horizon year
    ref = p.get_table('ref_projection')
    reg_gq_horizon = ref.loc[ref.variable == 'GQ Pop', str(horizon_year)].item()

    # calculate GQ percentage of the region based on decennial data
    reg_dec_gq_sum = dec['dec_gq'].sum()
    df['dec_gq_pct'] = df['dec_gq'] / reg_dec_gq_sum

    # calculate target area GQ for horizon year as a percentage of the regional GQ from REF
    gq_horizon_col = f'gq_{horizon_year}'
    df[gq_horizon_col] = (df['dec_gq_pct'] * reg_gq_horizon).round(0).astype(int)
    return df


def load_base_year_emp(pipeline,emp_target_type):
    # emp_target_type: either 'includes res con' or 'excludes res con'
    p = pipeline
    base_year = p.settings['base_year']
    counties = p.settings['emp_target_types'][emp_target_type]

    emp = p.get_table(f'employment_{base_year}_by_regional_geography')
    rgid = p.get_id_col(f'employment_{base_year}_by_regional_geography')

    rg_lookup_id = p.get_id_col('regional_geography_lookup')
    regional_geog_lookup = p.get_table('regional_geography_lookup')[[rg_lookup_id,'target_id','county_id']]

    emp = (
        emp.merge(regional_geog_lookup, left_on=rgid, right_on=rg_lookup_id, how='left')
        .query(f'county_id in {counties}')
        .groupby(['target_id','county_id']).sum()
        .drop(columns=[rgid, rg_lookup_id])
        .add_suffix(f'_{base_year}')
        .reset_index()
    )
    return emp