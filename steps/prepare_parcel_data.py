def sum_ofm_by_control_area(pipeline):
    p = pipeline
    dec = p.get_table('ofm_')
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