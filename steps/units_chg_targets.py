import pandas as pd
from util import Pipeline, calc_gq


def load_tables(pipeline):
    p = pipeline
    base_year = p.settings['base_year']

    units = p.get_table('adjusted_units_change_targets').drop(columns=['start'])
    pop = p.get_table('adjusted_total_pop_change_targets').drop(columns=['start'])
    df = pop.merge(units, on='target_id',how='inner')
    
    ofm = p.get_table(f'ofm_parcelized_{base_year}_by_control_area')
    xwalk = p.get_table('control_target_xwalk')
    ofm = ofm.merge(xwalk,on='control_id',how='left')
    ofm = ofm.groupby('target_id').agg({
        'rgid':'first',
        'county_id':'first',
        'ofm_total_pop':'sum',
        'ofm_hhpop':'sum',
        'ofm_units':'sum',
        'ofm_hh':'sum',
        'ofm_gq':'sum'
    }).reset_index()
    ofm['ofm_vacancy_by_rgid'] = \
        1 - ofm.groupby(['rgid','county_id'])['ofm_hh'].transform('sum') / ofm.groupby(['rgid','county_id'])['ofm_units'].transform('sum')
    df = df.merge(ofm,on='target_id',how='left')
    df = calc_gq(p,df,ofm,2044,'OFM')
    return df

def targets_calculations(pipeline,df):
    p = pipeline
    targets_end_year = p.settings["targets_end_year"]
    total_pop_horizon_col = f'total_pop_{targets_end_year}'
    hhpop_horizon_col = f'hhpop_{targets_end_year}'
    gq_horizon_col = f'gq_{targets_end_year}'
    hhsz_horizon_col = f'hhsz_{targets_end_year}'
    hh_horizon_col = f'hh_{targets_end_year}'
    units_horizon_col = f'units_{targets_end_year}'

    # calculate total population for horizon year using REF GQ
    df[total_pop_horizon_col] = df['ofm_total_pop'] + df['total_pop_chg_adj']
    # calculate hhpop for horizon year
    df[hhpop_horizon_col] = df[total_pop_horizon_col] - df[gq_horizon_col]
    # calculate housing units for horizon year
    df[units_horizon_col] = df['ofm_units'] + df['units_chg_adj']
    # calculat hhlds using OFM vacancy rate by RGID
    df[hh_horizon_col] = df[units_horizon_col] * (1 - df['ofm_vacancy_by_rgid'])
    # calcualte implied hhsz for horizon year for reference
    df[hhsz_horizon_col] = df[hhpop_horizon_col] / df[hh_horizon_col]
    return df

def run_step(context):
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that use housing targets...')
    df = load_tables(p)
    df = targets_calculations(p,df)
    p.save_table('adjusted_units_change_targets',df)
    return context

    