import pandas as pd
from util import Pipeline
from iteround import saferound

def get_start_year(pipeline):
    for table in pipeline.settings['targets_tables']:
        if table['name'] == 'kitsap_targets':
            return table['total_pop_chg_start']

def load_tables(pipeline, start_year):
    p = pipeline
    df = p.get_table('kitsap_targets')
    xwalk = p.get_table('control_target_xwalk')[['control_id','target_id']]
    ofm = (
        p.get_table(f'ofm_parcelized_{start_year}_by_control_area')
        .merge(xwalk, on='control_id', how='left').drop(columns=['control_id'])
        .groupby('target_id').sum().reset_index()
    )
    df = df.merge(ofm, on='target_id', how='left')
    return df

def normalize(df, target_total, value_col, new_col):
    df[new_col] = df[value_col] * (target_total / df[value_col].sum())
    return df

def split_housing_growth_targets(df, targets_year):
    # setup column names for target year
    units_target_col = f'units_{targets_year}'
    hh_target_col = f'hh_{targets_year}'
    hhpop_target_col = f'hhpop_{targets_year}'
    total_pop_target_col = f'total_pop_{targets_year}'
    # split unincorp and incorp for separate handling
    df_incorp = df.loc[df['HousingJuris'] != 'Unincorporated'].copy()
    df = df.loc[df['HousingJuris'] == 'Unincorporated'].copy()
    # unincorp housing target
    unincorp_units_target = df[units_target_col].sum()
    # unincorp population target sum
    unincorp_pop_target = df[total_pop_target_col].sum()
    # calculate %GQ in start year population
    df['ofm_gq_pct'] = df['ofm_gq'] / df['ofm_total_pop']
    # use %GQ to calculate hhpop for target year
    df[hhpop_target_col] = df[total_pop_target_col] * (1 - df['ofm_gq_pct'])
    # ofm hhsz and vacancy for combined unincorporated areas
    start_uninc_hhsz = df['ofm_hhpop'].sum() / df['ofm_hh'].sum()
    start_uninc_vacancy = 1 - df['ofm_hh'].sum() / df['ofm_units'].sum()
    # hh target for combined unincorporated areas
    unincorp_hh_target = unincorp_units_target * (1 - start_uninc_vacancy)
    # hhsz target for combined unincorporated areas
    target_uninc_hhsz = df[hhpop_target_col].sum() / unincorp_hh_target
    # ratio of target hhsz to start year hhsz for combined unincorporated areas
    target_to_start_hhsz_ratio = target_uninc_hhsz / start_uninc_hhsz
    # ofm hhsz
    df['ofm_hhsz'] = df['ofm_hhpop'] / df['ofm_hh']
    # target hhsz
    df['hhsz_target'] = df['ofm_hhsz'] * target_to_start_hhsz_ratio
    # prelim hh
    df['prelim_hh_target'] = df[hhpop_target_col] / df['hhsz_target']
    # normalize hh to match unincorp total
    df = normalize(df, unincorp_hh_target, 'prelim_hh_target', hh_target_col)
    # calculate start year vacancy rate
    df['ofm_vacancy'] = 1 - df['ofm_hh'] / df['ofm_units']
    df['prelim_units_target'] = df[hh_target_col] * (1 - df['ofm_vacancy'])
    # normalize units to match unincorp housing target
    df = normalize(df, unincorp_units_target, 'prelim_units_target', 'norm_units_target')
    # round units to integers
    df[units_target_col] = saferound(df['norm_units_target'],0)
    df[units_target_col] = df[units_target_col].astype(int)
    # combine incorp and unincorp back together, keeping only relevant columns
    df = pd.concat([df_incorp, df], ignore_index=True)
    df = df[['target_id','name','HousingJuris','total_pop_chg','units_chg','emp_chg',total_pop_target_col,units_target_col,f'emp_{targets_year}']]
    df[units_target_col] = df[units_target_col].astype(int)
    return df

def run_step(context):
    print('Splitting unincorporated Kitsap housing targets...')
    p = Pipeline(settings_path=context['configs_dir'])
    start_year = get_start_year(p)
    targets_year = p.settings['targets_end_year']
    df = load_tables(p, start_year)
    df = split_housing_growth_targets(df, targets_year)
    p.save_table('kitsap_targets',df)
    return context