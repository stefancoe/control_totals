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
    """Scale values in a column so their sum matches target_total."""
    df[new_col] = df[value_col] * (target_total / df[value_col].sum())
    return df

def get_target_cols(targets_year):
    """Return a dict of target-year column names used by this step."""
    return {
        'units_target_col': f'units_{targets_year}',
        'hh_target_col': f'hh_{targets_year}',
        'hhpop_target_col': f'hhpop_{targets_year}',
        'total_pop_target_col': f'total_pop_{targets_year}',
        'emp_target_col': f'emp_{targets_year}',
    }

def split_incorp_unincorp(df):
    """Split input targets into incorporated and unincorporated subsets."""
    df_incorp = df.loc[df['HousingJuris'] != 'Unincorporated'].copy()
    df_unincorp = df.loc[df['HousingJuris'] == 'Unincorporated'].copy()
    return df_incorp, df_unincorp

def compute_unincorp_hh_targets(df_unincorp, cols, unincorp_units_target):
    """Compute and normalize unincorporated household targets from population targets."""
    hhpop_target_col = cols['hhpop_target_col']
    total_pop_target_col = cols['total_pop_target_col']

    df_unincorp['ofm_gq_pct'] = df_unincorp['ofm_gq'] / df_unincorp['ofm_total_pop']
    df_unincorp[hhpop_target_col] = df_unincorp[total_pop_target_col] * (1 - df_unincorp['ofm_gq_pct'])

    start_uninc_hhsz = df_unincorp['ofm_hhpop'].sum() / df_unincorp['ofm_hh'].sum()
    start_uninc_vacancy = 1 - df_unincorp['ofm_hh'].sum() / df_unincorp['ofm_units'].sum()
    unincorp_hh_target = unincorp_units_target * (1 - start_uninc_vacancy)
    target_uninc_hhsz = df_unincorp[hhpop_target_col].sum() / unincorp_hh_target
    target_to_start_hhsz_ratio = target_uninc_hhsz / start_uninc_hhsz

    df_unincorp['ofm_hhsz'] = df_unincorp['ofm_hhpop'] / df_unincorp['ofm_hh']
    df_unincorp['hhsz_target'] = df_unincorp['ofm_hhsz'] * target_to_start_hhsz_ratio
    df_unincorp['prelim_hh_target'] = df_unincorp[hhpop_target_col] / df_unincorp['hhsz_target']
    return normalize(df_unincorp, unincorp_hh_target, 'prelim_hh_target', cols['hh_target_col'])

def allocate_unincorp_units(df_unincorp, cols, unincorp_units_target):
    """Allocate and round unincorporated unit targets to match the total units target."""
    df_unincorp['ofm_vacancy'] = 1 - df_unincorp['ofm_hh'] / df_unincorp['ofm_units']
    df_unincorp['prelim_units_target'] = df_unincorp[cols['hh_target_col']] * (1 - df_unincorp['ofm_vacancy'])
    df_unincorp = normalize(df_unincorp, unincorp_units_target, 'prelim_units_target', 'norm_units_target')
    df_unincorp[cols['units_target_col']] = saferound(df_unincorp['norm_units_target'], 0)
    df_unincorp[cols['units_target_col']] = df_unincorp[cols['units_target_col']].astype(int)
    return df_unincorp

def finalize_targets(df_incorp, df_unincorp, cols):
    """Combine incorporated/unincorporated rows and return the final output schema."""
    df_out = pd.concat([df_incorp, df_unincorp], ignore_index=True)
    keep_cols = [
        'target_id', 'name', 'HousingJuris', 'total_pop_chg', 'units_chg', 'emp_chg',
        cols['total_pop_target_col'], cols['units_target_col'], cols['emp_target_col']
    ]
    df_out = df_out[keep_cols]
    df_out[cols['units_target_col']] = df_out[cols['units_target_col']].astype(int)
    return df_out

def split_housing_growth_targets(df, targets_year):
    """Split and rebalance unincorporated Kitsap housing targets for the target year."""
    cols = get_target_cols(targets_year)
    df_incorp, df_unincorp = split_incorp_unincorp(df)
    unincorp_units_target = df_unincorp[cols['units_target_col']].sum()

    df_unincorp = compute_unincorp_hh_targets(df_unincorp, cols, unincorp_units_target)
    df_unincorp = allocate_unincorp_units(df_unincorp, cols, unincorp_units_target)

    return finalize_targets(df_incorp, df_unincorp, cols)

def run_step(context):
    print('Splitting unincorporated Kitsap housing targets...')
    p = Pipeline(settings_path=context['configs_dir'])
    start_year = get_start_year(p)
    targets_year = p.settings['targets_end_year']
    df = load_tables(p, start_year)
    df = split_housing_growth_targets(df, targets_year)
    p.save_table('kitsap_targets',df)
    return context