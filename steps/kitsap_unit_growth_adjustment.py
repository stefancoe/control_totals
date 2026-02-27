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

def split_housing_growth_targets(df):
    pop_growth = 'total_pop_chg'
    unit_growth_total = 'units_chg'
    df['ofm_hhsz'] = df['ofm_hhpop'] / df['ofm_hh']
    df['ofm_vacancy'] = 1 - df['ofm_hh'] / df['ofm_units']
    df['calc_hh_growth'] = df[pop_growth] / df['ofm_hhsz']
    df['calc_unit_growth'] = df['calc_hh_growth'] * (1 - df['ofm_vacancy'])
    df['pct_of_calc_unit_growth'] = df['calc_unit_growth'] / df['calc_unit_growth'].groupby(df['HousingJuris']).transform('sum')
    df[unit_growth_total] = df.groupby('HousingJuris')[unit_growth_total].transform('sum')
    df['units_chg'] = df['pct_of_calc_unit_growth'] * df[unit_growth_total]
    for juris in df['HousingJuris'].unique():
        mask = df['HousingJuris'] == juris
        df.loc[mask, 'units_chg'] = saferound(df.loc[mask, 'units_chg'], 0)
    df['units_chg'] = df['units_chg'].astype(int)
    df = df[['target_id','name','total_pop_chg',
        'units_chg', 'emp_chg']]
    return df

def run_step(context):
    print('Filling in missing Kitsap housing unit growth targets...')
    p = Pipeline(settings_path=context['configs_dir'])
    start_year = get_start_year(p)
    df = load_tables(p, start_year)
    df = split_housing_growth_targets(df)
    p.save_table('kitsap_targets',df)
    return context