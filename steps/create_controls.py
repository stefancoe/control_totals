import pandas as pd
from util import Pipeline


def load_tables(pipeline):
    p = pipeline
    base_year = p.settings['base_year']
    target_year = p.settings['targets_end_year']
    control_year = p.settings['end_year']
    target_cols = [
        'target_id',
        f'hh_{target_year}',
        f'total_pop_{target_year}',
        f'gq_{target_year}',
        f'hhpop_{target_year}',
        f'emp_{target_year}',
        
        f'hh_{control_year}',
        f'total_pop_{control_year}',
        f'gq_{control_year}',
        f'hhpop_{control_year}',
        f'emp_{control_year}'
    ]
    # load extrapolated targets
    targets = p.get_table('extrapolated_targets')[target_cols].set_index('target_id').astype(float).reset_index()
    # load control area to target xwalk
    xwalk = p.get_table('control_target_xwalk')
    # load base year ofm data
    base_ofm = p.get_table(f'ofm_parcelized_{base_year}_by_control_area')
    # load base year employment data
    base_emp = p.get_table(f'employment_{base_year}_by_control_area')
    # merge all tables together
    df = (
        xwalk
        .merge(base_ofm, on='control_id', how='left').drop(columns='control_name', errors='ignore')
        .merge(base_emp, on='control_id', how='left').drop(columns='control_name', errors='ignore')
        .merge(targets, on='target_id', how='left')
    )

    return df

def recalc_excluded_control_areas(pipeline, df):
    # set values for excluded areas to their base year values ie. military bases
    p = pipeline
    targets_end_year = p.settings['targets_end_year']
    controls_end_year = p.settings['end_year']
    
    # flag from xwalk for control areas to exclude from target totals
    mask = df['exclude_from_target'] == 1
    # left is horizon year column name, right is base year column name
    updates = {
    'total_pop': 'ofm_total_pop',
    'hhpop': 'ofm_hhpop',
    'gq': 'ofm_gq',
    'hh': 'ofm_hh',
    'emp': f'Emp_TotNoMil',
    }
    # for each horizon year and for each column above, set the value to equal base year value
    # if the control area is flagged for exclusion.
    for year in [targets_end_year, controls_end_year]:
        for prefix, src in updates.items():
            df.loc[mask, f'{prefix}_{year}'] = df.loc[mask, src]

    return df

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    df = load_tables(p)
    recalc_excluded_control_areas(p, df)
    p.save_table('control_totals', df)
    return context