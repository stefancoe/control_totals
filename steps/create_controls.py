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

def save_r_scrpt_inputs(pipeline, control_totals_df):
    p = pipeline
    
    # rename columns for r script inputs, this will need to be updated to dynamically change based
    # on base year in settings, not sure why the targets were'nt just adjusted to 2018 to begin with.
    rename_cols_2018 = {
        'ofm_total_pop': 'Pop18',
        'ofm_hhpop': 'HHpop18',
        'ofm_hh': 'HH18',
        'ofm_gq': 'GQ18',
        'ofm_units': 'Units18',
    }
    ofm_2018 = p.get_table('ofm_parcelized_2018_by_control_area').rename(columns=rename_cols_2018)
    emp_2018 = p.get_table('employment_2018_by_control_area').rename(columns={'Emp_TotNoMil': 'Emp18'})
    
    rename_cols_2020 = {
        'rgid': 'RGID',
        'target_name': 'name',
        'ofm_total_pop': 'TotPop20',
        'ofm_hhpop': 'HHpop20',
        'ofm_hh': 'HH20',
        'ofm_gq': 'GQ20',
        'ofm_units': 'Units20',
        'Emp_TotNoMil': 'TotEmp20_wCRnoMil',
        'total_pop_2050': 'TotPop50',
        'emp_2050': 'TotEmp50_wCRnoMil'
    }
    df = (
        control_totals_df
        .merge(ofm_2018, on='control_id', how='left')
        .merge(emp_2018, on='control_id', how='left')
        .rename(columns=rename_cols_2020)
    )

    # calculate additional columns needed for r script
    df['TotEmpTrg_wCRnoMil'] = df['TotEmp50_wCRnoMil'] - df['TotEmp20_wCRnoMil']
    df['TotPopTrg'] = df['TotPop50'] - df['TotPop20']
    df['GQpct50'] = df['gq_2050'] / df['TotPop50']
    df['PPH50'] = df['hhpop_2050'] / df['hh_2050']
    # take last 2 digits of county id
    df['county_id'] = df['county_id'].astype(str).str[-2:].astype(int)
    # export final table to excel for r script input
    today = pd.Timestamp.today().strftime('%Y%m%d')
    df.to_excel(f'data/control_id_working_{today}.xlsx', index=False)

    return df


def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    df = load_tables(p)
    df = recalc_excluded_control_areas(p, df)
    df = save_r_scrpt_inputs(p, df)
    p.save_table('control_totals', df)
    return context