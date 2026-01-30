from util import Pipeline,load_base_year_emp


def load_targets(pipeline):
    p = pipeline
    emp = load_base_year_emp(p,'includes res con')
    df = (
        p.get_table('adjusted_emp_change_targets')
        .merge(emp, on='target_id', how='inner')
    )
    return df

def calc_targets(pipeline):
    p = pipeline
    df = load_targets(p)

    base_year = p.settings['base_year']
    end_year = p.settings['targets_end_year']

    emp_end_year_col = f'emp_{end_year}'
    emp_no_mil_base_year_col = f'Emp_TotNoMil_{base_year}'

    df[emp_end_year_col] = (df[emp_no_mil_base_year_col] + df['emp_chg_adj']).round(0).astype(int)

    return df

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print('Calculating targets for counties that include resource and construction employment...')
    df = calc_targets(p)
    p.save_table('adjusted_emp_change_targets_res_con', df)
    return context