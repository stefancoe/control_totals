import pandas as pd
from util import Pipeline


def get_emp_no_mil_res_con_col(pipeline, year):
    # get column name for employment excluding military, resource and construction
    p = pipeline
    table_name = f'employment_{year}_by_control_area'
    for table in p.settings['data_tables']:
        if table['name'] == table_name:
            if 'no_mil_res_con_col' not in table or not table['no_mil_res_con_col']:
                raise ValueError(f"'no_mil_res_con_col' not specified in settings for table '{table_name}'")
            return table['no_mil_res_con_col']
    raise ValueError(f"Table '{table_name}' not found in settings")


def combine_targets(pipeline, target_type):
    # target_type: 'total_pop' or 'units', or 'emp'
    df = pd.DataFrame()
    for table in pipeline.settings['targets_tables']:
        if f'{target_type}_chg_col' in table:
            df_table = pipeline.get_table(table['name'])

            # add start year column
            df_table['start'] = table[f'{target_type}_chg_start']

            df = pd.concat([df, df_table], ignore_index=True)
    return df[['target_id', f'{target_type}_chg', 'start']]


def sum_estimates_to_target_area(pipeline, year, target_type, table):
    # target_type: 'total_pop' or 'units' or 'emp'
    # table: 'ofm_estimates' or 'employment'

    p = pipeline
    
    if target_type == 'emp':
        # get column name for employment excluding military, resource and construction
        emp_col = get_emp_no_mil_res_con_col(p, year)
        col_name = emp_col
    else:
        col_name = f'ofm_{target_type}'

    # get control area to target lookup
    xwalk = p.get_table('control_target_xwalk')
    
    # sum estimates by target areas
    df = (
        p.get_table(f'{table}_{year}_by_control_area')
        # add year suffix to ofm column
        .rename(columns={f'{col_name}':f'{target_type}_{year}'})
        # join to target ids
        .merge(xwalk[['control_id', 'target_id']], on='control_id', how='left')
        # groupby sum to target id
        .groupby('target_id').sum().reset_index()
        # return only target id and needed ofm column
        [['target_id', f'{target_type}_{year}']]
    )
    return df

def get_estimates_all_years(pipeline, start_years, target_type, table):
    p = pipeline
    base_year = p.settings['base_year']

    # create empty dataframe to hold all years of needed ofm columns
    est_all_years = pd.DataFrame()
    
    # loop through baseyear and start years and sum ofm to target area

    for start_year in list(set([base_year] + start_years)):
        ofm_df = sum_estimates_to_target_area(p, start_year, target_type, table)

        # merge to all years dataframe
        est_all_years = est_all_years.merge(ofm_df, on='target_id', how='outer') if not est_all_years.empty else ofm_df
    return est_all_years

def adjust_targets(pipeline, target_type, table):
    # target_type: 'pop' or 'units' or 'emp'
    # table: 'ofm_estimates' or 'employment'

    p = pipeline
    base_year = p.settings['base_year']

    # combine county targets
    df = combine_targets(p, target_type)

    # get unique start years in the targets
    start_years = df['start'].unique().tolist()

    # get estimates for all start years and base year amd merge to targets
    est_all_years = get_estimates_all_years(p, start_years, target_type, table)
    df = df.merge(est_all_years, on='target_id', how='left')

    # loop through each row to calculate change from target start year to base year
    for index, row in df.iterrows():
        start = int(row['start'])
        start_col = f'{target_type}_{start}'
        base_col = f'{target_type}_{base_year}'
        est_chg_col = f'est_{target_type}_chg'
        df.at[index, est_chg_col] = row[base_col] - row[start_col]

    chg_adj_col = f'{target_type}_chg_adj'
    chg_col = f'{target_type}_chg'
    if target_type == 'emp':
        df[est_chg_col] = df[est_chg_col].fillna(0).round(0).astype(int)
        df[chg_adj_col] = (df[chg_col] - df[est_chg_col])
    else:
        # fill NA, round and clip to 0 (no negative change)
        df[est_chg_col] = df[est_chg_col].fillna(0).round(0).clip(lower=0).astype(int)
        # adjust target change by subtracting est change, minimum of 0
        df[chg_adj_col] = (df[chg_col] - df[est_chg_col]).clip(lower=0)

    # save adjusted targets table
    table_name = f'adjusted_{target_type}_change_targets'
    out_df = df[['target_id','start',chg_col,chg_adj_col]]
    p.save_table(table_name,out_df)



def run_step(context):
    p = Pipeline(settings_path=context['configs_dir'])
    print("Adjusting targets to base year using OFM and Employment estimates...")
    adjust_targets(p,'units','ofm_parcelized')
    adjust_targets(p,'total_pop','ofm_parcelized')
    adjust_targets(p,'emp','employment')
    return context