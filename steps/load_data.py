import pandas as pd
from util import Pipeline

def load_data_tables_to_hdf5(pipeline):
    # load general data tables in the data_tables list in settings.yaml
    p = pipeline
    data_tables = p.settings.get('data_tables', [])
    for table in data_tables:
        table_name = table['name']
        file_path = f"{p.get_data_dir()}/{table['file']}"
        print(f"Loading {file_path} into HDF5 as {table_name}...")
        df = pd.read_csv(file_path)
        
        # check that the correct columns are present
        data_check_tables(df, table_name)

        # save to HDF5
        p.save_table(table_name, df)

def data_check_tables(df, table_name):
    if table_name == 'control_areas':
        if 'control_id' not in df.columns:
            raise ValueError("control_areas table must have control_id column.")


def load_targets_to_hdf5(pipeline):
    # load target tables for each county
    p = pipeline
    for table in p.settings['targets_tables']:
        table_name = table['name']
        file_path = f"{p.get_data_dir()}/{table['file']}"
        print(f"Loading {file_path} into HDF5 as {table_name}...")
        df = pd.read_csv(file_path)
        
        # rename columns based on settings
        for col in ['total_pop_chg_col', 'units_chg_col', 'emp_chg_col']:
            if col in table:
                df.rename(columns={table[f'{col}']: col.replace('_col', '')}, inplace=True, errors='ignore')
        
        # check that base year data exists for years specified in targets table settings
        check_base_year_data_exists(pipeline,table)

        # check that the correct columns are present
        data_check_targets(df, table_name)
        
        # save to HDF5
        p.save_table(table_name, df)


def check_exists(chg_col,targets_table, type, data_table_names):
    """
    Checks if the required data table exists for a given change column and type.

    Parameters:
        chg_col (str): The column name to check in the targets_table.
        targets_table (dict): Dictionary containing table info from settings.yaml.
        type (str): The type of data to check ('emp' for employment, 'ofm' for OFM estimates).
        data_table_names (list): List of available data table names.

    Raises:
        ValueError: If the required data table for the specified type and start year is not found in data_table_names.
    """
    if chg_col in targets_table:
        start_year = targets_table[chg_col]
        if type == 'emp':
            table_name = f'employment_{start_year}_by_regional_geography'
        elif type == 'ofm':
            table_name = f'ofm_estimates_{start_year}'
        if table_name not in data_table_names:
            raise ValueError(f"{type} data for start year {start_year} not found in data_tables in settings.yaml.")

def check_base_year_data_exists(pipeline,targets_table):
    p = pipeline

    # check that employment data exists for emp_chg_start year
    data_table_names = [table['name'] for table in p.get_data_table_list()]
    check_exists('emp_chg_start',targets_table,'emp',data_table_names)

    # check that ofm data exists for total_pop_chg_start and units_chg_start years
    elmer_table_names = [table['name'] for table in p.get_elmer_list()]
    check_exists('total_pop_chg_start',targets_table,'ofm',elmer_table_names)
    check_exists('units_chg_start',targets_table,'ofm',elmer_table_names)


def data_check_targets(df, table_name):
    # each targets table should have either units_chg or total_pop_chg, but not both
    # and each should have emp_chg and target_id
    if 'units_chg' in df.columns and 'total_pop_chg' in df.columns:
        raise ValueError(f"{table_name} cannot have both units_chg and total_pop_chg columns.")
    if 'emp_chg' not in df.columns:
        raise ValueError(f"{table_name} must have emp_chg column.")
    if 'target_id' not in df.columns:
        raise ValueError(f"{table_name} must have target_id column.")
    if 'units_chg' not in df.columns and 'total_pop_chg' not in df.columns:
        raise ValueError(f"{table_name} must have either units_chg or total_pop_chg column.")
    

def run_step(context):
    # pypyr step
    p = Pipeline(settings_path=context['configs_dir'])
    print("Loading data tables from CSV files into HDF5...")
    load_data_tables_to_hdf5(p)
    load_targets_to_hdf5(p)
    return context