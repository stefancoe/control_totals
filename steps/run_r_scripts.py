import subprocess


def run_step(context):
    command = ['Rscript', 'r_scripts/run_creating_control_totals_from_targets.R']

    # Run the command and capture the output
    try:
        # check_output raises a CalledProcessError if the command returns a non-zero exit status
        output = subprocess.check_output(command, universal_newlines=True, stderr=subprocess.PIPE)
        print('R OUTPUT:\n', output)
    except subprocess.CalledProcessError as e:
        print('R ERROR:\n', e.stderr)
    except FileNotFoundError:
        print("Error: Rscript not found. Make sure R is installed and in your system's PATH.")
    
    return context
