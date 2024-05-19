from soda.scan import Scan
import json
import pandas as pd
from flatten_json import flatten

def save_to_csv(filename):
    """
    The function `save_to_csv` reads JSON data from a file, normalizes it into a DataFrame, and saves it
    as a CSV file with the same name as the input file.
    
    :param filename: The `save_to_csv` function takes a `filename` as a parameter. This function reads
    data from a JSON file specified by the `filename`, normalizes the JSON data into a pandas DataFrame,
    and then saves the DataFrame to a CSV file with the same name as the input JSON file but with
    """

    with open(filename, encoding="utf8") as f:
        data = json.load(f)

    df = pd.json_normalize(data, record_path=['metrics'])
    file_name = filename.split('.')[0]
    df.to_csv(f'{file_name}.csv')

def save_to_json(filename, data):
    """
    The function `save_to_json` saves data to a JSON file with specified filename.
    
    :param filename: The `filename` parameter is a string that represents the name of the file where the
    data will be saved in JSON format
    :param data: The `data` parameter in the `save_to_json` function is the information or data that you
    want to save to a JSON file. This data can be in the form of a dictionary, list, string, integer, or
    any other JSON serializable object that you want to write to the file
    """

    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

client_name = 'db_ca_kerry'

# initiates a soda core scan
scan = Scan()

# assigns the data source name incase of multiple data sources
scan.set_data_source_name(client_name)

# Add configuration YAML files. For BQ, its the service account key
scan.add_configuration_yaml_file(file_path="configuration.yml")


# Add date if required
#scan.add_variables({"date": "2024-01-28"})

# Add check YAML files. Contains all data quality checks for the defined dataset(s).
scan.add_sodacl_yaml_file("checks.yml")

# Execute the scan. Indicates whether scan was successful or not. Refer to soda website for definiton of different exit codes
exit_code = scan.execute()
print('exit_code is', exit_code)

# Set scan definition name, equivalent to CLI -s option
scan.set_scan_definition_name(client_name + '_dq_checks')

# Do not send results to Soda Cloud, equivalent to CLI -l option;
# scan.set_is_local(True)

# Set logs to verbose mode, equivalent to CLI -V option
scan.set_verbose(True)

# Gets the scan results
scan.get_scan_results()


# Assign output filename if need to save the scan results locally
filename = 'output.json'
output = scan.scan_results
# save_to_json(filename, output)
# save_to_csv(filename)

# Print results of scan
print(scan.get_logs_text())

# # Typical log inspection
# scan.assert_no_error_logs()
# # scan.assert_no_checks_fail()

# # Advanced methods to inspect scan execution logs
# scan.has_error_logs()
# scan.get_error_logs_text()

# Advanced methods to review check results details
########################################
scan.get_checks_fail()
scan.has_check_fails()
scan.get_checks_fail_text()
# scan.assert_no_checks_warn_or_fail()
# scan.get_checks_warn_or_fail()
# scan.has_checks_warn_or_fail()
# scan.get_checks_warn_or_fail_text()
# scan.get_all_checks_text()