
import pandas as pd
PATH = "/opt/airflow/s3-drive"
source_path = f"{PATH}/Stocks/"
save_path = f"{PATH}//Output/"
index_file_path = f"{PATH}/CompanyNames/top_companies.txt"

def extract_companies_from_index(index_file_path):
    company_file = open(index_file_path, "r")
    contents = company_file.read()
    contents = contents.replace("'","")
    contents_list = contents.split(",")
    cleaned_contents_list = [item.strip() for item in contents_list]
    company_file.close()
    return cleaned_contents_list


def get_path_to_company_data(list_of_companies, source_data_path):
    path_to_company_data = []
    for file_name in list_of_companies:
        path_to_company_data.append(source_data_path + file_name)
    return path_to_company_data


def save_table(dataframe, output_path, file_name, header):
    print(f"Path = {output_path}, file = {file_name}")
    dataframe.to_csv(output_path + file_name + ".csv", index=False, header=header)

def data_processing(file_paths, output_path):
    output_data = pd.DataFrame()

    i = 0
    for file_path in file_paths:
        try:
            data = pd.read_csv(file_path, parse_dates=['Dates']).drop('OpenInt', axis=1)
            data['daily_percent_change'] = (data['Close'] - data['Open']) * 100
            data['value_change'] = data['Close'] - data['Open']
            data['company_name'] = file_path.split("/")[-1].split(".")[0]

            output_data = output_data.append(data)
        except Exception as e:
            print(f"Error Processing File: {file_path}")
            print(f"Error Message: {str(e)}")

    output_data.columns = ['stock_date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_traded', 'daily_percent_change', 'value_change', 'company_name']
    save_table(output_data, output_path, 'historical_stock_data', header=False)

if __name__ == "__main__":
    file_names = extract_companies_from_index(index_file_path)
    path_to_company_data = get_path_to_company_data(file_names, source_path)
    data_processing(path_to_company_data, save_path)

