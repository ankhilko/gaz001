import os
import pandas as pd
from glob import glob
import openpyxl
from openpyxl.utils import get_column_letter

data_to_parse = [
    ['Документ об отгрузке', '(5а)'],
    ['Продавец:', '(2)'],
    ['ИНН/КПП продавца:', '(2б)'],
]
target_headers = [
    "А", "1", "1а", "1б", "2", "2а", "3", "4", "5", "6", "7", "8", "9", "10", "10а", "11", "12", "12а", "13", "14"
]


def merge_xlsx_by_headers(source_path, target_path):
    source_wb = openpyxl.load_workbook(source_path)
    target_wb = openpyxl.load_workbook(target_path)

    source_sheet = source_wb.active
    target_sheet = target_wb.active

    target_headers = []
    for cell in target_sheet[1]:
        target_headers.append(cell.value)

    source_headers = []
    for cell in source_sheet[1]:
        source_headers.append(cell.value)

    column_mapping = {}
    for idx, header in enumerate(target_headers, 1):
        if header in source_headers:
            source_col = source_headers.index(header) + 1  # +1 т.к. индексы с 1
            column_mapping[idx] = source_col
        else:
            column_mapping[idx] = None  # Если столбца нет в исходном файле

    last_target_row = target_sheet.max_row

    for source_row in range(2, source_sheet.max_row + 1):  # Начинаем со 2 строки (пропускаем заголовки)
        last_target_row += 1
        for target_col, source_col in column_mapping.items():
            if source_col is not None:
                cell_value = source_sheet.cell(row=source_row, column=source_col).value
                target_sheet.cell(row=last_target_row, column=target_col, value=cell_value)

    target_wb.save(target_path)
    print(f"Данные успешно объединены в файл: {target_path}")


def parse_xls_xlsx_get_data(file_path, data_to_get):
    try:
        if file_path.lower().endswith('.xls'):
            df = pd.read_excel(file_path, header=None, engine='xlrd')
        elif file_path.lower().endswith('xlsx'):
            df = pd.read_excel(file_path, header=None, engine='openpyxl')
        df = df.fillna('')  # NaN
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return

    # print(df.iloc[4].to_string())

    found_rows = []
    for index, row in df.iterrows():
        row_str = '|'.join(row.astype(str)).lower()  # объединить строку
        if (data_to_get[0].lower() in row_str) and (data_to_get[1] in row_str):
            non_empty_cells = [file_path] + [cell for cell in row if str(cell).strip() not in ('', 'nan')]
            found_rows.append(non_empty_cells)

    return found_rows


def save_to_excel(data_df, output_file="результат.xlsx"):
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            data_df.to_excel(
                writer,
                index=False,  # Не записывать индексы строк
                sheet_name='Данные',  # Название листа
            )
            worksheet = writer.sheets['Данные']
            for column in worksheet.columns:
                max_length = max(len(str(cell.value)) for cell in column)
                worksheet.column_dimensions[column[0].column_letter].width = max_length + 2

        print(f"✅ Таблица успешно сохранена в файл: {output_file}")

    except Exception as e:
        print(f"❌ Ошибка при сохранении: {e}")


def find_and_extract_table(file_path):

    print(file_path)

    if file_path.lower().endswith('.xls'):
        df = pd.read_excel(file_path, header=None, engine='xlrd')
    elif file_path.lower().endswith('xlsx'):
        df = pd.read_excel(file_path, header=None, engine='openpyxl')

    for row_idx in range(len(df)):
        row_values = [str(cell) for cell in df.iloc[row_idx].values if pd.notna(cell)]

        if all(header in row_values for header in target_headers[:6]):
            data_df = pd.read_excel(
                file_path,
                header=row_idx,
                usecols=lambda x: str(x) in target_headers
            )

            data_df = data_df.dropna(how='all')

            first_empty_row = None
            for i in range(len(data_df)):
                if pd.isna(data_df.iloc[i, 0]):  # Проверяем первую ячейку строки
                    first_empty_row = i
                    break

            if first_empty_row is not None:
                data_df = data_df.iloc[:first_empty_row]

            for data_set in data_to_parse:
                to_add = parse_xls_xlsx_get_data(file_path, data_set)

                data_df[to_add[0][3]] = to_add[0][2]

            return data_df


if __name__ == "__main__":

    folder_path = "upd"
    target_path = "all_data_file.xlsx"
    temp_file = "temp_data_file.xlsx"

    excel_files = glob(os.path.join(folder_path, "*.xls*"))

    new_path = input(f'введите путь к папке с файлами XLSX и XLS (стандартно - это папка "{folder_path}"): ')
    if new_path:
        folder_path = new_path
    new_path = input("введите путь к папке с файлом all_data_file.xlsx (стандартно - это текущая папка): ")
    if new_path:
        target_path = new_path

    for file in excel_files:
        result_df = find_and_extract_table(file)
        save_to_excel(result_df, temp_file)
        merge_xlsx_by_headers(temp_file, target_path)

"""
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null openpyxl
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null xlrd  
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null pandas 
"""
