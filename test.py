import os
import pandas as pd
from glob import glob
import re


def is_valid_string(s):
    # Проверяем, что строка состоит только из цифр (0-9), точек (.) и пробелов (\s)
    return bool(re.fullmatch(r'^[\d.\s]+$', s))


data_to_parse = [
    ['Продавец:', '(2)'],
    ['ИНН/КПП продавца:', '(2б)'],
    ['Документ об отгрузке', '(5а)'],
]

data_to_parse_no_index = [
    ['Продавец', '(2)'],
    ['ИНН/КПП продавца', '(2б)'],
    ['Документ об отгрузке:', '(5а)'],
]

target_headers = [
    "А", "1", "1а", "1б", "2", "2а", "3", "4", "5", "6", "7", "8", "9", "10", "10а", "11", "12", "12а", "13", "14",
    "(5а)", "(2)", "(2б)"
]
clean_number = "(номер без @ и без -)"
COLUMN_ORDER = [
    "А", clean_number, "1", "1а", "1б", "2", "2а", "3",
    "4", "5", "6", "7", "8", "9", "10", "10а", "11", "12",
    "12а", "13", "14", "(5а)", "(2)", "(2б)"
]


def merge_csv_by_headers(source_path, target_path):
    try:
        # Чтение данных
        source_df = pd.read_csv(source_path)
        target_df = pd.read_csv(target_path)

        # Проверка на пустые данные
        if source_df.empty:
            print(f"⚠️ Источник {source_path} пуст - пропускаем")
            return

        if target_df.empty:
            print(f"⚠️ Цель {target_path} пуста - создаем новый")
            # Приводим столбцы к нужному порядку перед сохранением
            ordered_df = source_df.reindex(columns=COLUMN_ORDER)
            ordered_df.to_csv(target_path, index=False, encoding='utf-8-sig')
            return

        # Приводим оба DataFrame к нужному порядку столбцов
        source_df = source_df.reindex(columns=COLUMN_ORDER)
        target_df = target_df.reindex(columns=COLUMN_ORDER)

        # Объединение
        merged_df = pd.concat([target_df, source_df], ignore_index=True)

        # Убедимся, что порядок сохранился
        merged_df = merged_df[COLUMN_ORDER]

        # Сохранение
        merged_df.to_csv(target_path, index=False, encoding='utf-8-sig')
        print(f"✅ Успешно объединено {len(source_df)} записей в {target_path}")

    except Exception as e:
        print(f"❌ Ошибка при объединении {source_path} -> {target_path}: {str(e)}")

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

    found_rows = []
    for index, row in df.iterrows():
        row_str = '|'.join(row.astype(str)).lower()

        if data_to_get[0].lower() in row_str:
            non_empty_cells = [file_path] + [cell for cell in row if str(cell).strip() not in ('', 'nan')]
            found_rows.append(non_empty_cells)

    return found_rows


def save_to_csv(data_df, output_file="результат.csv"):
    try:
        # Добавляем новый столбец
        if 'А' in data_df.columns:
            data_df.insert(1, clean_number,
                           data_df['А'].astype(str).str.replace('[@-]', '', regex=True))

        # Приводим к нужному порядку столбцов
        data_df = data_df.reindex(columns=COLUMN_ORDER)

        # Сохраняем
        data_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ Таблица успешно сохранена в файл: {output_file}")

    except Exception as e:
        print(f"❌ Ошибка при сохранении: {e}")

def find_and_extract_tables(file_path):
    print(f"Обработка файла: {file_path}")

    if file_path.lower().endswith('.xls'):
        df_list = [pd.read_excel(file_path, header=None, engine='xlrd')]
    elif file_path.lower().endswith('xlsx'):
        # Для xlsx читаем все листы
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        df_list = [pd.read_excel(xls, sheet_name=sheet, header=None)
                   for sheet in xls.sheet_names]

    all_tables = []

    for df in df_list:
        df = df.fillna('')
        tables_in_sheet = []
        current_table_start = None

        for row_idx in range(len(df)):
            row_values = [str(cell) for cell in df.iloc[row_idx].values if str(cell).strip() not in ('', 'nan')]

            if all(header in row_values for header in target_headers[:6]):
                if current_table_start is not None:
                    tables_in_sheet.append((current_table_start, row_idx - 1))
                current_table_start = row_idx
            elif current_table_start is not None and len(row_values) == 0:
                tables_in_sheet.append((current_table_start, row_idx - 1))
                current_table_start = None

        if current_table_start is not None:
            tables_in_sheet.append((current_table_start, len(df) - 1))

        for start, end in tables_in_sheet:
            try:
                data_df = pd.read_excel(
                    file_path,
                    header=start,
                    usecols=lambda x: str(x) in target_headers,
                    nrows=end - start,
                    engine='openpyxl' if file_path.lower().endswith('xlsx') else 'xlrd'
                )

                # Удаляем строки, где 4-я колонка пустая
                if len(data_df.columns) >= 4:  # Проверяем, что в DF есть хотя бы 4 колонки
                    data_df = data_df[data_df[data_df.columns[3]].notna()]  # Удаляем пустые

                data_df = data_df.dropna(how='all')

                for i in range(len(data_to_parse)):
                    to_add = parse_xls_xlsx_get_data(file_path, data_to_parse[i])
                    if to_add and len(to_add[0]) >= 4:
                        data_df[to_add[0][3]] = to_add[0][2]
                    else:
                        to_add1 = parse_xls_xlsx_get_data(file_path, data_to_parse_no_index[i])
                        if 'тот' in to_add1[0][2]:
                            to_add2 = parse_xls_xlsx_get_data(file_path, 'Счет-фактура')
                            data_df[data_to_parse_no_index[i][1]] = to_add2[0][2]
                        else:
                            data_df[data_to_parse_no_index[i][1]] = to_add1[0][2]

                all_tables.append(data_df)

            except Exception as e:
                print(f"Ошибка при обработке таблицы (строки {start}-{end}): {e}")

    return all_tables


if __name__ == "__main__":
    folder_path = "upd"
    folder_path_2 = "report_abcp"
    folder_path_3 = "справочниктнвэд.xlsx"
    target_path = "all_data_file.csv"
    temp_file = "temp_data_file.csv"
    csv_file = "all_data_file.csv"

    excel_files = glob(os.path.join(folder_path, "*.xls*"))

    new_path = input(f'Введите путь к папке с UPD-файлами в XLS/XLSX (стандартно - это папка "{folder_path}"): ')
    if new_path:
        folder_path = new_path
    new_path = input(f'Введите путь к папке с файлами ABCP (стандартно - это папка "{folder_path_2}"): ')
    if new_path:
        folder_path_2 = new_path
    new_path = input(f'Введите путь к справочнику ТН ВЭД (стандартно - это текущая папка и файл "{folder_path_3}"): ')
    if new_path:
        folder_path_3 = new_path
    new_path = input("Введите путь к файлу all_data_file.csv (стандартно - это текущая папка): ")
    if new_path:
        target_path = os.path.join(new_path, "all_data_file.csv")

    if not os.path.exists(target_path):
        # Создаем DataFrame с нужными колонками, включая новую колонку
        pd.DataFrame(columns=COLUMN_ORDER).to_csv(target_path, index=False, encoding='utf-8-sig')

    for file in excel_files:
        tables = find_and_extract_tables(file)
        for i, table in enumerate(tables, 1):
            print(f"Обработка таблицы {i} из файла {file}")
            save_to_csv(table, temp_file)
            merge_csv_by_headers(temp_file, target_path)

    if os.path.exists(temp_file):
        os.remove(temp_file)

    print("Обработка всех файлов завершена!")


import pandas as pd


def merge_dataframes(dp1: pd.DataFrame, dp2: pd.DataFrame) -> pd.DataFrame:
    """
    Объединяет dp1 и dp2 по первому столбцу, подставляя значения из 6-го столбца dp2

    Параметры:
    dp1 - исходный DataFrame (ключ в первом столбце)
    dp2 - DataFrame из которого берутся значения (ключ в первом столбце, данные в 6-м)

    Возвращает:
    Новый DataFrame с добавленными данными из dp2
    """
    # Копируем исходный dataframe, чтобы не изменять оригинал
    result = dp1.copy()

    # Создаем словарь из dp2: ключ - первый столбец, значение - шестой столбец
    value_dict = pd.Series(dp2.iloc[:, 5].values, index=dp2.iloc[:, 0]).to_dict()

    # Создаем новый столбец в result с значениями из словаря по ключу
    # Используем map для сопоставления значений
    result['new_column'] = result.iloc[:, 0].map(value_dict)

    return result