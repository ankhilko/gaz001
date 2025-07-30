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


def merge_csv_preserve_headers(
        csv1_path: str,
        csv2_path: str,
        output_path: str = None,
        csv1_key_col: int = 1,  # Ключ во 2-м столбце (индекс 1)
        csv1_target_col: int = 4,  # Целевой столбец в 1-м файле (5-й столбец, индекс 4)
        csv2_key_col: int = 0,  # Ключ в 1-м столбце (индекс 0)
        csv2_value_col: int = 5,  # Значение в 6-м столбце (индекс 5)
        keep_unmatched: bool = True,
        case_sensitive: bool = False,
        strip_spaces: bool = True
) -> pd.DataFrame:
    """
    Заменяет данные в 5-м столбце первого CSV на значения из 6-го столбца второго CSV,
    сохраняя заголовки (первую строку) неизменными.

    Параметры:
    -----------
    csv1_path : str
        Путь к основному CSV (данные будут изменены)
    csv2_path : str
        Путь к CSV с данными для подстановки
    output_path : str, optional
        Если указан, сохраняет результат в CSV
    csv1_key_col : int, optional (default=1)
        Столбец с ключом в первом файле (индекс)
    csv1_target_col : int, optional (default=4)
        Столбец для замены в первом файле (5-й столбец, индекс 4)
    csv2_key_col : int, optional (default=0)
        Столбец с ключом во втором файле (индекс)
    csv2_value_col : int, optional (default=5)
        Столбец с данными во втором файле (6-й столбец, индекс 5)
    keep_unmatched : bool, optional (default=True)
        Сохранять строки без совпадений?
    case_sensitive : bool, optional (default=False)
        Учитывать регистр?
    strip_spaces : bool, optional (default=True)
        Удалять пробелы в ключах?

    Возвращает:
    -----------
    pd.DataFrame
        Результат объединения с сохранёнными заголовками
    """

    # Загрузка данных с сохранением заголовков
    df1 = pd.read_csv(csv1_path, header=0)  # Первая строка - заголовок
    df2 = pd.read_csv(csv2_path, header=0)  # Первая строка - заголовок

    # Сохраняем заголовки
    headers1 = df1.columns.tolist()
    headers2 = df2.columns.tolist()

    # Функция обработки ключей
    def process_key(key):
        key = str(key) if pd.notna(key) else ""
        if strip_spaces:
            key = key.strip()
        if not case_sensitive:
            key = key.lower()
        return key

    # Создаем словарь для подстановки {ключ: значение}
    value_dict = {
        process_key(k): v
        for k, v in zip(
            df2.iloc[:, csv2_key_col],
            df2.iloc[:, csv2_value_col]
        )
        if pd.notna(k)
    }

    # Копируем DataFrame для безопасности
    result = df1.copy()

    # Заменяем значения в целевом столбце (кроме заголовка)
    result.iloc[0:, csv1_target_col] = (
        result.iloc[0:, csv1_key_col]
        .apply(process_key)
        .map(value_dict)
    )

    # Удаляем строки без совпадений (если нужно, кроме заголовка)
    if not keep_unmatched:
        result = result.dropna(subset=[result.columns[csv1_target_col]])

    # Сохранение с заголовками (если указан путь)
    if output_path:
        result.to_csv(output_path, index=False)

    return result


def merge_csv_files(
    file_1_path: str,
    file_2_path: str,
    output_path: str = None,
    key_column_1: str = "(номер без @ и без -)",
    key_column_2: str = "Номер без разделителей",
    columns_to_add: list = [
        "Клиент",
        "Поставщик",
        "Бренд",
        "Номер",
        "Описание",
        "Тип оплаты",
        "Кол.",
        "Цена продажи",
        "Вес",
        "Адрес доставки",
        "Создал",
    ],
) -> None:
    """
    Добавляет в file_1.csv новые столбцы из file_2.csv по совпадению ключей.

    Параметры:
        file_1_path (str): Путь к основному CSV-файлу, который нужно дополнить.
        file_2_path (str): Путь к CSV-файлу с дополнительными данными.
        output_path (str, optional): Куда сохранить результат. Если None, перезаписывает file_1.csv.
        key_column_1 (str): Название ключевого столбца в file_1.csv.
        key_column_2 (str): Название ключевого столбца в file_2.csv.
        columns_to_add (list): Список столбцов для добавления из file_2.csv.
    """
    # Загружаем оба файла
    df1 = pd.read_csv(file_1_path)
    df2 = pd.read_csv(file_2_path)

    # Проверяем, что ключевые столбцы существуют
    if key_column_1 not in df1.columns:
        raise ValueError(f"Столбец '{key_column_1}' не найден в {file_1_path}")
    if key_column_2 not in df2.columns:
        raise ValueError(f"Столбец '{key_column_2}' не найден в {file_2_path}")

    # Проверяем, что запрашиваемые столбцы есть в file_2
    missing_columns = [col for col in columns_to_add if col not in df2.columns]
    if missing_columns:
        raise ValueError(f"Столбцы {missing_columns} не найдены в {file_2_path}")

    # Выбираем только нужные столбцы из file_2
    df2_selected = df2[[key_column_2] + columns_to_add]

    # Объединяем данные
    merged_df = df1.merge(
        df2_selected,
        how="left",
        left_on=key_column_1,
        right_on=key_column_2,
    )

    # Удаляем временный ключевой столбец из file_2 (если он отличается)
    if key_column_1 != key_column_2:
        merged_df.drop(columns=[key_column_2], inplace=True)

    # Сохраняем результат
    output_path = output_path or file_1_path
    merged_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Файл успешно сохранён: {output_path}")


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


def replace_missing_country(csv_file_path, column_name, new_value):
    df = pd.read_csv(csv_file_path)
    df[column_name] = df[column_name].replace('--', new_value)
    df.to_csv(csv_file_path, index=False, encoding='utf-8')


def save_to_csv(data_df, output_file="результат.csv"):
    try:
        # Добавляем новый столбец
        if 'А' in data_df.columns:
            data_df.insert(1, clean_number,
                           data_df['А'].astype(str).str.replace('[@-]', '', regex=True).str.strip())

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


def csv_to_xlsx(csv_file_path, xlsx_file_path=None):
    """
    Конвертирует CSV-файл в XLSX-файл.

    Параметры:
    - csv_file_path: str - путь к исходному CSV-файлу
    - xlsx_file_path: str (опциональный) - путь для сохранения XLSX-файла.
      Если не указан, будет использовано то же имя файла, что у CSV, но с расширением .xlsx

    Возвращает:
    - str - путь к сохранённому XLSX-файлу
    """
    # Читаем CSV-файл
    df = pd.read_csv(csv_file_path)

    # Если путь для XLSX не указан, создаём его из пути CSV
    if xlsx_file_path is None:
        if csv_file_path.lower().endswith('.csv'):
            xlsx_file_path = csv_file_path[:-4] + '.xlsx'
        else:
            xlsx_file_path = csv_file_path + '.xlsx'

    # Сохраняем в XLSX
    df.to_excel(xlsx_file_path, index=False, engine='openpyxl')

    return xlsx_file_path


def xlsx_to_csv(xlsx_file_path, csv_file_path=None, sheet_name=0, delimiter=','):
    """
    Конвертирует XLSX-файл в CSV.

    Параметры:
    - xlsx_file_path: str - путь к исходному XLSX-файлу.
    - csv_file_path: str (опциональный) - путь для сохранения CSV.
      Если не указан, будет использовано то же имя, что у XLSX, но с расширением .csv.
    - sheet_name: str/int (опциональный) - имя или номер листа в XLSX (по умолчанию первый лист).
    - delimiter: str (опциональный) - разделитель для CSV (по умолчанию ',').

    Возвращает:
    - str - путь к сохранённому CSV-файлу.
    """
    # Читаем XLSX-файл
    df = pd.read_excel(xlsx_file_path, sheet_name=sheet_name)

    # Если путь для CSV не указан, создаём его из пути XLSX
    if csv_file_path is None:
        if xlsx_file_path.lower().endswith('.xlsx'):
            csv_file_path = xlsx_file_path[:-4] + 'csv'
        else:
            csv_file_path = xlsx_file_path + '.csv'

    # Сохраняем в CSV
    df.to_csv(csv_file_path, index=False, sep=delimiter)

    return csv_file_path


def xls_to_csv(xls_file_path, csv_file_path=None, sheet_name=0, delimiter=','):
    """
    Устойчивая конвертация XLS/XLSX в CSV с автоматическим выбором движка.
    """
    try:
        # Пробуем openpyxl для XLSX
        df = pd.read_excel(xls_file_path, sheet_name=sheet_name, engine='openpyxl')
    except:
        try:
            # Пробуем xlrd для старых XLS
            df = pd.read_excel(xls_file_path, sheet_name=sheet_name, engine='xlrd')
        except Exception as e:
            raise ValueError(f"Не удалось прочитать файл: {str(e)}")

    if csv_file_path is None:
        csv_file_path = xls_file_path.rsplit('.', 1)[0] + '.csv'

    df.to_csv(csv_file_path, index=False, sep=delimiter)
    return csv_file_path


if __name__ == "__main__":
    folder_path = "upd"
    folder_report_abcp_xls = "report_abcp/report_20250730_114138.xls"
    folder_report_abcp = "report_abcp"
    report_abcp_xls = glob(os.path.join(folder_report_abcp, "*.xls"))[0]
    folder_report_abcp_csv = xls_to_csv(report_abcp_xls)
    folder_spravochnik_tnved_xlsx = "tnved/справочниктнвэд.xlsx"
    folder_spravochnik_tnved_csv = xlsx_to_csv(folder_spravochnik_tnved_xlsx)
    target_path_as_csv = "all_data_file.csv"
    temp_file = "temp_data_file.csv"
    csv_file = "all_data_file.csv"

    excel_files = glob(os.path.join(folder_path, "*.xls*"))


    new_path = input(f'Введите путь к папке с UPD-файлами в XLS/XLSX (стандартно - это папка "{folder_path}"): ')
    if new_path:
        folder_path = new_path
    new_path = input(f'Введите путь к папке с файлами ABCP (стандартно - это папка "{report_abcp_xls}"): ')
    if new_path:
        report_abcp_xls = new_path
    new_path = input(f'Введите путь к справочнику ТН ВЭД (стандартно - это текущая папка и файл "{folder_spravochnik_tnved_xlsx}"): ')
    if new_path:
        folder_spravochnik_tnved_xlsx = new_path
    new_path = input("Введите путь к файлу all_data_file.csv (стандартно - это текущая папка): ")
    if new_path:
        target_path_as_csv = os.path.join(new_path, "all_data_file.csv")

    if not os.path.exists(target_path_as_csv):
        # Создаем DataFrame с нужными колонками, включая новую колонку
        pd.DataFrame(columns=COLUMN_ORDER).to_csv(target_path_as_csv, index=False, encoding='utf-8-sig')

    for file in excel_files:
        tables = find_and_extract_tables(file)
        for i, table in enumerate(tables, 1):
            print(f"Обработка таблицы {i} из файла {file}")
            save_to_csv(table, temp_file)
            merge_csv_by_headers(temp_file, target_path_as_csv)

    if os.path.exists(temp_file):
        os.remove(temp_file)

    merge_csv_preserve_headers(target_path_as_csv, folder_spravochnik_tnved_csv, target_path_as_csv)
    replace_missing_country(target_path_as_csv, "10а", "РОССИЯ")

    merge_csv_files(target_path_as_csv, folder_report_abcp_csv)

    csv_to_xlsx(target_path_as_csv)

    print("Обработка всех файлов завершена!")

"""
Linux and Mac:
Terminal
pip install openpyxl xlrd xlwt pandas pyxlsb

Windows:
CMD
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null openpyxl
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null xlrd
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null xlwt
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null pandas
pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --cert /dev/null pyxlsb
"""
