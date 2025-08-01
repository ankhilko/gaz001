import os
import pandas as pd
from glob import glob
import re


def is_valid_string(s):
    # Проверяем, что строка состоит только из цифр (0-9), точек (.) и пробелов (\s)
    return bool(re.fullmatch(r'^[\d.\s]+$', s))


DATA_TO_PARSE = [
    ['Продавец:', '(2)'],
    ['ИНН/КПП продавца:', '(2б)'],
    ['Документ об отгрузке', '(5а)'],
]

DATA_TO_PARSE_NO_INDEX = [
    ['Продавец', '(2)'],
    ['ИНН/КПП продавца', '(2б)'],
    ['Документ об отгрузке:', '(5а)'],
]

TARGET_HEADERS = [
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
    Гарантирует, что первый и второй столбцы обрабатываются как строки (object).
    """
    # Загрузка данных с сохранением заголовков
    df1 = pd.read_csv(csv1_path, header=0, dtype={0: 'object', 1: 'object'})
    df2 = pd.read_csv(csv2_path, header=0, dtype={0: 'object', 1: 'object'})

    # Явное преобразование первых двух столбцов к строковому типу
    df1.iloc[:, 0] = df1.iloc[:, 0].astype('object')
    df1.iloc[:, 1] = df1.iloc[:, 1].astype('object')
    df2.iloc[:, 0] = df2.iloc[:, 0].astype('object')
    df2.iloc[:, 1] = df2.iloc[:, 1].astype('object')

    # Остальной код остается без изменений
    headers1 = df1.columns.tolist()
    headers2 = df2.columns.tolist()

    def process_key(key):
        key = str(key) if pd.notna(key) else ""
        if strip_spaces:
            key = key.strip()
        if not case_sensitive:
            key = key.lower()
        return key

    value_dict = {
        process_key(k): v
        for k, v in zip(
            df2.iloc[:, csv2_key_col],
            df2.iloc[:, csv2_value_col]
        )
        if pd.notna(k)
    }

    result = df1.copy()
    result.iloc[0:, csv1_target_col] = (
        result.iloc[0:, csv1_key_col]
        .apply(process_key)
        .map(value_dict)
    )

    if not keep_unmatched:
        result = result.dropna(subset=[result.columns[csv1_target_col]])

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
    Гарантирует, что первый и второй столбцы обрабатываются как строки (object).
    """
    # Загружаем оба файла с явным указанием типов для первых двух столбцов
    df1 = pd.read_csv(file_1_path, dtype={0: 'object', 1: 'object'})
    df2 = pd.read_csv(file_2_path, dtype={0: 'object', 1: 'object'})

    # Явное преобразование первых двух столбцов
    df1.iloc[:, 0] = df1.iloc[:, 0].astype('object')
    df1.iloc[:, 1] = df1.iloc[:, 1].astype('object')
    df2.iloc[:, 0] = df2.iloc[:, 0].astype('object')
    df2.iloc[:, 1] = df2.iloc[:, 1].astype('object')

    # Остальной код остается без изменений
    if key_column_1 not in df1.columns:
        raise ValueError(f"Столбец '{key_column_1}' не найден в {file_1_path}")
    if key_column_2 not in df2.columns:
        raise ValueError(f"Столбец '{key_column_2}' не найден в {file_2_path}")

    missing_columns = [col for col in columns_to_add if col not in df2.columns]
    if missing_columns:
        raise ValueError(f"Столбцы {missing_columns} не найдены в {file_2_path}")

    df2_selected = df2[[key_column_2] + columns_to_add]

    merged_df = df1.merge(
        df2_selected,
        how="left",
        left_on=key_column_1,
        right_on=key_column_2,
    )

    if key_column_1 != key_column_2:
        merged_df.drop(columns=[key_column_2], inplace=True)

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


def clean_and_convert_to_float(df, columns):
    """
    Очищает указанные колонки от лишних пробелов и преобразует их в тип float.

    Параметры:
    df (pd.DataFrame): Исходный DataFrame
    columns (list): Список колонок для обработки

    Возвращает:
    pd.DataFrame: Новый DataFrame с обработанными колонками
    """
    # Создаём копию DataFrame, чтобы не изменять исходный
    new_df = df.copy()

    for col in columns:
        if col in new_df.columns:
            try:
                # Удаляем лишние пробелы и преобразуем в float
                new_df[col] = (
                    new_df[col]
                    .astype(str)  # Преобразуем в строку на случай, если это другой тип
                    .str.strip()  # Удаляем пробелы в начале и конце
                    .str.replace(' ',
                                 '')  # Удаляем все пробелы (если нужно оставить десятичные пробелы, измените эту строку)
                    .replace('', pd.NA)  # Пустые строки заменяем на NA
                    .astype('float64')  # Преобразуем в float
                )
            except Exception as e:
                raise print(f"Предупреждение: ячейка '{col}' содержит нечисловое значение")
        else:
            print(f"Предупреждение: Колонка '{col}' не найдена в DataFrame")
    return new_df


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
    df[column_name] = df[column_name].replace('----', new_value).replace('--', new_value).replace('-', new_value)
    df.to_csv(csv_file_path, index=False, encoding='utf-8')


def save_to_csv(data_df, output_file="результат.csv"):
    try:
        # Добавляем новый столбец
        if 'А' in data_df.columns:

            data_df.insert(1, clean_number,
                           data_df['А'].astype('object').str.replace('[@-]', '', regex=True).str.strip())

        # Явное преобразование первых двух столбцов
        if len(data_df.columns) >= 1:
            data_df.iloc[:, 0] = data_df.iloc[:, 0].astype('object')
        if len(data_df.columns) >= 2:
            data_df.iloc[:, 1] = data_df.iloc[:, 1].astype('object')

        # Приводим к нужному порядку столбцов
        data_df = data_df.reindex(columns=COLUMN_ORDER)

        # Сохраняем
        data_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"✅ Таблица успешно сохранена в файл: {output_file}")

    except Exception as e:
        print(f"❌ Ошибка при сохранении: {e}")


def find_and_extract_tables(file_path):
    """
    Функция для поиска и извлечения таблиц из Excel-файла (xls/xlsx) по заданным заголовкам.

    Параметры:
    file_path (str): Путь к файлу Excel для обработки

    Возвращает:
    list: Список DataFrame с извлеченными таблицами

    Логика работы:
    1. Чтение файла Excel (всех листов для xlsx)
    2. Поиск таблиц по совпадению целевых заголовков
    3. Извлечение и очистка найденных таблиц
    4. Добавление дополнительных данных из файла
    """
    print(f"Обработка файла: {file_path}")

    # Чтение файла Excel в зависимости от формата
    if file_path.lower().endswith('.xls'):
        # Для старых xls файлов используем xlrd
        df_list = [pd.read_excel(file_path, header=None, engine='xlrd')]
    elif file_path.lower().endswith('xlsx'):
        # Для xlsx читаем все листы с помощью openpyxl
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        df_list = [pd.read_excel(xls, sheet_name=sheet, header=None)
                   for sheet in xls.sheet_names]

    all_tables = []  # Список для хранения всех найденных таблиц

    # Обработка каждого листа/DataFrame
    for df in df_list:
        df = df.fillna('')  # Заменяем NaN на пустые строки
        tables_in_sheet = []  # Список для хранения диапазонов таблиц на текущем листе
        current_table_start = None  # Индекс начала текущей таблицы

        # Поиск таблиц по заголовкам
        for row_idx in range(len(df)):

            # Получаем непустые значения строки
            row_values = [str(cell) for cell in df.iloc[row_idx].values if str(cell).strip() not in ('', 'nan')]

            # Проверяем, содержит ли строка все целевые заголовки
            if all(header in row_values for header in TARGET_HEADERS[:6]):
                if current_table_start is not None:
                    # Если уже была начата таблица, сохраняем предыдущую
                    tables_in_sheet.append((current_table_start, row_idx - 1))
                current_table_start = row_idx  # Начинаем новую таблицу
            elif current_table_start is not None and len(row_values) == 0:
                # Если встретили пустую строку после начала таблицы
                tables_in_sheet.append((current_table_start, row_idx - 1))
                current_table_start = None

        # Добавляем последнюю таблицу, если она не была закрыта
        if current_table_start is not None:
            tables_in_sheet.append((current_table_start, len(df) - 1))

        # Извлечение данных для каждой найденной таблицы
        for start, end in tables_in_sheet:
            try:
                # Чтение таблицы с нужными колонками
                data_df = pd.read_excel(
                    file_path,
                    header=start,  # Используем строку с заголовками как заголовки DF
                    usecols=lambda x: str(x) in TARGET_HEADERS,  # Фильтруем только нужные колонки
                    nrows=end - start,  # Ограничиваем количество строк
                    engine='openpyxl' if file_path.lower().endswith('xlsx') else 'xlrd',
                    dtype='object',
                )

                # Очистка данных:
                # Удаляем строки, где 4-я колонка пустая
                if len(data_df.columns) >= 6:  # Проверяем, что в DF есть хотя бы 4 колонки
                    data_df = data_df[data_df[data_df.columns[5]].notna()]  # Удаляем пустые

                data_df = data_df.dropna(how='all')  # Удаляем полностью пустые строки

                # Добавление дополнительных данных из файла
                for i in range(len(DATA_TO_PARSE)):
                    to_add = parse_xls_xlsx_get_data(file_path, DATA_TO_PARSE[i])
                    if to_add and len(to_add[0]) >= 4:
                        data_df[to_add[0][3]] = to_add[0][2]  # Добавляем данные в DF
                    else:
                        # Альтернативный поиск данных, если первый вариант не сработал
                        to_add1 = parse_xls_xlsx_get_data(file_path, DATA_TO_PARSE_NO_INDEX[i])
                        if 'тот' in to_add1[0][2]:
                            # Особый случай для определенного ключевого слова
                            to_add2 = parse_xls_xlsx_get_data(file_path, 'Счет-фактура')
                            data_df[DATA_TO_PARSE_NO_INDEX[i][1]] = to_add2[0][2]
                        else:
                            data_df[DATA_TO_PARSE_NO_INDEX[i][1]] = to_add1[0][2]

                all_tables.append(data_df)  # Добавляем обработанную таблицу в результат

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

    df = clean_and_convert_to_float(df,['4', '5', '8', '9'])

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
    folder_path = "upd_snab"
    folder_report_abcp = "report_abcp_snab"
    report_abcp_xls = glob(os.path.join(folder_report_abcp, "*.xls"))[0]
    folder_report_abcp_csv = xls_to_csv(report_abcp_xls)
    folder_spravochnik_tnved = "tnved"
    folder_spravochnik_tnved_xlsx = glob(os.path.join(folder_spravochnik_tnved, "*.xls"))[0]
    folder_spravochnik_tnved_csv = xlsx_to_csv(folder_spravochnik_tnved_xlsx)
    target_path_as_csv = "main_snab.csv"
    temp_file = "temp_data_file.csv"

    excel_files = glob(os.path.join(folder_path, "*.xls*"))

    if not os.path.exists(target_path_as_csv):
        # Создаем DataFrame с нужными колонками, включая новую колонку
        pd.DataFrame(columns=COLUMN_ORDER).to_csv(target_path_as_csv, index=False, encoding='utf-8-sig')

    for file in excel_files:
        tables = find_and_extract_tables(file)

        for i, table in enumerate(tables, 1):
            print(f"Обработка таблицы {i} из файла {file}")
            save_to_csv(table, temp_file)
            merge_csv_by_headers(temp_file, target_path_as_csv)

    # добавляем коды ТН ВЭД
    merge_csv_preserve_headers(target_path_as_csv, folder_spravochnik_tnved_csv, target_path_as_csv)

    # подставляем Россия в страну
    replace_missing_country(target_path_as_csv, "10а", "РОССИЯ")

    # добавляем данные из REPORT ABCP
    merge_csv_files(target_path_as_csv, folder_report_abcp_csv)

    csv_to_xlsx(target_path_as_csv)

    if os.path.exists(temp_file):
        os.remove(temp_file)
        os.remove(folder_report_abcp_csv)
        os.remove(folder_spravochnik_tnved_csv)
        os.remove(target_path_as_csv)

    print("Обработка всех файлов завершена!")
