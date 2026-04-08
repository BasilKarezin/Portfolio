# fssp_updater_final_simple.py
import pandas as pd
import pyodbc
import warnings
from datetime import datetime
import sys
import logging
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fssp_update.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore')


class FSSPUpdater:
    def __init__(self, server: str, database: str, username: str, password: str):
        self.server = server
        self.database = database
        self.username = username
        self.password = password

        self.driver = 'SQL Server Native Client 11.0'
        self.conn_str = (
            f"Driver={{{self.driver}}};"
            f"Server={self.server};"
            f"Database={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
        )

        self.conn = None
        self.cursor = None
        self.table_columns_info = {}

    def connect(self):
        """Подключение к базе данных"""
        try:
            logger.debug(f"Подключение: {self.server} -> {self.database}")

            self.conn = pyodbc.connect(self.conn_str, timeout=30)
            self.cursor = self.conn.cursor()

            self.cursor.execute("SELECT @@VERSION")
            version = self.cursor.fetchone()[0]
            logger.info(f"✓ Подключение успешно!")

            return True

        except pyodbc.Error as e:
            logger.error(f"Ошибка подключения ODBC: {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def get_table_structure(self):
        """Получение структуры таблицы fssp_reestr"""
        try:
            if not self.connect():
                return {}

            # Получаем информацию о колонках таблицы
            self.cursor.execute("""
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'fssp_reestr'
                ORDER BY ORDINAL_POSITION
            """)

            columns = self.cursor.fetchall()
            self.table_columns_info = {}

            for col in columns:
                col_name = col[0]
                self.table_columns_info[col_name] = {
                    'data_type': col[1],
                    'max_length': col[2],
                    'nullable': col[3],
                    'default': col[4]
                }

            logger.info(f"Структура таблицы fssp_reestr ({len(columns)} колонок):")
            for col_name, info in list(self.table_columns_info.items())[:15]:
                logger.info(f"  {col_name}: {info['data_type']} (nullable: {info['nullable']})")

            return self.table_columns_info

        except Exception as e:
            logger.error(f"Ошибка получения структуры таблицы: {e}")
            return {}

    def test_connection(self):
        """Тестирование подключения"""
        print("\n" + "=" * 60)
        print("ТЕСТ ПОДКЛЮЧЕНИЯ И СТРУКТУРЫ БАЗЫ")
        print("=" * 60)

        if not self.connect():
            print("✗ Подключение не удалось")
            return False

        try:
            # Проверяем версию
            self.cursor.execute("SELECT @@VERSION")
            version = self.cursor.fetchone()[0]
            print(f"✓ Версия SQL Server: {version[:100]}...")

            # Получаем структуру таблицы
            print("\nСтруктура таблицы fssp_reestr:")
            columns_info = self.get_table_structure()

            if columns_info:
                print(f"✓ Найдено {len(columns_info)} колонок")

                # Показываем ключевые колонки
                print("\nКлючевые колонки и их типы:")
                key_columns = [
                    'fssp_reestr_id', 'osp_code', 'region_code',
                    'code_of_the_territorial_agency', 'name_of_the_territorial_agency',
                    'postal_address', 'postal_address_valid', 'city',
                    'territory_of_service', 'region'
                ]

                for col in key_columns:
                    if col in columns_info:
                        info = columns_info[col]
                        print(f"  {col}: {info['data_type']}")
                    else:
                        print(f"  {col}: НЕ НАЙДЕНА")

            print("\n" + "=" * 60)
            print("✓ ТЕСТ УСПЕШНО ЗАВЕРШЕН!")
            print("=" * 60)
            return True

        except Exception as e:
            print(f"✗ Ошибка при тестировании: {e}")
            return False
        finally:
            self.close()

    def load_excel_data(self, filepath: str, sheet_name: str = 'temp'):
        """Загрузка данных из Excel"""
        try:
            logger.info(f"Загрузка Excel: {filepath}")

            if filepath.endswith('.xls'):
                engine = 'xlrd'
            else:
                engine = 'openpyxl'

            df_new = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str, engine=engine)
            logger.info(f"✓ Загружено {len(df_new)} строк из Excel")

            # Заменяем NaN на None
            df_new = df_new.where(pd.notnull(df_new), None)

            # Загружаем данные из базы
            if not self.connect():
                raise ConnectionError("Не удалось подключиться к базе")

            df_existing = pd.read_sql("SELECT * FROM fssp_reestr", self.conn)
            logger.info(f"✓ Загружено {len(df_existing)} строк из базы")

            return df_new, df_existing

        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            raise

    def prepare_new_data(self, df_new: pd.DataFrame):
        """Подготовка данных из Excel с учетом всех полей"""
        # Полный маппинг колонок из Excel в таблицу
        mapping = {
            'CONCATENATION': 'osp_code',
            'TERRITORY': 'region_code',
            'DIV_NAME': 'name_of_the_territorial_agency',
            'DIV_FULLNAME': 'code_of_the_territorial_agency',
            'DIV_ADR': 'postal_address',
            'DIV_HEAD_NAME': 'chiefs_full_name',
            'DIV_TEL': 'telephone_number',
            'DIV_FAX': 'fax',
            'DIV_TEL_PRIEM': 'phone_of_help_service',
            'INQUIRY_SERVICES_TEL': 'phone_of_help_service_2',
            'DIV_HOURS': 'working_hours_of_agency',
            'DIV_REGION': 'territory_of_service',
            'DIV_EMAIL': 'email',
            'DIV_REGION': 'region',
            'DIV_CITY': 'city'
        }

        result = pd.DataFrame()

        # Добавляем колонки из Excel
        for excel_col, db_col in mapping.items():
            if excel_col in df_new.columns:
                result[db_col] = df_new[excel_col]
                logger.debug(f"Добавлена колонка: {excel_col} -> {db_col}")

        # Очистка osp_code
        if 'osp_code' in result.columns:
            result['osp_code'] = result['osp_code'].astype(str).str.strip()

        # ВАЖНОЕ ИСПРАВЛЕНИЕ: postal_address_valid заполняем из postal_address
        if 'postal_address' in result.columns:
            result['postal_address_valid'] = result['postal_address']
            logger.info("✓ postal_address_valid заполнен из postal_address")

        # ВАЖНОЕ ИСПРАВЛЕНИЕ: territory_of_service заполняем из city (если еще не заполнено)
        if 'city' in result.columns and 'territory_of_service' in result.columns:
            # Если territory_of_service пустое, заполняем из city
            mask = (result['territory_of_service'].isna()) | (result['territory_of_service'] == '')
            result.loc[mask, 'territory_of_service'] = result.loc[mask, 'city']
            logger.info("✓ territory_of_service заполнен из city (где было пусто)")

        # Обработка числовых полей
        numeric_fields = ['region_code', 'code_of_the_territorial_agency']

        for field in numeric_fields:
            if field in result.columns:
                # Пробуем преобразовать в число, если возможно
                def safe_int(x):
                    if pd.isna(x):
                        return None
                    try:
                        # Убираем все нецифровые символы
                        cleaned = re.sub(r'[^\d]', '', str(x))
                        if cleaned:
                            return int(cleaned)
                        return None
                    except:
                        return None

                result[field] = result[field].apply(safe_int)
                logger.info(f"✓ {field} преобразован в числовой формат")

        logger.info(f"✓ Подготовлено {len(result)} строк, {len(result.columns)} колонок")

        # Показываем примеры данных
        if len(result) > 0:
            logger.info("Пример подготовленных данных:")
            for i in range(min(3, len(result))):
                row = result.iloc[i]
                logger.info(f"  Строка {i + 1}: OSP={row.get('osp_code', '')}, "
                            f"Код терр. органа={row.get('code_of_the_territorial_agency', '')}, "
                            f"Город={row.get('city', '')}, "
                            f"Территория={row.get('territory_of_service', '')}")

        return result

    def analyze_changes(self, excel_filepath: str, sheet_name: str = 'temp'):
        """Анализ изменений без записи в базу"""
        try:
            print("\n" + "=" * 60)
            print("АНАЛИЗ ИЗМЕНЕНИЙ (без записи в базу)")
            print("=" * 60)

            df_new, df_existing = self.load_excel_data(excel_filepath, sheet_name)
            df_prepared = self.prepare_new_data(df_new)

            # Анализ
            existing_codes = set(df_existing['osp_code'].astype(str).str.strip())
            new_codes = set(df_prepared['osp_code'].astype(str).str.strip())

            new_to_add = new_codes - existing_codes
            existing_to_update = existing_codes & new_codes
            unchanged = existing_codes - new_codes

            print("\n" + "=" * 60)
            print("РЕЗУЛЬТАТЫ АНАЛИЗА:")
            print("=" * 60)
            print(f"Всего в Excel: {len(new_codes)} записей")
            print(f"Всего в базе: {len(existing_codes)} записей")
            print(f"Новых для добавления: {len(new_to_add)}")
            print(f"Существующих для обновления: {len(existing_to_update)}")
            print(f"Останутся без изменений: {len(unchanged)}")

            # Показываем примеры
            if new_to_add:
                print(f"\nПримеры новых записей (первые 5):")
                for code in list(new_to_add)[:5]:
                    record = df_prepared[df_prepared['osp_code'] == code].iloc[0]
                    name = record.get('name_of_the_territorial_agency', 'Не указано')
                    city = record.get('city', 'Не указан')
                    print(f"  {code}: {name[:40]}... (город: {city})")

            if existing_to_update:
                print(f"\nПримеры записей для обновления (первые 3):")
                for code in list(existing_to_update)[:3]:
                    old_record = df_existing[df_existing['osp_code'] == code].iloc[0]
                    new_record = df_prepared[df_prepared['osp_code'] == code].iloc[0]

                    # Сравниваем несколько полей
                    fields_to_check = ['name_of_the_territorial_agency', 'city', 'code_of_the_territorial_agency']
                    changes = []

                    for field in fields_to_check:
                        if field in old_record and field in new_record:
                            old_val = str(old_record[field]) if pd.notna(old_record[field]) else ''
                            new_val = str(new_record[field]) if pd.notna(new_record[field]) else ''

                            if old_val != new_val:
                                changes.append(f"{field}: '{old_val[:20]}'->'{new_val[:20]}'")

                    if changes:
                        print(f"  {code}: {', '.join(changes)}")
                    else:
                        print(f"  {code}: данные идентичны")

            print("\n" + "=" * 60)
            print("АНАЛИЗ ЗАВЕРШЕН")
            print("=" * 60)

            return {
                'new_to_add': new_to_add,
                'existing_to_update': existing_to_update,
                'unchanged': unchanged,
                'df_prepared': df_prepared,
                'df_existing': df_existing
            }

        except Exception as e:
            print(f"✗ Ошибка анализа: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            self.close()

    def convert_value_for_db(self, column_name: str, value):
        """Конвертирует значение для вставки в БД с учетом типа колонки"""
        if pd.isna(value) or value is None:
            return None

        if column_name not in self.table_columns_info:
            return str(value) if value is not None else None

        data_type = self.table_columns_info[column_name]['data_type'].upper()

        try:
            # Обработка числовых типов
            if 'INT' in data_type or 'NUMERIC' in data_type or 'DECIMAL' in data_type:
                if value == '' or str(value).strip() == '':
                    return None
                try:
                    # Пробуем преобразовать в число
                    cleaned = re.sub(r'[^\d]', '', str(value))
                    if cleaned:
                        return int(cleaned)
                    return None
                except:
                    return None

            # Обработка дат
            elif 'DATE' in data_type or 'TIME' in data_type:
                if isinstance(value, datetime):
                    return value
                try:
                    return pd.to_datetime(value)
                except:
                    return None

            # Для строковых типов
            else:
                str_value = str(value).strip()
                # Проверяем длину строки
                max_len = self.table_columns_info[column_name].get('max_length')
                if max_len and len(str_value) > max_len:
                    str_value = str_value[:max_len]
                return str_value

        except Exception as e:
            logger.warning(f"Ошибка конвертации {column_name}: {value} -> {e}")
            return str(value) if value is not None else None

    def update_fssp_reestr(self, df_prepared: pd.DataFrame):
        """Обновление только таблицы fssp_reestr"""
        try:
            if not self.connect():
                raise ConnectionError("Нет подключения к базе")

            # Получаем структуру таблицы
            self.get_table_structure()
            if not self.table_columns_info:
                raise ValueError("Не удалось получить структуру таблицы")

            updated_count = 0
            inserted_count = 0
            errors = []

            logger.info("Начало обновления таблицы fssp_reestr...")
            print(f"\nОбновление fssp_reestr...")

            for idx, row in df_prepared.iterrows():
                try:
                    osp_code = str(row['osp_code']).strip() if pd.notna(row['osp_code']) else None

                    if not osp_code:
                        continue

                    # Проверяем, существует ли запись
                    self.cursor.execute("SELECT fssp_reestr_id FROM fssp_reestr WHERE osp_code = ?", osp_code)
                    existing = self.cursor.fetchone()

                    if existing:
                        # ОБНОВЛЯЕМ существующую запись
                        update_fields = []
                        update_values = []

                        # Все поля для обновления
                        field_mapping = {
                            'region_code': row.get('region_code'),
                            'code_of_the_territorial_agency': row.get('code_of_the_territorial_agency'),
                            'name_of_the_territorial_agency': row.get('name_of_the_territorial_agency'),
                            'postal_address': row.get('postal_address'),
                            'chiefs_full_name': row.get('chiefs_full_name'),
                            'telephone_number': row.get('telephone_number'),
                            'fax': row.get('fax'),
                            'phone_of_help_service': row.get('phone_of_help_service'),
                            'phone_of_help_service_2': row.get('phone_of_help_service_2'),
                            'working_hours_of_agency': row.get('working_hours_of_agency'),
                            'territory_of_service': row.get('territory_of_service'),
                            'postal_address_valid': row.get('postal_address_valid'),
                            'email': row.get('email'),
                            'region': row.get('region'),
                            'city': row.get('city')
                        }

                        # Дополнительно: если postal_address_valid не заполнен, заполняем из postal_address
                        if pd.isna(field_mapping['postal_address_valid']) and pd.notna(field_mapping['postal_address']):
                            field_mapping['postal_address_valid'] = field_mapping['postal_address']

                        # Дополнительно: если territory_of_service не заполнен, заполняем из city
                        if pd.isna(field_mapping['territory_of_service']) and pd.notna(field_mapping['city']):
                            field_mapping['territory_of_service'] = field_mapping['city']

                        # Добавляем updated_date если есть в таблице
                        if 'updated_date' in self.table_columns_info:
                            field_mapping['updated_date'] = datetime.now()

                        # Добавляем access_code если есть в таблице
                        if 'access_code' in self.table_columns_info:
                            field_mapping['access_code'] = osp_code

                        # Формируем поля для обновления
                        for field, value in field_mapping.items():
                            if field in self.table_columns_info:
                                # Конвертируем значение для БД
                                converted_value = self.convert_value_for_db(field, value)
                                if converted_value is not None or self.table_columns_info[field]['nullable'] == 'YES':
                                    update_fields.append(f"{field} = ?")
                                    update_values.append(converted_value)

                        # Добавляем osp_code для WHERE
                        update_values.append(osp_code)

                        if update_fields:
                            update_sql = f"UPDATE fssp_reestr SET {', '.join(update_fields)} WHERE osp_code = ?"
                            self.cursor.execute(update_sql, *update_values)
                            updated_count += 1

                    else:
                        # ДОБАВЛЯЕМ новую запись
                        # Получаем новый ID
                        self.cursor.execute("SELECT ISNULL(MAX(fssp_reestr_id), 0) + 1 FROM fssp_reestr")
                        new_id = self.cursor.fetchone()[0]

                        # Формируем поля для вставки
                        insert_fields = ['fssp_reestr_id', 'osp_code']
                        insert_values = [new_id, osp_code]

                        # Все поля для вставки
                        field_mapping = {
                            'region_code': row.get('region_code'),
                            'code_of_the_territorial_agency': row.get('code_of_the_territorial_agency'),
                            'name_of_the_territorial_agency': row.get('name_of_the_territorial_agency'),
                            'postal_address': row.get('postal_address'),
                            'chiefs_full_name': row.get('chiefs_full_name'),
                            'telephone_number': row.get('telephone_number'),
                            'fax': row.get('fax'),
                            'phone_of_help_service': row.get('phone_of_help_service'),
                            'phone_of_help_service_2': row.get('phone_of_help_service_2'),
                            'working_hours_of_agency': row.get('working_hours_of_agency'),
                            'territory_of_service': row.get('territory_of_service'),
                            'postal_address_valid': row.get('postal_address_valid'),
                            'email': row.get('email'),
                            'region': row.get('region'),
                            'city': row.get('city')
                        }

                        # Дополнительная логика заполнения
                        if pd.isna(field_mapping['postal_address_valid']) and pd.notna(field_mapping['postal_address']):
                            field_mapping['postal_address_valid'] = field_mapping['postal_address']

                        if pd.isna(field_mapping['territory_of_service']) and pd.notna(field_mapping['city']):
                            field_mapping['territory_of_service'] = field_mapping['city']

                        # Добавляем updated_date если есть в таблице
                        if 'updated_date' in self.table_columns_info:
                            field_mapping['updated_date'] = datetime.now()

                        # Добавляем access_code если есть в таблице
                        if 'access_code' in self.table_columns_info:
                            field_mapping['access_code'] = osp_code

                        # Формируем поля для вставки
                        for field, value in field_mapping.items():
                            if field in self.table_columns_info:
                                # Конвертируем значение для БД
                                converted_value = self.convert_value_for_db(field, value)
                                if converted_value is not None or self.table_columns_info[field]['nullable'] == 'YES':
                                    insert_fields.append(field)
                                    insert_values.append(converted_value)

                        # Формируем SQL
                        placeholders = ', '.join(['?'] * len(insert_fields))
                        insert_sql = f"INSERT INTO fssp_reestr ({', '.join(insert_fields)}) VALUES ({placeholders})"

                        self.cursor.execute(insert_sql, *insert_values)
                        inserted_count += 1

                except Exception as e:
                    error_msg = f"Ошибка в строке {idx} (osp_code: {osp_code}): {e}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    continue

            self.conn.commit()

            print(f"\n✓ Обновление fssp_reestr завершено:")
            print(f"  Обновлено записей: {updated_count}")
            print(f"  Добавлено новых: {inserted_count}")

            if errors:
                print(f"\n⚠ Было {len(errors)} ошибок (детали в логе)")
                for error in errors[:5]:
                    print(f"  - {error}")

            logger.info(f"Обновлено: {updated_count}, Добавлено: {inserted_count}, Ошибок: {len(errors)}")

            return updated_count, inserted_count

        except Exception as e:
            self.conn.rollback()
            logger.error(f"✗ Ошибка при обновлении: {e}")
            raise
        finally:
            self.close()

    def check_integrity(self):
        """Проверка целостности данных только для fssp_reestr"""
        try:
            if not self.connect():
                return

            print("\n" + "=" * 60)
            print("ПРОВЕРКА ЦЕЛОСТНОСТИ ДАННЫХ (fssp_reestr)")
            print("=" * 60)

            # Проверка дубликатов
            print("\n1. Проверка дубликатов osp_code:")
            self.cursor.execute("""
                SELECT osp_code, COUNT(*) as cnt 
                FROM fssp_reestr 
                GROUP BY osp_code 
                HAVING COUNT(*) > 1
            """)

            duplicates = self.cursor.fetchall()
            if not duplicates:
                print("  ✓ Дубликатов osp_code нет")
            else:
                print(f"  ⚠ Найдены дубликаты osp_code: {len(duplicates)}")
                for dup in duplicates[:3]:
                    print(f"    Код {dup[0]}: {dup[1]} записей")

            # Проверка заполненности полей
            print("\n2. Проверка заполненности ключевых полей:")
            fields_to_check = [
                ('osp_code', 'Код ОСП'),
                ('name_of_the_territorial_agency', 'Название органа'),
                ('city', 'Город'),
                ('postal_address', 'Почтовый адрес')
            ]

            for field, description in fields_to_check:
                self.cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM fssp_reestr 
                    WHERE {field} IS NULL OR {field} = ''
                """)
                empty_count = self.cursor.fetchone()[0]

                self.cursor.execute(f"SELECT COUNT(*) FROM fssp_reestr")
                total_count = self.cursor.fetchone()[0]

                if empty_count == 0:
                    print(f"  ✓ {description}: 100% заполнено")
                else:
                    percent = 100 - (empty_count / total_count * 100)
                    print(f"  ⚠ {description}: заполнено на {percent:.1f}% ({empty_count} пустых)")

            # Статистика
            print("\n3. Статистика таблицы fssp_reestr:")
            self.cursor.execute("SELECT COUNT(*) FROM fssp_reestr")
            total = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT COUNT(DISTINCT osp_code) FROM fssp_reestr")
            unique = self.cursor.fetchone()[0]

            self.cursor.execute("SELECT MIN(osp_code), MAX(osp_code) FROM fssp_reestr")
            min_code, max_code = self.cursor.fetchone()

            print(f"  Всего записей: {total}")
            print(f"  Уникальных osp_code: {unique}")
            if min_code and max_code:
                print(f"  Диапазон кодов: от {min_code} до {max_code}")

            print("\n" + "=" * 60)
            print("ПРОВЕРКА ЗАВЕРШЕНА")
            print("=" * 60)

        except Exception as e:
            print(f"✗ Ошибка при проверке целостности: {e}")
        finally:
            self.close()

    def run_full_update(self, excel_filepath: str, sheet_name: str = 'temp'):
        """Полное обновление таблицы fssp_reestr"""
        print("\n" + "=" * 60)
        print("ПОЛНОЕ ОБНОВЛЕНИЕ ТАБЛИЦЫ FSSP_REESTR")
        print("=" * 60)

        try:
            # 1. Загрузка данных
            print("\n1. Загрузка данных...")
            df_new, df_existing = self.load_excel_data(excel_filepath, sheet_name)
            df_prepared = self.prepare_new_data(df_new)

            # 2. Обновление основной таблицы
            print("\n2. Обновление таблицы fssp_reestr...")
            updated, inserted = self.update_fssp_reestr(df_prepared)

            print(f"\n✓ Таблица fssp_reestr обновлена:")
            print(f"  Обновлено: {updated} записей")
            print(f"  Добавлено: {inserted} записей")

            # 3. Проверка целостности
            print("\n3. Проверка целостности...")
            self.check_integrity()

            print("\n" + "=" * 60)
            print("✓ ОБНОВЛЕНИЕ ЗАВЕРШЕНО УСПЕШНО!")
            print("=" * 60)
            print("\nЛог сохранен в файле: fssp_update.log")

        except Exception as e:
            print(f"\n✗ Ошибка при обновлении: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Главная функция"""
    CONFIG = {
        'server': '-',
        'database': '-',
        'username': '-',
        'password': '-'
    }

    EXCEL_FILEPATH = r'D:\Users\Desktop\моя\справочник ВКСП.xls'
    SHEET_NAME = 'temp'

    print("\n" + "=" * 60)
    print("ОБНОВЛЕНИЕ СПРАВОЧНИКА FSSP_REESTR")
    print("=" * 60)
    print(f"Сервер: {CONFIG['server']}")
    print(f"База: {CONFIG['database']}")
    print(f"Файл: {EXCEL_FILEPATH}")
    print("=" * 60)

    updater = FSSPUpdater(**CONFIG)

    while True:
        print("\n" + "=" * 40)
        print("МЕНЮ")
        print("=" * 40)
        print("1. Тестирование подключения и структуры")
        print("2. Анализ изменений (без записи в базу)")
        print("3. Полное обновление")
        print("4. Проверка целостности данных")
        print("5. Выход")
        print("=" * 40)

        choice = input("\nВыберите действие (1-5): ").strip()

        if choice == '1':
            updater.test_connection()

        elif choice == '2':
            print("\n" + "=" * 60)
            print("РЕЖИМ АНАЛИЗА")
            print("Только показывает, что изменится, без записи в базу!")
            print("=" * 60)

            result = updater.analyze_changes(EXCEL_FILEPATH, SHEET_NAME)
            if result:
                print("\n✓ Анализ завершен. Смотрите fssp_update.log")

        elif choice == '3':
            print("\n" + "=" * 60)
            print("РЕАЛЬНОЕ ОБНОВЛЕНИЕ БАЗЫ ДАННЫХ!")
            print("=" * 60)

            updater.run_full_update(EXCEL_FILEPATH, SHEET_NAME)

        elif choice == '4':
            updater.check_integrity()

        elif choice == '5':
            print("Выход...")
            break

        else:
            print("Неверный выбор. Попробуйте снова.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nПрограмма прервана пользователем.")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")

    input("\nНажмите Enter для выхода...")

# ------------------------------------------------------------
# ИСПРАВЛЕНИЯ В ЭТОЙ ВЕРСИИ:
# 1. Скрипт обновляет ТОЛЬКО таблицу fssp_reestr
# 2. Удалены все методы для работы с другими таблицами
# 3. Удалены все подтверждения
# 4. Сохранена полная функциональность для fssp_reestr:
#    - Загрузка данных из Excel
#    - Подготовка данных
#    - Анализ изменений
#    - Обновление таблицы
#    - Проверка целостности
# ------------------------------------------------------------
