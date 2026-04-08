import pyodbc
import requests
import time
import urllib.parse
import json
from datetime import datetime, timedelta
import logging
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fedresurs_work.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class Config:
    # ОПТИМИЗИРОВАННЫЕ настройки для 8 запросов/секунда
    REQUEST_INTERVAL = 0.125  # 8 запросов/сек = 0.125 сек между запросами
    DB_CONN_STR = (
        "Driver={SQL Server Native Client 11.0};"
        "Server=-;"
        "Database=-;"
        "UID=-;"
        "PWD=-;"
    )
    BATCH_SIZE = 10000
    LIMIT = 15
    REQUESTS_BEFORE_PAUSE = 500
    PAUSE_SECONDS = 2
    BATCH_PAUSE_SECONDS = 3


def format_date(date_string):
    """Форматирование даты"""
    if not date_string:
        return None
    try:
        if isinstance(date_string, str):
            if 'T' in date_string:
                date_obj = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
                return date_obj.strftime('%Y-%m-%d')
            else:
                date_obj = datetime.strptime(date_string.split('T')[0], '%Y-%m-%d')
                return date_obj.strftime('%Y-%m-%d')
        return None
    except (ValueError, AttributeError):
        return None


class FedresursAPI:
    """API клиент для fedresurs"""

    def __init__(self):
        self.session = requests.Session()
        self._setup_headers()
        self.last_request = 0
        self.request_count = 0

    def _setup_headers(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://bankrot.fedresurs.ru',
            'Accept-Language': 'ru-RU',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'application/json, text/plain, */*',
        }
        self.session.headers.clear()
        self.session.headers.update(headers)

    def _wait(self):
        """Ожидание между запросами"""
        current_time = time.time()
        elapsed = current_time - self.last_request

        if elapsed < Config.REQUEST_INTERVAL:
            time.sleep(Config.REQUEST_INTERVAL - elapsed)

        self.last_request = time.time()
        self.request_count += 1

        if self.request_count % Config.REQUESTS_BEFORE_PAUSE == 0:
            logger.info(f"Пауза {Config.PAUSE_SECONDS} сек после {self.request_count} запросов")
            time.sleep(Config.PAUSE_SECONDS)

    def get_person_details(self, guid):
        """Получение детальной информации о человеке по GUID"""
        try:
            self._wait()

            url = f'https://bankrot.fedresurs.ru/backend/prsnbankrupts/{guid}'

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            return response.json()

        except Exception as e:
            logger.debug(f"Ошибка получения деталей для GUID {guid}: {e}")
            return None

    def search_fio(self, fio):
        """Поиск по ФИО с получением максимального количества данных"""
        try:
            self._wait()

            encoded_fio = urllib.parse.quote(fio)
            url = f'https://bankrot.fedresurs.ru/backend/prsnbankrupts?searchString={encoded_fio}&isActiveLegalCase=null&limit={Config.LIMIT}&offset=0'

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            case_info = response.json()
            results = []

            if 'pageData' in case_info:
                cases = case_info['pageData']
                for case in cases:
                    guid = case.get('guid')

                    # Получаем детальную информацию
                    details = self.get_person_details(guid) if guid else {}

                    last_legal_case = case.get('lastLegalCase', {})
                    status_info = last_legal_case.get('status', {})
                    extrajudicial_info = case.get('extrajudicialBankruptcy', {})

                    status_description = status_info.get('description') if isinstance(status_info, dict) else None
                    status_date = format_date(status_info.get('date')) if isinstance(status_info, dict) else None

                    # Получаем информацию из детального ответа
                    birth_date = None
                    if details and 'birthDate' in details:
                        birth_date = format_date(details.get('birthDate'))

                    # Извлекаем все возможные данные
                    case_data = {
                        # Основные данные из поиска
                        'snils': case.get('snils'),
                        'category': case.get('category'),
                        'region': case.get('region'),
                        'address': case.get('address'),
                        'guid': guid,
                        'fio': case.get('fio'),
                        'inn': case.get('inn'),
                        'ФИО': fio,

                        # Данные о последнем деле
                        'case_number': last_legal_case.get('number'),
                        'lastLegalCase_arbitrManagerFio': last_legal_case.get('arbitrManagerFio'),
                        'lastLegalCase_status': status_description,
                        'lastLegalCase_status_date': status_date,

                        # Внесудебное банкротство
                        'Vnesudebnoe': str(extrajudicial_info) if extrajudicial_info else None,

                        # ДЕТАЛЬНЫЕ ДАННЫЕ из отдельного запроса
                        'birthdate': birth_date,
                        'birthplace': details.get('birthPlace') if details else None,

                        # Паспортные данные
                        'passport_series': details.get('passportSeries') if details else None,
                        'passport_number': details.get('passportNumber') if details else None,
                        'passport_date': format_date(details.get('passportDate')) if details else None,
                        'passport_department': details.get('passportDepartment') if details else None,

                        # Контакты
                        'email': details.get('email') if details else None,
                        'phone': details.get('phone') if details else None,

                        # СНИЛС и ИНН (дублируем для надежности)
                        'snils_detailed': details.get('snils') if details else None,
                        'inn_detailed': details.get('inn') if details else None,

                        # Полная история банкротств
                        'bankruptcies': details.get('bankruptcies') if details else None,
                        'bankruptcies_count': len(details.get('bankruptcies', [])) if details and details.get('bankruptcies') else 0,

                        # Сообщения
                        'messages_count': details.get('totalMessages', 0) if details else 0,

                        # Флаги
                        'has_bankruptcies': details.get('hasBankruptcies', False) if details else False,
                        'has_messages': details.get('hasMessages', False) if details else False,
                        'has_extrajudicial': details.get('hasExtrajudicialBankruptcy', False) if details else False,

                        # Дата регистрации
                        'registration_date': format_date(details.get('registrationDate')) if details else None,
                    }
                    results.append(case_data)

            if (self.request_count % 100 == 0) or (len(results) > 0):
                logger.info(f"Запрос {self.request_count}: {fio[:30]}... найдено {len(results)} случаев")

            return results

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP ошибка для {fio[:30]}: {e.response.status_code}")

            if e.response.status_code == 451:
                logger.warning("451 ошибка, ждем 10 секунд...")
                time.sleep(10)
                return None
            elif e.response.status_code == 429:
                logger.warning("429 Rate Limit, ждем 30 секунд...")
                time.sleep(30)
                return None
            else:
                logger.error(f"Тело ответа: {e.response.text[:200]}")
                time.sleep(2)
                return None

        except Exception as e:
            logger.error(f"Ошибка для {fio[:30]}: {str(e)[:100]}")
            time.sleep(2)
            return None


def get_db_connection():
    """Подключение к БД"""
    try:
        conn = pyodbc.connect(Config.DB_CONN_STR)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        return None


def get_total_fio_count():
    """Получение общего количества ФИО для обработки"""
    with get_db_connection() as connection:
        query = """
        SELECT COUNT(DISTINCT CONCAT(UPPER(p.last_name), ' ', UPPER(p.first_name), ' ', UPPER(p.patronymic))) AS total
        FROM persons p 
        inner join cont_pers_dtl cp on p.person_id = cp.person_id
        inner join contracts c on cp.contract_id = c.contract_id
        WHERE p.birth_date IS NOT NULL and p.fedresurs_guid is null 
        and c.ostatok_dolga > 0
        AND (p.last_name IS NOT NULL AND p.first_name IS NOT NULL)
        """
        cursor = connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        return row[0] if row else 0


def fetch_fio(offset=0, batch_size=None):
    """Получение батча ФИО"""
    if batch_size is None:
        batch_size = Config.BATCH_SIZE

    with get_db_connection() as connection:
        query = f"""
        SELECT DISTINCT CONCAT(UPPER(p.last_name), ' ', UPPER(p.first_name), ' ', UPPER(p.patronymic)) AS FIO
        FROM persons p 
        inner join cont_pers_dtl cp on p.person_id = cp.person_id
        inner join contracts c on cp.contract_id = c.contract_id
        WHERE p.birth_date IS NOT NULL and p.fedresurs_guid is null 
        and c.ostatok_dolga > 0
        AND (p.last_name IS NOT NULL AND p.first_name IS NOT NULL)
        ORDER BY FIO
        OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY
        """
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        return [fio[0] for fio in rows]


def update_raw_table_structure():
    """Обновление структуры таблицы raw_search_results для новых полей"""
    with get_db_connection() as connection:
        cursor = connection.cursor()

        # Список новых полей для добавления
        new_columns = [
            ('birthdate', 'DATE'),
            ('birthplace', 'NVARCHAR(500)'),
            ('passport_series', 'NVARCHAR(50)'),
            ('passport_number', 'NVARCHAR(50)'),
            ('passport_date', 'DATE'),
            ('passport_department', 'NVARCHAR(500)'),
            ('email', 'NVARCHAR(255)'),
            ('phone', 'NVARCHAR(100)'),
            ('snils_detailed', 'NVARCHAR(50)'),
            ('inn_detailed', 'NVARCHAR(50)'),
            ('bankruptcies_count', 'INT DEFAULT 0'),
            ('messages_count', 'INT DEFAULT 0'),
            ('has_bankruptcies', 'BIT DEFAULT 0'),
            ('has_messages', 'BIT DEFAULT 0'),
            ('has_extrajudicial', 'BIT DEFAULT 0'),
            ('registration_date', 'DATE'),
        ]

        for col_name, col_type in new_columns:
            try:
                cursor.execute(f"""
                    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = 'raw_search_results' AND COLUMN_NAME = '{col_name}')
                    BEGIN
                        ALTER TABLE raw_search_results ADD {col_name} {col_type}
                    END
                """)
            except Exception as e:
                logger.warning(f"Ошибка при добавлении колонки {col_name}: {e}")

        connection.commit()
        logger.info("Структура таблицы raw_search_results обновлена")


def initialize_raw_table():
    """ИНИЦИАЛИЗАЦИЯ ТАБЛИЦЫ raw_search_results с поддержкой GUID"""
    with get_db_connection() as connection:
        cursor = connection.cursor()

        # Создаем таблицу если её нет
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'raw_search_results')
        BEGIN
            CREATE TABLE raw_search_results (
                id INT IDENTITY PRIMARY KEY,
                fio_original NVARCHAR(255) NOT NULL,
                case_data NVARCHAR(MAX) NOT NULL,
                search_date DATETIME DEFAULT GETDATE(),
                processed INT DEFAULT 0,
                error_message NVARCHAR(500) NULL,
                person_id INT NULL,
                fio_hash NVARCHAR(100) NULL,
                guid NVARCHAR(100) NULL,
                inn NVARCHAR(50) NULL,
                snils NVARCHAR(50) NULL
            )

            CREATE INDEX idx_raw_search_processed ON raw_search_results (processed)
            CREATE INDEX idx_raw_search_date ON raw_search_results (search_date)
            CREATE INDEX idx_raw_search_fio ON raw_search_results (fio_original)
            CREATE INDEX idx_raw_search_guid ON raw_search_results (guid)
            CREATE INDEX idx_raw_search_inn ON raw_search_results (inn)
            CREATE INDEX idx_raw_search_snils ON raw_search_results (snils)
        END
        ELSE
        BEGIN
            -- Добавляем новые колонки если их нет
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'raw_search_results' AND COLUMN_NAME = 'guid')
            BEGIN
                ALTER TABLE raw_search_results ADD guid NVARCHAR(100) NULL
                CREATE INDEX idx_raw_search_guid ON raw_search_results (guid)
            END

            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'raw_search_results' AND COLUMN_NAME = 'inn')
            BEGIN
                ALTER TABLE raw_search_results ADD inn NVARCHAR(50) NULL
                CREATE INDEX idx_raw_search_inn ON raw_search_results (inn)
            END

            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'raw_search_results' AND COLUMN_NAME = 'snils')
            BEGIN
                ALTER TABLE raw_search_results ADD snils NVARCHAR(50) NULL
                CREATE INDEX idx_raw_search_snils ON raw_search_results (snils)
            END
        END
        """)
        connection.commit()

        # Добавляем новые поля
        update_raw_table_structure()

        logger.info("Таблица raw_search_results инициализирована (с расширенными полями)")


def save_results_to_raw(fio, results):
    """СОХРАНЕНИЕ РЕЗУЛЬТАТОВ В raw_search_results - с обновлением существующих GUID"""
    if not results:
        return 0

    with get_db_connection() as connection:
        cursor = connection.cursor()
        saved_count = 0
        skipped_count = 0
        updated_count = 0
        inserted_count = 0

        for result in results:
            guid = result.get('guid')
            if not guid:
                skipped_count += 1
                continue

            # Проверяем, есть ли уже такой GUID
            cursor.execute(
                "SELECT id, case_data FROM raw_search_results WHERE guid = ?",
                guid
            )
            existing = cursor.fetchone()

            # Преобразуем результат в JSON строку
            result_json = json.dumps(result, ensure_ascii=False)

            try:
                if existing:
                    # Сравниваем статусы, чтобы понять, изменилось ли что-то
                    old_data = json.loads(existing[1]) if existing[1] else {}
                    old_status = old_data.get('lastLegalCase_status')
                    old_status_date = old_data.get('lastLegalCase_status_date')
                    old_vnesudebny_status = old_data.get('vnesudebny_status')

                    new_status = result.get('lastLegalCase_status')
                    new_status_date = result.get('lastLegalCase_status_date')
                    new_vnesudebny_status = result.get('vnesudebny_status')

                    # Проверяем, изменился ли статус
                    status_changed = (
                            (old_status != new_status) or
                            (old_status_date != new_status_date) or
                            (old_vnesudebny_status != new_vnesudebny_status)
                    )

                    if status_changed:
                        logger.info(f"ИЗМЕНЕНИЕ СТАТУСА для GUID {guid}:")
                        logger.info(f"  Было: статус={old_status}, дата={old_status_date}")
                        logger.info(f"  Стало: статус={new_status}, дата={new_status_date}")

                        # Обновляем существующую запись с расширенными полями
                        cursor.execute("""
                        UPDATE raw_search_results 
                        SET case_data = ?,
                            search_date = GETDATE(),
                            processed = 0,
                            fio_original = ?,
                            inn = ?,
                            snils = ?,
                            birthdate = ?,
                            birthplace = ?,
                            passport_series = ?,
                            passport_number = ?,
                            passport_date = ?,
                            passport_department = ?,
                            email = ?,
                            phone = ?,
                            snils_detailed = ?,
                            inn_detailed = ?,
                            bankruptcies_count = ?,
                            messages_count = ?,
                            has_bankruptcies = ?,
                            has_messages = ?,
                            has_extrajudicial = ?,
                            registration_date = ?
                        WHERE guid = ?
                        """,
                                       result_json,
                                       fio,
                                       result.get('inn', ''),
                                       result.get('snils', ''),
                                       result.get('birthdate'),
                                       result.get('birthplace'),
                                       result.get('passport_series'),
                                       result.get('passport_number'),
                                       result.get('passport_date'),
                                       result.get('passport_department'),
                                       result.get('email'),
                                       result.get('phone'),
                                       result.get('snils_detailed'),
                                       result.get('inn_detailed'),
                                       result.get('bankruptcies_count', 0),
                                       result.get('messages_count', 0),
                                       result.get('has_bankruptcies', False),
                                       result.get('has_messages', False),
                                       result.get('has_extrajudicial', False),
                                       result.get('registration_date'),
                                       guid
                                       )
                        updated_count += 1
                    else:
                        logger.debug(f"Нет изменений для GUID {guid}, пропускаем обновление")
                        # Можно все равно обновить дату поиска, но не менять данные
                        cursor.execute("""
                        UPDATE raw_search_results 
                        SET search_date = GETDATE()
                        WHERE guid = ?
                        """, guid)
                else:
                    # Вставляем новую запись
                    cursor.execute("""
                    INSERT INTO raw_search_results (
                        fio_original, 
                        case_data, 
                        processed,
                        guid,
                        inn,
                        snils,
                        birthdate,
                        birthplace,
                        passport_series,
                        passport_number,
                        passport_date,
                        passport_department,
                        email,
                        phone,
                        snils_detailed,
                        inn_detailed,
                        bankruptcies_count,
                        messages_count,
                        has_bankruptcies,
                        has_messages,
                        has_extrajudicial,
                        registration_date
                    )
                    VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                                   fio,
                                   result_json,
                                   guid,
                                   result.get('inn', ''),
                                   result.get('snils', ''),
                                   result.get('birthdate'),
                                   result.get('birthplace'),
                                   result.get('passport_series'),
                                   result.get('passport_number'),
                                   result.get('passport_date'),
                                   result.get('passport_department'),
                                   result.get('email'),
                                   result.get('phone'),
                                   result.get('snils_detailed'),
                                   result.get('inn_detailed'),
                                   result.get('bankruptcies_count', 0),
                                   result.get('messages_count', 0),
                                   result.get('has_bankruptcies', False),
                                   result.get('has_messages', False),
                                   result.get('has_extrajudicial', False),
                                   result.get('registration_date')
                                   )
                    inserted_count += 1

                saved_count += 1

            except Exception as e:
                logger.error(f"Ошибка при сохранении записи с GUID {guid}: {e}")
                connection.rollback()
                continue

        try:
            connection.commit()
            if inserted_count > 0 or updated_count > 0:
                logger.info(f"Коммит успешен: новых={inserted_count}, обновлено={updated_count}")
        except Exception as e:
            logger.error(f"Ошибка при коммите: {e}")
            return 0

        if saved_count > 0:
            logger.info(
                f"Обработано {saved_count} записей для {fio[:30]}: новых={inserted_count}, обновлено={updated_count}, пропущено без GUID={skipped_count}")

        return saved_count


def process_batch(search_fios, api_client, existing_guids):
    """Обработка батча ФИО - теперь всегда сохраняем/обновляем"""
    total_processed = 0
    total_new_cases = 0
    total_updates = 0
    batch_start = time.time()
    total_results_found = 0

    # Не копируем existing_guids, потому что мы теперь всегда сохраняем

    for i, fio in enumerate(search_fios):
        results = api_client.search_fio(fio)

        if results is not None and results:
            total_results_found += len(results)

            # Сохраняем ВСЕ результаты (функция сама разберется - обновить или вставить)
            saved = save_results_to_raw(fio, results)

            if saved > 0:
                total_processed += 1
                # Не можем точно сказать, сколько новых, сколько обновленных,
                # так как это внутри save_results_to_raw

        # Логирование каждые 100 запросов
        if (i + 1) % 100 == 0:
            elapsed = time.time() - batch_start
            if elapsed > 0:
                speed = (i + 1) / (elapsed / 60)
                logger.info(
                    f"Прогресс: обработано {i + 1} ФИО, "
                    f"найдено результатов: {total_results_found}, "
                    f"скорость: {speed:.0f} ФИО/мин"
                )

    return total_processed, total_results_found


def get_all_existing_guids():
    """Получение всех существующих GUID из raw_search_results"""
    with get_db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT guid FROM raw_search_results WHERE guid IS NOT NULL AND guid != ''")
        rows = cursor.fetchall()
        guid_set = set(row[0] for row in rows)
        logger.info(f"Загружено {len(guid_set)} существующих GUID")
        return guid_set



def check_table_structure():
    """Проверка структуры таблицы"""
    with get_db_connection() as connection:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'raw_search_results'
            ORDER BY ORDINAL_POSITION
        """)
        columns = cursor.fetchall()
        logger.info("Структура таблицы raw_search_results:")
        for col in columns:
            logger.info(f"  {col[0]} - {col[1]}")
        return columns


def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("FEDRESURS ПОИСК ДАННЫХ - РАСШИРЕННЫЙ СБОР ДАННЫХ")
    logger.info(f"Скорость: 8 запросов/сек ({Config.REQUEST_INTERVAL} сек интервал)")
    logger.info(f"Размер батча: {Config.BATCH_SIZE}")
    logger.info("=" * 60)

    max_fios = None
    if len(sys.argv) > 1:
        try:
            max_fios = int(sys.argv[1])
            logger.info(f"Ограничение: {max_fios} ФИО")
        except:
            logger.warning(f"Неверный аргумент: {sys.argv[1]}")

    # 1. Инициализация таблицы raw_search_results
    logger.info("Инициализация таблицы raw_search_results...")
    initialize_raw_table()

    # После инициализации таблицы
    check_table_structure()

    # 2. Тестирование API
    logger.info("Тестирование API...")
    api_client = FedresursAPI()
    test_fios = ["ИВАНОВ ИВАН ИВАНОВИЧ", "ПЕТРОВ ПЕТР ПЕТРОВИЧ"]

    for fio in test_fios:
        results = api_client.search_fio(fio)
        if results is None:
            logger.error("API тестирование провалилось")
            return
        logger.info(f"Тест {fio}: OK, найдено {len(results)} случаев")

    # 3. Получение статистики
    logger.info("Получение статистики...")
    total_count = get_total_fio_count()
    logger.info(f"Всего ФИО для обработки: {total_count:,}")

    # Получаем все существующие GUID
    existing_guids = get_all_existing_guids()
    logger.info(f"Существующих GUID в базе: {len(existing_guids):,}")

    if total_count == 0:
        logger.info("Нет данных для обработки")
        return

    # 4. Основной цикл обработки
    offset = 0
    total_processed = 0
    total_new_cases = 0
    start_time = datetime.now()
    batch_number = 0

    try:
        while True:
            if max_fios and total_processed >= max_fios:
                logger.info(f"Достигнут лимит в {max_fios:,} ФИО")
                break

            batch_number += 1
            logger.info(f"\n{'=' * 50}")
            logger.info(f"Батч #{batch_number}: offset={offset:,}, batch_size={Config.BATCH_SIZE}")
            logger.info(f"{'=' * 50}")

            search_fios = fetch_fio(offset, Config.BATCH_SIZE)

            if not search_fios:
                logger.info("Нет данных для обработки")
                break

            logger.info(f"Получено {len(search_fios):,} ФИО")

            processed, new_cases = process_batch(search_fios, api_client, existing_guids)

            total_processed += processed
            total_new_cases += new_cases
            offset += Config.BATCH_SIZE

            elapsed_total = (datetime.now() - start_time).total_seconds()
            if elapsed_total > 0 and total_processed > 0:
                speed_total = total_processed / (elapsed_total / 3600)
                hours_elapsed = elapsed_total / 3600

                logger.info(f"\n{'=' * 50}")
                logger.info(f"ИТОГИ БАТЧА #{batch_number}:")
                logger.info(f"  Обработано ФИО (с новыми данными): {processed:,}")
                logger.info(f"  Найдено новых случаев: {new_cases:,}")
                logger.info(f"  ВСЕГО обработано ФИО: {total_processed:,}")
                logger.info(f"  ВСЕГО новых случаев: {total_new_cases:,}")
                logger.info(f"  Скорость: {speed_total:.0f} ФИО/час")
                logger.info(f"  Время работы: {hours_elapsed:.1f} часов")

                remaining = total_count - (offset // Config.BATCH_SIZE * Config.BATCH_SIZE)
                if speed_total > 0 and remaining > 0:
                    eta_hours = remaining / speed_total
                    eta_days = eta_hours / 24
                    logger.info(f"  Осталось ФИО для проверки: {remaining:,}")
                    logger.info(f"  ETA: {eta_hours:.1f} часов ({eta_days:.1f} дней)")

            if processed > 0:
                logger.info(f"\nПауза {Config.BATCH_PAUSE_SECONDS} сек между батчами...")
                time.sleep(Config.BATCH_PAUSE_SECONDS)
            else:
                logger.info("\nНет новых данных в батче, но продолжаем поиск...")

    except KeyboardInterrupt:
        logger.info("\nОбработка прервана пользователем")
    except Exception as e:
        logger.error(f"\nКритическая ошибка: {e}")
        import traceback
        logger.error(traceback.format_exc())

    finally:
        end_time = datetime.now()
        duration = end_time - start_time
        hours_total = duration.total_seconds() / 3600

        logger.info("\n" + "=" * 60)
        logger.info("ВЫПОЛНЕНИЕ ЗАВЕРШЕНО")
        logger.info(f"Начало: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Окончание: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Длительность: {hours_total:.1f} часов")
        logger.info(f"Обработано ФИО (с новыми данными): {total_processed:,}")
        logger.info(f"Найдено новых случаев: {total_new_cases:,}")
        logger.info(f"Данные сохранены в таблицу: raw_search_results (с расширенными полями)")

        if total_processed > 0:
            speed_final = total_processed / hours_total
            logger.info(f"Средняя скорость: {speed_final:.0f} ФИО/час")

        logger.info("=" * 60)


if __name__ == "__main__":
    main()
