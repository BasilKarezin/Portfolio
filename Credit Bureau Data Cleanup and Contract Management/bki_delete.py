import xml.etree.ElementTree as ET
from xml.dom import minidom
from datetime import datetime
from typing import List, Dict, Optional
import pyodbc
import logging
import sys
import re
import uuid
import pandas as pd
import pyperclip
import io
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('correction_generator.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)


class DefaultValues:
    """Значения по умолчанию для полей, отсутствующих в БД."""
    ISSUE_DATE = "-"  # Заглушка для issueDate
    ISSUER_PLACEHOLDER = "-"  # Заглушка для docIssuer, middleName
    BIRTH_PLACE_PLACEHOLDER = "-"  # Заглушка для birthPlace (до 09.05.2025)
    DEPT_CODE = "000-000"  # Код подразделения по умолчанию
    CUTOFF_DATE = datetime(2025, 5, 9)  # Дата отсечки для удаления birthPlace


class ExcelClipboardReader:
    """Класс для чтения данных из буфера обмена Excel"""

    @staticmethod
    def read_from_clipboard() -> List[str]:
        """
        Чтение данных из буфера обмена Excel

        Returns:
            Список ID контрактов
        """
        try:
            # Получаем данные из буфера обмена
            clipboard_data = pyperclip.paste()

            if not clipboard_data:
                logging.error("Буфер обмена пуст")
                return []

            logging.info(f"Получено {len(clipboard_data)} символов из буфера обмена")

            # Пробуем разные способы разбора данных
            contract_ids = []

            # Способ 1: Пробуем как CSV (табличные данные)
            try:
                # Используем StringIO для имитации файла
                df = pd.read_csv(io.StringIO(clipboard_data), header=None, sep='\t|,', engine='python')

                # Собираем все непустые значения из первого столбца
                for col in df.columns:
                    for value in df[col].dropna():
                        str_value = str(value).strip()
                        if str_value and str_value not in contract_ids:
                            contract_ids.append(str_value)

                logging.info(f"Разобрано как таблица: {len(contract_ids)} ID")
            except Exception as e:
                logging.debug(f"Не удалось разобрать как таблицу: {e}")

            # Способ 2: Если не получилось как таблица, разбираем построчно
            if not contract_ids:
                # Разделяем по строкам и символам-разделителям
                lines = clipboard_data.replace('\r\n', '\n').split('\n')

                for line in lines:
                    # Разделяем строку по возможным разделителям
                    parts = re.split(r'[,\t;|\s]+', line.strip())
                    for part in parts:
                        if part.strip():
                            contract_ids.append(part.strip())

            # Удаляем дубликаты, сохраняя порядок
            seen = set()
            unique_ids = []
            for id_val in contract_ids:
                if id_val not in seen:
                    seen.add(id_val)
                    unique_ids.append(id_val)

            logging.info(f"Всего уникальных ID: {len(unique_ids)}")

            if len(unique_ids) > 0:
                logging.info(f"Первые 5 ID: {unique_ids[:5]}")

            return unique_ids

        except Exception as e:
            logging.error(f"Ошибка при чтении из буфера обмена: {e}")
            return []

    @staticmethod
    def read_from_clipboard_batch() -> List[str]:
        """
        Чтение данных из буфера обмена с пакетной обработкой для больших объемов

        Returns:
            Список ID контрактов
        """
        try:
            clipboard_data = pyperclip.paste()

            if not clipboard_data:
                return []

            # Обрабатываем большие объемы данных
            contract_ids = []

            # Используем более эффективный метод для больших данных
            buffer = io.StringIO(clipboard_data)

            # Читаем построчно
            for line in buffer:
                # Обрабатываем каждую строку
                if line.strip():
                    # Разделяем по табуляции (стандартный разделитель Excel)
                    parts = line.strip().split('\t')
                    for part in parts:
                        if part.strip():
                            contract_ids.append(part.strip())

            buffer.close()

            # Удаляем дубликаты
            unique_ids = list(dict.fromkeys(contract_ids))

            logging.info(f"Прочитано {len(unique_ids)} уникальных ID из буфера обмена")

            return unique_ids

        except Exception as e:
            logging.error(f"Ошибка при пакетном чтении: {e}")
            return []


def get_contract_ids_from_excel() -> List[str]:
    """
    Функция для получения ID контрактов из буфера обмена Excel

    Returns:
        Список ID контрактов
    """
    print("\nИнструкция:")
    print("1. Откройте Excel файл с ID контрактов")
    print("2. Выделите столбец или диапазон с ID контрактов")
    print("3. Нажмите Ctrl+C (Копировать)")
    print("4. Вернитесь в это окно и нажмите Enter")

    input("\nНажмите Enter после копирования данных из Excel...")

    # Читаем из буфера обмена
    contract_ids = ExcelClipboardReader.read_from_clipboard()

    if contract_ids:
        print(f"\n✅ Прочитано {len(contract_ids)} ID контрактов")
        print(f"📋 Первые 10: {', '.join(contract_ids[:10])}")

        # Подтверждение
        response = input("\nПродолжить обработку? (y/n): ").strip().lower()
        if response != 'y':
            return []

        return contract_ids
    else:
        print("❌ Не удалось прочитать данные из буфера обмена")
        return []


class TextNormalizer:
    """Класс для нормализации текстовых полей согласно требованиям схемы"""

    @staticmethod
    def to_uppercase_russian(text: Optional[str]) -> str:
        """
        Преобразование строки в заглавные буквы с учетом русских символов.
        Возвращает пустую строку, если входные данные None или пусты.
        """
        if not text or pd.isna(text):
            return ""

        text = str(text).strip()
        if not text:
            return ""

        # Маппинг строчных букв на заглавные (русский алфавит)
        lower_to_upper = {
            'а': 'А', 'б': 'Б', 'в': 'В', 'г': 'Г', 'д': 'Д',
            'е': 'Е', 'ё': 'Ё', 'ж': 'Ж', 'з': 'З', 'и': 'И',
            'й': 'Й', 'к': 'К', 'л': 'Л', 'м': 'М', 'н': 'Н',
            'о': 'О', 'п': 'П', 'р': 'Р', 'с': 'С', 'т': 'Т',
            'у': 'У', 'ф': 'Ф', 'х': 'Х', 'ц': 'Ц', 'ч': 'Ч',
            'ш': 'Ш', 'щ': 'Щ', 'ъ': 'Ъ', 'ы': 'Ы', 'ь': 'Ь',
            'э': 'Э', 'ю': 'Ю', 'я': 'Я'
        }

        result = []
        for char in text:
            if char in lower_to_upper:
                result.append(lower_to_upper[char])
            else:
                result.append(char.upper())
        return ''.join(result)

    @staticmethod
    def normalize_fio(text: Optional[str]) -> str:
        """Нормализация ФИО для полей lastName, firstName, middleName"""
        if not text or pd.isna(text):
            return ""
        normalized = TextNormalizer.to_uppercase_russian(str(text))
        normalized = ' '.join(normalized.split())
        return normalized

    @staticmethod
    def normalize_doc_issuer(text: Optional[str]) -> Optional[str]:
        """
        Нормализация поля docIssuer.
        Возвращает None, если после нормализации строка пуста.
        """
        if not text or pd.isna(text):
            return None
        normalized = TextNormalizer.to_uppercase_russian(str(text))
        normalized = ' '.join(normalized.split())
        # Если после нормализации (удаления пробелов) строка пуста, возвращаем None
        return normalized if normalized else None

    @staticmethod
    def normalize_birth_place(text: Optional[str]) -> Optional[str]:
        """Нормализация поля birthPlace. Возвращает None, если строка пуста."""
        if not text or pd.isna(text):
            return None
        normalized = TextNormalizer.to_uppercase_russian(str(text))
        normalized = ' '.join(normalized.split())
        return normalized if normalized else None

    @staticmethod
    def normalize_passport_number(passport: Optional[str]) -> str:
        """Нормализация номера паспорта - только цифры"""
        if not passport or pd.isna(passport):
            return ""
        digits = re.sub(r'\D', '', str(passport))
        if len(digits) == 10:
            return digits
        elif len(digits) > 10:
            return digits[:10]
        else:
            return digits.ljust(10, '0')[:10]

    @staticmethod
    def normalize_uuid(uuid_str: Optional[str]) -> str:
        """Нормализация UUID - приведение к нижнему регистру"""
        if not uuid_str or pd.isna(uuid_str):
            return ""
        normalized = str(uuid_str).strip().lower()
        try:
            if len(normalized) > 20 and '-' in normalized:
                uuid_obj = uuid.UUID(normalized)
                return str(uuid_obj)
        except (ValueError, AttributeError):
            pass
        return normalized


class DatabaseConnector:
    """Класс для подключения к базе данных SQL Server"""

    def __init__(self, conn_str: str):
        self.conn_str = conn_str
        self.connection = None

    def connect(self):
        """Установка соединения с базой данных"""
        try:
            self.connection = pyodbc.connect(self.conn_str)
            logging.info("Успешное подключение к базе данных")
            return self.connection
        except pyodbc.Error as e:
            logging.error(f"Ошибка подключения к базе данных: {e}")
            raise

    def disconnect(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            self.connection.close()
            logging.info("Соединение с базой данных закрыто")

    def fetch_contracts_data_batch(self, contract_ids: List[str], batch_size: int = 1000) -> List[Dict]:
        """
        Получение данных контрактов из базы данных пакетами для больших объемов
        """
        if not contract_ids:
            logging.warning("Список ID контрактов пуст")
            return []

        all_contracts_data = []
        total_batches = (len(contract_ids) + batch_size - 1) // batch_size

        try:
            cursor = self.connection.cursor()

            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(contract_ids))
                batch_ids = contract_ids[start_idx:end_idx]

                logging.info(f"Обработка пакета {batch_num + 1}/{total_batches} ({len(batch_ids)} ID)")

                # Создаем строку с плейсхолдерами для IN условия
                placeholders = ','.join('?' * len(batch_ids))

                sql = f"""
                SELECT 
                    c.contract_id, 
                    c.uuid, 
                    p.doc_date, 
                    p.doc_issued, 
                    p.passport, 
                    p.last_name, 
                    p.first_name, 
                    p.patronymic, 
                    p.deptcode, 
                    p.birth_date, 
                    p.birth_place, 
                    c.initial_debt_date
                FROM contracts c
                LEFT JOIN cont_pers_dtl cp ON c.contract_id = cp.contract_id
                LEFT JOIN persons p ON cp.person_id = p.person_id
                WHERE c.contract_id IN ({placeholders}) and cp.person_type_id = 1
                ORDER BY c.contract_id
                """

                cursor.execute(sql, batch_ids)
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                for row in rows:
                    contract_dict = dict(zip(columns, row))

                    # Преобразование дат в строки или None
                    for date_field in ['doc_date', 'birth_date', 'initial_debt_date']:
                        if contract_dict.get(date_field):
                            if hasattr(contract_dict[date_field], 'strftime'):
                                contract_dict[date_field] = contract_dict[date_field].strftime('%Y-%m-%d')
                            else:
                                try:
                                    dt = datetime.strptime(str(contract_dict[date_field]), '%Y-%m-%d')
                                    contract_dict[date_field] = dt.strftime('%Y-%m-%d')
                                except:
                                    contract_dict[date_field] = None
                        else:
                            contract_dict[date_field] = None

                    # Нормализация текстовых полей
                    contract_dict['last_name'] = TextNormalizer.normalize_fio(contract_dict.get('last_name'))
                    contract_dict['first_name'] = TextNormalizer.normalize_fio(contract_dict.get('first_name'))
                    contract_dict['patronymic'] = TextNormalizer.normalize_fio(contract_dict.get('patronymic'))
                    contract_dict['doc_issued'] = TextNormalizer.normalize_doc_issuer(contract_dict.get('doc_issued'))
                    contract_dict['birth_place'] = TextNormalizer.normalize_birth_place(
                        contract_dict.get('birth_place'))
                    contract_dict['passport'] = TextNormalizer.normalize_passport_number(contract_dict.get('passport'))
                    contract_dict['uuid'] = TextNormalizer.normalize_uuid(contract_dict.get('uuid'))

                    all_contracts_data.append(contract_dict)

                logging.info(f"Пакет {batch_num + 1} обработан, получено {len(rows)} записей")

            cursor.close()
            logging.info(f"Всего получено {len(all_contracts_data)} записей из базы данных")
            return all_contracts_data

        except pyodbc.Error as e:
            logging.error(f"Ошибка при выполнении SQL запроса: {e}")
            return []


class CorrectiveFileGenerator:
    def __init__(self, inn: str, ogrn: str, source_id: str):
        self.inn = inn
        self.ogrn = ogrn
        self.source_id = source_id
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.current_datetime = datetime.now()

    def prettify_xml(self, elem) -> bytes:
        """Форматирование XML для читаемости"""
        xml_declaration = '<?xml version="1.0" encoding="utf-8"?>\n'
        rough_string = ET.tostring(elem, encoding='utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent="  ", encoding='utf-8')
        pretty_str = pretty_xml.decode('utf-8')
        if pretty_str.startswith('<?xml'):
            end_of_first_line = pretty_str.find('\n') + 1
            pretty_str = xml_declaration + pretty_str[end_of_first_line:]
        return pretty_str.encode('utf-8')

    def create_document_root(self, contracts_count: int) -> ET.Element:
        """Создание корневого элемента документа"""
        root = ET.Element("Document")
        root.set("schemaVersion", "4.1")
        root.set("inn", self.inn)
        root.set("ogrn", self.ogrn)
        root.set("sourceID", self.source_id)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        root.set("regNumberDoc", f"{self.source_id}_{timestamp}")
        root.set("dateDoc", self.current_date)
        root.set("subjectsCount", str(contracts_count))
        root.set("groupBlocksCount", str(contracts_count))
        return root

    def create_source_section(self, org_name: str, org_short_name: str, start_date: str = "2009-09-14") -> ET.Element:
        """Создание раздела Source с информацией об организации"""
        source = ET.Element("FL_46_UL_36_OrgSource")

        source_code = ET.SubElement(source, "sourceCode")
        source_code.text = "17"

        ET.SubElement(source, "sourceRegistrationFact_1")

        full_name = ET.SubElement(source, "fullName")
        full_name.text = org_name

        short_name = ET.SubElement(source, "shortName")
        short_name.text = org_short_name

        other_name = ET.SubElement(source, "otherName")
        other_name.text = "-"

        date_start = ET.SubElement(source, "sourceDateStart")
        date_start.text = start_date

        reg_num = ET.SubElement(source, "regNum")
        reg_num.text = self.ogrn

        # TaxNum group
        tax_group = ET.SubElement(source, "TaxNum_group_FL_46_UL_36_OrgSource")
        tax_code = ET.SubElement(tax_group, "taxCode")
        tax_code.text = "1"
        tax_num = ET.SubElement(tax_group, "taxNum")
        tax_num.text = self.inn

        credit_info = ET.SubElement(source, "sourceCreditInfoDate")
        credit_info.text = self.current_date

        return source

    def create_subject_fl(self, person_data: Dict, contract_data: Dict, order_num: int) -> ET.Element:
        """
        Создание элемента Subject_FL для одного контракта с использованием заглушек.
        """
        subject = ET.Element("Subject_FL")
        title = ET.SubElement(subject, "Title")

        # --- FL_1_4_Group ---
        fl_1_4 = ET.SubElement(title, "FL_1_4_Group")
        fl_1 = ET.SubElement(fl_1_4, "FL_1_Name")
        last_name = ET.SubElement(fl_1, "lastName")
        last_name.text = person_data.get('last_name', '')
        first_name = ET.SubElement(fl_1, "firstName")
        first_name.text = person_data.get('first_name', '')

        # Отчество - всегда используем "-", если нет данных
        middle_name = ET.SubElement(fl_1, "middleName")
        patronymic_val = person_data.get('patronymic')
        middle_name.text = patronymic_val if patronymic_val else DefaultValues.ISSUER_PLACEHOLDER

        # FL_4_Doc
        fl_4 = ET.SubElement(fl_1_4, "FL_4_Doc")
        country_code = ET.SubElement(fl_4, "countryCode")
        country_code.text = "643"
        doc_code = ET.SubElement(fl_4, "docCode")
        doc_code.text = "21"

        passport = person_data.get('passport', '')
        if passport and len(passport) >= 10:
            doc_series = ET.SubElement(fl_4, "docSeries")
            doc_series.text = passport[:4]
            doc_num = ET.SubElement(fl_4, "docNum")
            doc_num.text = passport[4:10]
        else:
            doc_series = ET.SubElement(fl_4, "docSeries")
            doc_series.text = ""
            doc_num = ET.SubElement(fl_4, "docNum")
            doc_num.text = ""

        # Дата выдачи паспорта - используем заглушку 1900-01-01
        issue_date = ET.SubElement(fl_4, "issueDate")
        issue_date_val = person_data.get('doc_date')
        issue_date.text = issue_date_val if issue_date_val else DefaultValues.ISSUE_DATE

        # Орган выдачи паспорта
        doc_issuer = ET.SubElement(fl_4, "docIssuer")
        doc_issuer_val = person_data.get('doc_issued')
        doc_issuer.text = doc_issuer_val if doc_issuer_val else DefaultValues.ISSUER_PLACEHOLDER

        # Код подразделения
        dept_code = ET.SubElement(fl_4, "deptCode")
        dept_code_val = person_data.get('deptcode')
        dept_code.text = dept_code_val if dept_code_val else DefaultValues.DEPT_CODE

        foreigner_code = ET.SubElement(fl_4, "foreignerCode")
        foreigner_code.text = "3"

        # --- FL_2_5_Group ---
        fl_2_5 = ET.SubElement(title, "FL_2_5_Group")
        fl_2 = ET.SubElement(fl_2_5, "FL_2_PrevName")
        ET.SubElement(fl_2, "prevNameFlag_0")
        fl_5 = ET.SubElement(fl_2_5, "FL_5_PrevDoc")
        ET.SubElement(fl_5, "prevDocFact_0")

        # --- FL_3_Birth ---
        fl_3 = ET.SubElement(title, "FL_3_Birth")
        birth_date = ET.SubElement(fl_3, "birthDate")
        birth_date.text = person_data.get('birth_date', '') or ""

        birth_country = ET.SubElement(fl_3, "countryCode")
        birth_country.text = "999"

        # Обработка birthPlace в зависимости от даты генерации
        birth_place_val = person_data.get('birth_place')

        if birth_place_val:
            # Если есть данные, всегда добавляем тег
            birth_place = ET.SubElement(fl_3, "birthPlace")
            birth_place.text = birth_place_val
        else:
            # Если данных нет
            if self.current_datetime < DefaultValues.CUTOFF_DATE:
                # До 09.05.2025 - вставляем заглушку "-"
                birth_place = ET.SubElement(fl_3, "birthPlace")
                birth_place.text = DefaultValues.BIRTH_PLACE_PLACEHOLDER
            else:
                # После 09.05.2025 - не добавляем тег birthPlace
                pass

        # --- Events ---
        events = ET.SubElement(subject, "Events")
        event = ET.SubElement(events, "FL_Event_3_3")
        event.set("eventComment", "Причина исключения: ошибочно переданный договор")
        event.set("orderNum", str(order_num))
        event.set("eventDate", self.current_date)
        event.set("operationCode", "C.2")
        event.set("changeReason", "1")

        # FL_17_DealUid_R
        deal_uid = ET.SubElement(event, "FL_17_DealUid_R")
        uid = ET.SubElement(deal_uid, "uid")
        uid.text = str(contract_data.get('uuid', '')).lower()

        open_date = ET.SubElement(deal_uid, "openDate")
        open_date.text = contract_data.get('initial_debt_date', '') or ""

        return subject

    def generate_xml(self, contracts_data: List[Dict],
                     org_name: str = None,
                     org_short_name: str = None,
                     org_start_date: str = "2009-09-14") -> bytes:
        """
        Генерация полного XML файла
        """
        if not contracts_data:
            raise ValueError("Данные контрактов не предоставлены")

        # Значения по умолчанию
        if not org_name:
            org_name = 'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ "ФИНТРАСТ"'
        if not org_short_name:
            org_short_name = 'ООО "ФИНТРАСТ"'

        # Создание корневого элемента
        root = self.create_document_root(len(contracts_data))

        # Создание раздела Source
        source_element = self.create_source_section(org_name, org_short_name, org_start_date)
        source = ET.SubElement(root, "Source")
        source.append(source_element)

        # Создание раздела Data
        data = ET.SubElement(root, "Data")

        # Добавление Subject_FL для каждого контракта
        for i, contract_info in enumerate(contracts_data, 1):
            try:
                subject = self.create_subject_fl(
                    person_data=contract_info,
                    contract_data=contract_info,
                    order_num=i
                )
                data.append(subject)

                # Прогресс для больших объемов
                if i % 1000 == 0:
                    logging.info(f"Обработано {i} записей")

            except Exception as e:
                logging.error(f"Ошибка при обработке контракта {i}: {e}")
                continue

        # Форматирование XML
        xml_bytes = self.prettify_xml(root)
        return xml_bytes

    def save_to_file(self, xml_bytes: bytes, filepath: str = None):
        """
        Сохранение XML в файл
        """
        if filepath is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = f"correction_{timestamp}.xml"

        # Убедимся, что директория существует
        os.makedirs(os.path.dirname(os.path.abspath(filepath)) if os.path.dirname(filepath) else '.', exist_ok=True)

        with open(filepath, 'wb') as f:
            f.write(xml_bytes)

        logging.info(f"XML файл сохранен: {filepath}")
        return filepath


def fix_existing_xml(xml_file_path: str):
    """
    Исправление существующего XML файла
    """
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        current_datetime = datetime.now()

        # Исправляем ФИО (включая отчество)
        for elem_name in ['lastName', 'firstName']:
            elements = root.findall(f'.//{elem_name}')
            for elem in elements:
                if elem.text:
                    elem.text = TextNormalizer.normalize_fio(elem.text)

        # Отчество - всегда заменяем пустые на "-"
        middle_name_elements = root.findall('.//middleName')
        for elem in middle_name_elements:
            if not elem.text or elem.text.strip() == '':
                elem.text = DefaultValues.ISSUER_PLACEHOLDER
            else:
                elem.text = TextNormalizer.normalize_fio(elem.text)

        # Исправляем docIssuer
        doc_issuers = root.findall('.//docIssuer')
        for elem in doc_issuers:
            if elem.text and elem.text.strip():
                elem.text = TextNormalizer.normalize_doc_issuer(elem.text)
            else:
                elem.text = DefaultValues.ISSUER_PLACEHOLDER

        # Исправляем issueDate
        issue_dates = root.findall('.//issueDate')
        for elem in issue_dates:
            if not elem.text or elem.text.strip() == '':
                elem.text = DefaultValues.ISSUE_DATE

        # Обработка birthPlace
        birth_places = root.findall('.//birthPlace')

        if current_datetime < DefaultValues.CUTOFF_DATE:
            # До 09.05.2025 - заменяем пустые на "-"
            for elem in birth_places:
                if elem.text and elem.text.strip():
                    elem.text = TextNormalizer.normalize_birth_place(elem.text)
                else:
                    elem.text = DefaultValues.BIRTH_PLACE_PLACEHOLDER
        else:
            # После 09.05.2025 - удаляем пустые теги
            for parent in root.findall('.//FL_3_Birth'):
                birth_place_elem = parent.find('birthPlace')
                if birth_place_elem is not None:
                    if not birth_place_elem.text or birth_place_elem.text.strip() == '':
                        parent.remove(birth_place_elem)
                    else:
                        birth_place_elem.text = TextNormalizer.normalize_birth_place(birth_place_elem.text)

        # Исправляем UUID
        uid_elements = root.findall('.//uid')
        for elem in uid_elements:
            if elem.text:
                elem.text = TextNormalizer.normalize_uuid(elem.text)

        # Сохраняем исправленный файл
        output_path = xml_file_path.replace('.xml', '_fixed.xml')

        xml_declaration = '<?xml version="1.0" encoding="utf-8"?>\n'
        rough_string = ET.tostring(root, encoding='utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty_xml = reparsed.toprettyxml(indent="  ", encoding='utf-8')

        pretty_str = pretty_xml.decode('utf-8')
        if pretty_str.startswith('<?xml'):
            end_of_first_line = pretty_str.find('\n') + 1
            pretty_str = xml_declaration + pretty_str[end_of_first_line:]

        with open(output_path, 'wb') as f:
            f.write(pretty_str.encode('utf-8'))

        print(f"Исправленный файл сохранен как: {output_path}")
        return output_path

    except Exception as e:
        print(f"Ошибка при исправлении файла: {e}")
        return None


def main():
    """Основная функция для запуска генерации корректировочных файлов"""

    # Параметры подключения к БД
    conn_str = (
        "Driver={SQL Server Native Client 11.0};"
        "Server=-;"
        "Database=-;"
        "UID=-;"
        "PWD=-;"
    )

    # Данные организации
    ORGANIZATION = {
        "inn": "-",
        "ogrn": "-",
        "source_id": "-",
        "name": '-',
        "short_name": '-',
        "start_date": "-"
    }

    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        if sys.argv[1] == '--fix':
            if len(sys.argv) > 2:
                xml_file = sys.argv[2]
                fix_existing_xml(xml_file)
            else:
                print("Укажите путь к XML файлу для исправления")
                print("Пример: python script.py --fix D:\\путь\\к\\файлу.xml")
            return
        elif sys.argv[1] == '--excel':
            # Режим чтения из Excel через буфер обмена
            contract_ids = get_contract_ids_from_excel()
        else:
            # Обычный режим с аргументами
            contract_ids = sys.argv[1:]
    else:
        # Интерактивный режим
        print("Выберите режим работы:")
        print("1. Ввести ID вручную (через запятую)")
        print("2. Вставить из Excel (Ctrl+C)")
        print("3. Исправить существующий XML файл")

        choice = input("\nВаш выбор (1/2/3): ").strip()

        if choice == '3':
            xml_path = input("Введите путь к XML файлу: ").strip()
            fix_existing_xml(xml_path)
            return
        elif choice == '2':
            contract_ids = get_contract_ids_from_excel()
        else:
            input_ids = input("Введите ID контрактов через запятую: ").strip()
            contract_ids = [id.strip() for id in input_ids.split(',')] if input_ids else []

    if not contract_ids:
        logging.error("Не указаны ID контрактов для обработки")
        return

    logging.info(f"Начало обработки {len(contract_ids)} контрактов")
    logging.info(f"Первые 10: {contract_ids[:10]}")

    try:
        # Инициализация и подключение к БД
        db = DatabaseConnector(conn_str)
        db.connect()

        # Получение данных контрактов пакетами
        contracts_data = db.fetch_contracts_data_batch(contract_ids, batch_size=1000)

        if not contracts_data:
            logging.warning("Не найдено данных по указанным контрактам")
            return

        logging.info(f"Найдено данных по {len(contracts_data)} контрактам")

        # Инициализация генератора
        generator = CorrectiveFileGenerator(
            inn=ORGANIZATION["inn"],
            ogrn=ORGANIZATION["ogrn"],
            source_id=ORGANIZATION["source_id"]
        )

        # Генерация XML
        logging.info("Начало генерации XML...")
        xml_bytes = generator.generate_xml(
            contracts_data=contracts_data,
            org_name=ORGANIZATION["name"],
            org_short_name=ORGANIZATION["short_name"],
            org_start_date=ORGANIZATION["start_date"]
        )

        # Сохранение файла
        output_file = generator.save_to_file(xml_bytes)

        # Вывод сводки
        logging.info("=" * 50)
        logging.info("СВОДКА:")
        logging.info(f"✅ Файл: {output_file}")
        logging.info(f"📊 Контрактов обработано: {len(contracts_data)}")
        logging.info(f"📋 ID в файле: {len(contract_ids)}")
        logging.info(f"⚠️ Не найдено в БД: {len(contract_ids) - len(contracts_data)}")

        # Отключение от БД
        db.disconnect()

        print("\n" + "=" * 60)
        print("✅ КОРРЕКТИРОВОЧНЫЙ ФАЙЛ УСПЕШНО СОЗДАН!")
        print("=" * 60)
        print(f"📁 Расположение: {output_file}")
        print(f"📊 Количество записей: {len(contracts_data)}")
        print(f"🔍 Все UUID преобразованы в нижний регистр")
        print(f"🔧 Пустые поля (middleName, docIssuer) заполнены заглушкой '-'")
        print(f"🔧 Пустые даты (issueDate) заполнены заглушкой '-'")

        if generator.current_datetime < DefaultValues.CUTOFF_DATE:
            print(f"🔧 Пустые birthPlace заполнены заглушкой '-' (до 09.05.2025)")
        else:
            print(f"🔧 Пустые birthPlace удалены из XML (после 09.05.2025)")

        print("=" * 60)

    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("=" * 60)
    print("ГЕНЕРАТОР КОРРЕКТИРОВОЧНЫХ ФАЙЛОВ ДЛЯ БКИ")
    print("=" * 60)
    print("Версия: 2.3 (с поддержкой удаления birthPlace после 09.05.2025)")
    print()

    # Проверка наличия необходимых библиотек
    try:
        import pyperclip
        import pandas as pd
    except ImportError as e:
        print(f"❌ Ошибка: не установлена необходимая библиотека")
        print("Установите зависимости:")
        print("pip install pyperclip pandas")
        sys.exit(1)

    main()
