import pandas as pd
import re
import os
import sys
from pathlib import Path


# ========== АБСОЛЮТНО ФИНАЛЬНЫЙ ПАРСЕР ==========
class AbsoluteFinalParser:
    """Абсолютно финальный парсер с исправлениями"""

    def __init__(self):
        pass

    def parse(self, address):
        """Финальный парсинг"""
        if not address or pd.isna(address):
            return self._empty_result()

        addr = str(address).strip()
        if not addr:
            return self._empty_result()

        original_upper = addr.upper()
        original = addr

        result = self._empty_result()

        # 1. Индекс
        zip_match = re.search(r'^(\d{6})\b', original_upper)
        if zip_match:
            result['почтовый индекс'] = zip_match.group(1)
            original_upper = original_upper[zip_match.end():].strip()

        # 2. Регион (особое внимание на автономные округа)
        region_info = self._extract_region_final(original_upper)
        if region_info:
            result['регион регистрации'] = region_info['formatted']
            original_upper = original_upper.replace(region_info['found'], '', 1).strip()

        # 3. Район
        district_info = self._extract_district_final(original_upper)
        if district_info:
            result['район'] = district_info['formatted']
            original_upper = original_upper.replace(district_info['found'], '', 1).strip()

        # 4. Город (исправляем "Г САРАНСК", "Г КАЗАНЬ", "Г УРАЙ")
        settlement_info = self._extract_city_final(original_upper, original)
        if settlement_info:
            result['город'] = settlement_info['formatted']
            original_upper = original_upper.replace(settlement_info['found'], '', 1).strip()

        # 5. Улица/микрорайон (особое внимание на "МКР 2-Й")
        street_info = self._extract_street_or_mkr(original_upper)
        if street_info:
            result['улица'] = street_info['formatted']
            original_upper = original_upper.replace(street_info['found'], '', 1).strip()

        # 6. НОМЕРА - ГЛАВНОЕ ИСПРАВЛЕНИЕ!
        numbers_info = self._extract_numbers_final_logic(original_upper, original)
        result.update(numbers_info)

        # 7. ФИНАЛЬНАЯ ОЧИСТКА
        self._absolute_final_cleanup(original, result)

        return result

    def _empty_result(self):
        return {k: '' for k in ['почтовый индекс', 'регион регистрации', 'район',
                                'город', 'улица', 'дом', 'здание', 'квартира']}

    def _extract_region_final(self, text):
        """Финальное извлечение региона"""
        # 1. Автономные округа с дефисами
        ao_patterns = [
            (r'ХАНТЫ-МАНСИЙСКИЙ\s+АВТОНОМНЫЙ\s+ОКРУГ\s*-\s*ЮГРА(?:\s+АО)?',
             'Ханты-Мансийский автономный округ - Югра'),
            (r'ЯМАЛО-НЕНЕЦКИЙ\s+АВТОНОМНЫЙ\s+ОКРУГ', 'Ямало-Ненецкий автономный округ'),
            (r'НЕНЕЦКИЙ\s+АВТОНОМНЫЙ\s+ОКРУГ', 'Ненецкий автономный округ'),
            (r'ЧУКОТСКИЙ\s+АВТОНОМНЫЙ\s+ОКРУГ', 'Чукотский автономный округ'),
        ]

        for pattern, formatted in ao_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return {'found': match.group(0), 'formatted': formatted}

        # 2. Республики (убираем "Г"!)
        resp_match = re.search(r'РЕСП(?:УБЛИКА)?\s+([А-ЯЁ\-]+(?:\s+[А-ЯЁ\-]*)?)', text, re.IGNORECASE)
        if resp_match:
            region_name = resp_match.group(1).strip()
            # УБИРАЕМ "Г" если прилипло в конце!
            region_name = re.sub(r'\s+Г$', '', region_name, flags=re.IGNORECASE)
            return {
                'found': resp_match.group(0),
                'formatted': f"Республика {region_name.title()}"
            }

        # 3. Области и края
        patterns = [
            (r'([А-ЯЁ]+(?:\s+[А-ЯЁ]+)?)\s+ОБЛ(?:АСТЬ)?(?:\s*-\s*[А-ЯЁ]+)?', 'область'),
            (r'([А-ЯЁ]+(?:\s+[А-ЯЁ]+)?)\s+КРАЙ', 'край'),
        ]

        for pattern, region_type in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                region_name = match.group(1).strip()
                return {
                    'found': match.group(0),
                    'formatted': f"{region_name.title()} {region_type}"
                }

        # 4. Города федерального значения
        fed_cities = {
            'Г МОСКВА': 'г. Москва',
            'Г САНКТ-ПЕТЕРБУРГ': 'г. Санкт-Петербург',
            'Г СЕВАСТОПОЛЬ': 'г. Севастополь'
        }

        for city_key, city_name in fed_cities.items():
            if city_key in text:
                return {'found': city_key, 'formatted': city_name}

        return None

    def _extract_district_final(self, text):
        """Финальное извлечение района"""
        pattern = r'([А-ЯЁ][А-ЯЁ\s\-]+?)\s+(?:Р[\-\s]?Н|РАЙОН)'
        match = re.search(pattern, text, re.IGNORECASE)

        if match:
            name = match.group(1).strip()
            if not re.match(r'^(Г|П|С|Д)\b', name, re.IGNORECASE):
                return {
                    'found': match.group(0),
                    'formatted': f"{name.title()} район"
                }
        return None

    def _extract_city_final(self, text, original):
        """Финальное извлечение города"""
        original_upper = original.upper()

        # 1. Федеральные города
        fed_cities = {
            'Г МОСКВА': 'г. Москва',
            'Г САНКТ-ПЕТЕРБУРГ': 'г. Санкт-Петербург',
            'Г СЕВАСТОПОЛЬ': 'г. Севастополь'
        }

        for city_key, city_name in fed_cities.items():
            if city_key in original_upper:
                # Находим позицию в тексте
                if city_key in text:
                    return {'found': city_key, 'formatted': city_name}

        # 2. Обычные города ("Г САРАНСК", "Г КАЗАНЬ", "Г УРАЙ")
        city_match = re.search(r'\b(?:Г|ГОРОД|Г\.)\s+([А-ЯЁ][А-ЯЁ\s\-]*?)(?=\s|$|,)', text, re.IGNORECASE)
        if city_match:
            city_name = city_match.group(1).strip()
            return {
                'found': city_match.group(0),
                'formatted': f"г. {city_name.title()}"
            }

        # 3. Села и деревни
        village_patterns = [
            (r'\b(С|СЕЛО|С\.)\s+([А-ЯЁ][А-ЯЁ\s\-]+?)(?=\s|$|,)', 'с.'),
            (r'\b(Д|ДЕРЕВНЯ|Д\.)\s+([А-ЯЁ][А-ЯЁ\s\-]+?)(?=\s|$|,)', 'д.'),
            (r'\b(П|ПОСЕЛОК|П\.|ПГТ)\s+([А-ЯЁ][А-ЯЁ\s\-]+?)(?=\s|$|,)', 'п.'),
        ]

        for pattern, prefix in village_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(2).strip()
                return {
                    'found': match.group(0),
                    'formatted': f"{prefix} {name.title()}"
                }

        return None

    def _extract_street_or_mkr(self, text):
        """Извлечение улицы или микрорайона"""
        # Сначала микрорайон (особый случай!)
        mkr_match = re.search(r'\b(МКР|МИКРОРАЙОН|МКР\.)\s+([А-ЯЁ0-9\-].*?)(?=\s+\d|$)', text, re.IGNORECASE)
        if mkr_match:
            mkr_name = mkr_match.group(2).strip()
            # "2-Й" -> "2-й"
            if re.match(r'^\d+-[А-ЯЁ]$', mkr_name, re.IGNORECASE):
                mkr_name = mkr_name.lower()
            return {
                'found': mkr_match.group(0),
                'formatted': f"микрорайон {mkr_name.title()}"
            }

        # Затем улицы
        patterns = [
            (r'\b(УЛ|УЛИЦА|УЛ\.)\s+([А-ЯЁ0-9].*?)(?=\s+\d|$)', 'улица'),
            (r'\b(ПР-КТ|ПРОСПЕКТ|ПР\.)\s+([А-ЯЁ0-9].*?)(?=\s+\d|$)', 'проспект'),
            (r'\b(ПЕР|ПЕРЕУЛОК|ПЕР\.)\s+([А-ЯЁ0-9].*?)(?=\s+\d|$)', 'переулок'),
            (r'\b(Ш|ШОССЕ|Ш\.)\s+([А-ЯЁ0-9].*?)(?=\s+\d|$)', 'шоссе'),
            (r'\b(ПРОЕЗД|ПРОЕЗД\.)\s+([А-ЯЁ0-9].*?)(?=\s+\d|$)', 'проезд'),
            (r'\b(НАБ|НАБЕРЕЖНАЯ|НАБ\.)\s+([А-ЯЁ0-9].*?)(?=\s+\d|$)', 'набережная'),
        ]

        for pattern, street_type in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                street_name = match.group(2).strip()
                return {
                    'found': match.group(0),
                    'formatted': f"{street_type} {street_name.title()}"
                }
        return None

    def _extract_numbers_final_logic(self, text, original):
        """Финальная логика извлечения номеров"""
        result = {'дом': '', 'здание': '', 'квартира': ''}
        original_upper = original.upper()

        # 1. Специальные случаи

        # Случай 1: "МКР 2-Й 92  27" -> дом 92, квартира 27 (2-Й это название микрорайона!)
        if 'МКР' in original_upper and re.search(r'МКР.*?\d+\s+\d+\s+\d+', original_upper):
            numbers = re.findall(r'\b(\d+)\b', original_upper)
            if len(numbers) >= 3:
                # Первое число после МКР - часть названия, второе - дом, третье - квартира
                result['дом'] = numbers[1] if len(numbers) > 1 else ''
                result['квартира'] = numbers[2] if len(numbers) > 2 else ''
                return result

        # Случай 2: "УЛ ЧТО-ТО ЧИСЛО  ЧИСЛО" (двойной пробел)
        if re.search(r'УЛ.*?\d+\s{2,}\d+', original_upper):
            numbers = re.findall(r'\b(\d+)\b', original_upper)
            if len(numbers) >= 2:
                result['дом'] = numbers[-2]  # предпоследнее число
                result['квартира'] = numbers[-1]  # последнее число
                return result

        # 2. Обычная логика

        # Ищем все числа в оставшемся тексте
        numbers_in_text = re.findall(r'\b(\d+[А-Я]?)\b', text)

        if numbers_in_text:
            if len(numbers_in_text) == 1:
                # ЕДИНСТВЕННОЕ ЧИСЛО В КОНЦЕ АДРЕСА - ЭТО ДОМ!
                result['дом'] = numbers_in_text[0]
            elif len(numbers_in_text) == 2:
                result['дом'] = numbers_in_text[0]
                result['квартира'] = numbers_in_text[1]
            elif len(numbers_in_text) >= 3:
                result['дом'] = numbers_in_text[0]
                # Второе число < 10 = корпус
                if numbers_in_text[1].isdigit() and int(numbers_in_text[1]) < 10:
                    result['здание'] = numbers_in_text[1]
                    result['квартира'] = numbers_in_text[2] if len(numbers_in_text) > 2 else ''
                else:
                    result['квартира'] = numbers_in_text[1]

        # 3. Если не нашли, ищем в оригинале
        if not result['дом']:
            # Ищем последнее число в адресе
            all_numbers = re.findall(r'\b(\d+[А-Я]?)\b', original_upper)
            if all_numbers:
                # Если в адресе только одно число в конце - это дом
                # Проверяем, что после этого числа ничего нет (или только пробелы)
                last_num = all_numbers[-1]
                last_num_pos = original_upper.rfind(last_num)
                after_last = original_upper[last_num_pos + len(last_num):].strip()

                if not after_last or after_last == '':
                    result['дом'] = last_num

        return result

    def _absolute_final_cleanup(self, original, result):
        """Абсолютно финальная очистка"""
        original_upper = original.upper()

        # 1. УБИРАЕМ ДУБЛИ ДОМА И КВАРТИРЫ
        if result['дом'] and result['квартира'] and result['дом'] == result['квартира']:
            # Если дом и квартира одинаковые - оставляем только дом
            result['квартира'] = ''

        # 2. Особый случай: "51  " (одиночное число в конце) - ТОЛЬКО ДОМ!
        # Проверяем, есть ли в оригинале только одно число в самом конце
        all_numbers = re.findall(r'\b(\d+[А-Я]?)\b', original_upper)
        if len(all_numbers) == 1:
            # Находим позицию этого числа
            num = all_numbers[0]
            num_pos = original_upper.find(num)
            # Проверяем, что после числа только пробелы или ничего
            after_num = original_upper[num_pos + len(num):].strip()
            if not after_num:
                # ЕДИНСТВЕННОЕ ЧИСЛО В КОНЦЕ - ТОЛЬКО ДОМ!
                result['дом'] = num
                result['квартира'] = ''

        # 3. Проверяем наличие города по контексту
        if not result['город']:
            # Ищем "Г ЧТО-ТО" в оригинале
            g_match = re.search(r'\bГ\s+([А-ЯЁ][А-ЯЁ\s]+?)(?=\s|$)', original_upper)
            if g_match:
                city_name = g_match.group(1).strip()
                # Проверяем, что это не часть улицы
                if not any(word in city_name for word in ['УЛ', 'ПР', 'ПЕР', 'ПРОЕЗД']):
                    result['город'] = f"г. {city_name.title()}"

        # 4. Убираем "Г" из региона если осталось
        if result['регион регистрации'] and ' Г' in result['регион регистрации']:
            result['регион регистрации'] = result['регион регистрации'].replace(' Г', '')


def parse_address(address):
    """Основная функция парсинга"""
    parser = AbsoluteFinalParser()
    return parser.parse(address)


def process_excel_file(file_path):
    """Обработка Excel файла"""
    try:
        df = pd.read_excel(file_path)

        # Поиск столбца с адресами
        address_column = None
        for col in df.columns:
            col_lower = str(col).lower()
            if any(kw in col_lower for kw in ['адрес', 'address', 'полный', 'регистрац']):
                address_column = col
                print(f"✓ Найден столбец: '{col}'")
                break

        if not address_column:
            print("Столбцы:", list(df.columns))
            address_column = input("Введите название столбца: ")

        # Добавляем новые колонки
        new_cols = ['почтовый индекс', 'регион регистрации', 'район',
                    'город', 'улица', 'дом', 'здание', 'квартира']
        for col in new_cols:
            df[col] = ''

        print(f"🔄 Обработка {len(df)} строк...")

        # Парсим все адреса
        parser = AbsoluteFinalParser()

        for idx, row in df.iterrows():
            if idx % 100 == 0 and idx > 0:
                print(f"  Обработано {idx} строк")

            address = row[address_column]
            if pd.notna(address) and str(address).strip():
                parsed = parser.parse(address)
                for col in new_cols:
                    df.at[idx, col] = parsed[col]

        # Сохраняем
        output_path = f"{os.path.splitext(file_path)[0]}_абсолютный_финал.xlsx"
        df.to_excel(output_path, index=False)

        print(f"\n✅ Готово! Файл сохранен: {output_path}")

        # Тестовые проблемные примеры
        print("\n🧪 Самые проблемные адреса:")
        print("=" * 120)

        test_addresses = [
            "... ",

        ]

        for addr in test_addresses:
            parsed = parse_address(addr)
            print(f"\n📫 Адрес: '{addr}'")
            print("📝 Результат:")
            for k, v in parsed.items():
                if v:
                    print(f"  {k}: {v}")
            print("-" * 120)

        return output_path

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return None


def main():
    file_path = r"... "

    if not os.path.exists(file_path):
        print(f"Файл не найден: {file_path}")
        dir_path = r"... "
        if os.path.exists(dir_path):
            files = list(Path(dir_path).glob('*.xlsx')) + list(Path(dir_path).glob('*.xls'))
            if files:
                print("Найденные файлы:")
                for i, f in enumerate(files, 1):
                    print(f"{i}. {f.name}")
                try:
                    choice = int(input(f"Выберите файл (1-{len(files)}): ")) - 1
                    file_path = str(files[choice])
                except:
                    return

    print(f"🎯 Обработка: {file_path}")
    output = process_excel_file(file_path)

    if output:
        open_file = input("\n📂 Открыть файл? (y/n): ")
        if open_file.lower() == 'y':
            os.startfile(output)


if __name__ == "__main__":
    main()
