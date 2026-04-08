import pyodbc
import pandas as pd
import re

# Настройки подключения к БД
DB_CONFIG = {
    'driver': '{SQL Server Native Client 11.0}',
    'server': '-',
    'database': '-',
    'uid': '-',
    'pwd': '-'
}


def get_db_connection():
    """Создает подключение к БД"""
    connection_string = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['uid']};"
        f"PWD={DB_CONFIG['pwd']}"
    )
    return pyodbc.connect(connection_string)


def create_mappings():
    """Создает маппинги для стандартизации"""

    # Общие значения для NULL/пустых
    null_values = {
        'NULL': None,
        'null': None,
        '': None,
        '-': None,
        '0': None,
        '9': None,  # Добавляем для region
    }

    # Маппинг для городов (только действительно неправильные написания)
    city_mapping = {
        **null_values,
        # Неправильные написания
        'г. Махачкла': 'Махачкала',
        '2-я Гавриловка': 'Гавриловка',
        'ВАСИЛЬЕВКА': 'Васильевка',
        'БЕРДЯНСК': 'Бердянск',
        'МЕЛИТОПОЛЬ': 'Мелитополь',
        'имени Полины Осипенко': 'им. Полины Осипенко',
        'Киргиз - Мияки': 'Киргиз-Мияки',
        'Лысые горы': 'Лысые Горы',
        'В. Яркеево': 'Верхнее Яркеево',
        'Сочи-Адлер': 'Сочи',
    }

    # Маппинг для регионов
    region_mapping = {
        **null_values,
        # Республики с ошибками
        'РеспубликаТатарстан': 'Республика Татарстан',
        'Республика  Коми': 'Республика Коми',
        'Республике Саха (Якутия) ': 'Республика Саха (Якутия)',
        'Республика Северная Осетия-Алания': 'Республика Северная Осетия - Алания',
        'Республика Северная Осетия – Алания': 'Республика Северная Осетия - Алания',
        'Республика Северная Осетия - Алания': 'Республика Северная Осетия - Алания',
        'Чувашская Республика — Чувашия': 'Чувашская Республика',
        'Чувашская Республика - Чувашия': 'Чувашская Республика',
        'Чеченская Респубоика': 'Чеченская Республика',
        'Марий-Эл': 'Республика Марий Эл',
        'Республика Марий Эл': 'Республика Марий Эл',
        'Саха /Якутия/': 'Республика Саха (Якутия)',
        'Саха (Якутия)': 'Республика Саха (Якутия)',
        'Сахалинская': 'Сахалинская область',
        'Саратовская облясть': 'Саратовская область',
        'Смоленская': 'Смоленская область',
        'Нижегородская': 'Нижегородская область',
        'Омская': 'Омская область',
        'Новгородская': 'Новгородская область',
        'Архангельская': 'Архангельская область',
        'Тверская обл.': 'Тверская область',
        'Тверская Область': 'Тверская область',
        'Ярославская': 'Ярославская область',
        'Красноярский': 'Красноярский край',
        'Краснодарский': 'Краснодарский край',
        'Приморский': 'Приморский край',
        'Алтайский': 'Алтайский край',
        'Брянская': 'Брянская область',
        'Самарская': 'Самарская область',
        'Оренбургская область': 'Оренбургская область',
        'Саратовская область': 'Саратовская область',
        'Ленинградская облась': 'Ленинградская область',
        'Тульской области': 'Тульская область',
        'Иркуская': 'Иркутская область',
        'Казахстан': None,
        'Донецкач Народная Республика': 'Донецкая Народная Республика',
        'Донецкая Народная Республика': 'Донецкая Народная Республика',
        'Луганская Народная Республика': 'Луганская Народная Республика',
        'ЗАПОРОЖСКАЯ ОБЛАСТЬ': 'Запорожская область',

        # Города федерального значения
        'г.Москва': 'Москва',
        'г. Москва': 'Москва',
        'г. Санкт-Петербург': 'Санкт-Петербург',
        'Санкт-Петербург': 'Санкт-Петербург',

        # Автономные округа
        'Ханты-Мансийский автономный округ-Югра': 'Ханты-Мансийский автономный округ - Югра',
        'Ханты-Мансийский автономный округ - Югра': 'Ханты-Мансийский автономный округ - Югра',
        'Ямало-Ненецкий автономный округ': 'Ямало-Ненецкий автономный округ',
        'Ямало-Ненецкий': 'Ямало-Ненецкий автономный округ',

        # Регионы без слова "Республика"
        'Коми': 'Республика Коми',
        'Дагестан': 'Республика Дагестан',
        'Татарстан': 'Республика Татарстан',
        'Адыгея': 'Республика Адыгея',
        'Ингушетия': 'Республика Ингушетия',
        'Бурятия': 'Республика Бурятия',
        'Алтай': 'Республика Алтай',
        'Крым': 'Республика Крым',
        'Башкортостан': 'Республика Башкортостан',
        'Карелия': 'Республика Карелия',
        'Мордовия': 'Республика Мордовия',
        'Удмуртская республика': 'Удмуртская Республика',
        'Республика башкортостан': 'Республика Башкортостан',
        'Чувашская республика': 'Чувашская Республика',
        'Алтай': 'Республика Алтай',

        # Края и области с ошибками
        'Амурская': 'Амурская область',
        'Свердловская': 'Свердловская область',
        'Костромская': 'Костромская область',
        'Челябинская': 'Челябинская область',
        'Курганская': 'Курганская область',
        'Орловская': 'Орловская область',
        'Ульяновская': 'Ульяновская область',
        'Псковская область': 'Псковская область',
        'Московская область': 'Московская область',
        'Тульская область': 'Тульская область',
        'Томская область': 'Томская область',
        'Кемеровская область - Кузбасс': 'Кемеровская область',
        'Камчатский край': 'Камчатский край',
        'Забайкальский край': 'Забайкальский край',
        'Приморский край': 'Приморский край',
        'Краснодарский край': 'Краснодарский край',
        'Красноярский край': 'Красноярский край',

        # Прочие
        'Смидовичский район': 'Еврейская автономная область',
        'Октябрьский район': None,
        'Брянский': 'Брянская область',
        'Република Хакасия': 'Республика Хакасия',
        'Тамбовская область': 'Тамбовская область',  # Исправление для Калязин
    }

    # Для territory_of_service используем те же маппинги, что и для city
    territory_mapping = {
        **null_values,
        # Специфичные для territory_of_service
        'Смоленское и г.Белокуриха': 'Смоленский район, Белокуриха',
        'В. Яркеево': 'Верхнее Яркеево',
        'Киргиз - Мияки': 'Киргиз-Мияки',
        'имени Полины Осипенко': 'им. Полины Осипенко',
        'Лысые горы': 'Лысые Горы',
        'Сочи-Адлер': 'Сочи',
        'c. Селты': 'Селты',
        'ВАСИЛЬЕВКА': 'Васильевка',
        'БЕРДЯНСК': 'Бердянск',
        'МЕЛИТОПОЛЬ': 'Мелитополь',
    }

    return city_mapping, region_mapping, territory_mapping


def clean_prefixes(cursor, column_name):
    """Удаляет префиксы из указанной колонки"""
    prefixes = [
        ('г.', 3, "г."),
        ('гор.', 5, "гор."),
        ('с.', 3, "с."),
        ('п.', 3, "п."),
        ('ст.', 5, "ст."),
        ('а.', 3, "а."),
        ('пгт.', 5, "пгт."),
        ('поселок ', 8, "поселок "),
        ('c.', 3, "c."),  # латинская c
        ('Ст.', 5, "Ст."),  # с заглавной буквы
    ]

    for prefix, length, description in prefixes:
        query = f"""
        UPDATE fssp_reestr 
        SET {column_name} = LTRIM(RTRIM(SUBSTRING({column_name}, {length}, LEN({column_name}))))
        WHERE {column_name} LIKE '{prefix}%' 
          AND LEN({column_name}) > {length - 1}
          AND {column_name} IS NOT NULL
        """
        cursor.execute(query)
        if cursor.rowcount > 0:
            print(f"  Префикс '{description}' в {column_name}: {cursor.rowcount} записей")


def apply_standardization():
    """Применяет стандартизацию ко всем колонкам"""

    print("Начинаю стандартизацию данных...")

    # Получаем маппинги
    city_mapping, region_mapping, territory_mapping = create_mappings()

    with get_db_connection() as conn:
        cursor = conn.cursor()

        # 1. Сначала удаляем все префиксы
        print("\n1. Удаление префиксов...")
        for column in ['city', 'territory_of_service']:
            print(f"\n  Обработка {column}:")
            clean_prefixes(cursor, column)

        conn.commit()

        # 2. Применяем точные маппинги для городов
        print("\n2. Стандартизация городов (точные совпадения)...")
        updates = 0
        for wrong, correct in city_mapping.items():
            if correct is None:
                query = "UPDATE fssp_reestr SET city = NULL WHERE city = ?"
                cursor.execute(query, (wrong,))
            else:
                query = "UPDATE fssp_reestr SET city = ? WHERE city = ?"
                cursor.execute(query, (correct, wrong))

            if cursor.rowcount > 0:
                updates += cursor.rowcount

        conn.commit()
        print(f"  Обновлено городов: {updates}")

        # 3. Применяем точные маппинги для регионов
        print("\n3. Стандартизация регионов (точные совпадения)...")
        updates = 0
        for wrong, correct in region_mapping.items():
            if correct is None:
                query = "UPDATE fssp_reestr SET region = NULL WHERE region = ?"
                cursor.execute(query, (wrong,))
            else:
                query = "UPDATE fssp_reestr SET region = ? WHERE region = ?"
                cursor.execute(query, (correct, wrong))

            if cursor.rowcount > 0:
                updates += cursor.rowcount

        conn.commit()
        print(f"  Обновлено регионов: {updates}")

        # 4. Применяем точные маппинги для territory_of_service
        print("\n4. Стандартизация территории обслуживания (точные совпадения)...")
        updates = 0
        for wrong, correct in territory_mapping.items():
            if correct is None:
                query = "UPDATE fssp_reestr SET territory_of_service = NULL WHERE territory_of_service = ?"
                cursor.execute(query, (wrong,))
            else:
                query = "UPDATE fssp_reestr SET territory_of_service = ? WHERE territory_of_service = ?"
                cursor.execute(query, (correct, wrong))

            if cursor.rowcount > 0:
                updates += cursor.rowcount

        conn.commit()
        print(f"  Обновлено территорий обслуживания: {updates}")

        # 5. Заменяем дефисы с пробелами на дефисы без пробелов
        print("\n5. Исправление дефисов...")
        for column in ['city', 'territory_of_service']:
            query = f"""
            UPDATE fssp_reestr 
            SET {column} = REPLACE({column}, ' - ', '-') 
            WHERE {column} LIKE '% - %'
            """
            cursor.execute(query)
            if cursor.rowcount > 0:
                print(f"  Дефисы с пробелами в {column}: {cursor.rowcount} записей")

        conn.commit()

        # 6. Приведение к правильному регистру
        print("\n6. Приведение к правильному регистру...")

        # Для городов (с учетом составных названий через дефис)
        update_city_case = """
        UPDATE fssp_reestr
        SET city = 
            CASE 
                WHEN CHARINDEX('-', city) > 0 THEN
                    UPPER(LEFT(city, 1)) + 
                    LOWER(SUBSTRING(city, 2, CHARINDEX('-', city) - 2)) +
                    '-' +
                    UPPER(SUBSTRING(city, CHARINDEX('-', city) + 1, 1)) +
                    LOWER(SUBSTRING(city, CHARINDEX('-', city) + 2, LEN(city)))
                WHEN CHARINDEX(' ', city) > 0 THEN
                    -- Для названий с пробелами (типа "Верхняя Пышма")
                    UPPER(LEFT(city, 1)) + 
                    LOWER(SUBSTRING(city, 2, CHARINDEX(' ', city) - 1)) +
                    UPPER(SUBSTRING(city, CHARINDEX(' ', city) + 1, 1)) +
                    LOWER(SUBSTRING(city, CHARINDEX(' ', city) + 2, LEN(city)))
                ELSE
                    UPPER(LEFT(city, 1)) + LOWER(SUBSTRING(city, 2, LEN(city)))
            END
        WHERE city IS NOT NULL 
          AND city <> ''
          AND LEN(city) > 0
          AND city COLLATE Cyrillic_General_CS_AS <> UPPER(LEFT(city, 1)) + LOWER(SUBSTRING(city, 2, LEN(city)))
        """
        cursor.execute(update_city_case)
        print(f"  Города: {cursor.rowcount} записей")

        # Для territory_of_service
        update_territory_case = """
        UPDATE fssp_reestr
        SET territory_of_service = 
            CASE 
                WHEN CHARINDEX('-', territory_of_service) > 0 THEN
                    UPPER(LEFT(territory_of_service, 1)) + 
                    LOWER(SUBSTRING(territory_of_service, 2, CHARINDEX('-', territory_of_service) - 2)) +
                    '-' +
                    UPPER(SUBSTRING(territory_of_service, CHARINDEX('-', territory_of_service) + 1, 1)) +
                    LOWER(SUBSTRING(territory_of_service, CHARINDEX('-', territory_of_service) + 2, LEN(territory_of_service)))
                WHEN CHARINDEX(' ', territory_of_service) > 0 THEN
                    UPPER(LEFT(territory_of_service, 1)) + 
                    LOWER(SUBSTRING(territory_of_service, 2, CHARINDEX(' ', territory_of_service) - 1)) +
                    UPPER(SUBSTRING(territory_of_service, CHARINDEX(' ', territory_of_service) + 1, 1)) +
                    LOWER(SUBSTRING(territory_of_service, CHARINDEX(' ', territory_of_service) + 2, LEN(territory_of_service)))
                ELSE
                    UPPER(LEFT(territory_of_service, 1)) + LOWER(SUBSTRING(territory_of_service, 2, LEN(territory_of_service)))
            END
        WHERE territory_of_service IS NOT NULL 
          AND territory_of_service <> ''
          AND LEN(territory_of_service) > 0
          AND territory_of_service COLLATE Cyrillic_General_CS_AS <> UPPER(LEFT(territory_of_service, 1)) + LOWER(SUBSTRING(territory_of_service, 2, LEN(territory_of_service)))
        """
        cursor.execute(update_territory_case)
        print(f"  Территория обслуживания: {cursor.rowcount} записей")

        # Для регионов (только первая буква заглавная)
        update_region_case = """
        UPDATE fssp_reestr
        SET region = UPPER(LEFT(region, 1)) + LOWER(SUBSTRING(region, 2, LEN(region)))
        WHERE region IS NOT NULL 
          AND region <> ''
          AND LEN(region) > 0
          AND region COLLATE Cyrillic_General_CS_AS <> UPPER(LEFT(region, 1)) + LOWER(SUBSTRING(region, 2, LEN(region)))
        """
        cursor.execute(update_region_case)
        print(f"  Регионы: {cursor.rowcount} записей")

        conn.commit()

        # 7. Удаляем лишние пробелы
        print("\n7. Удаление лишних пробелов...")

        trim_queries = [
            ("UPDATE fssp_reestr SET city = LTRIM(RTRIM(city)) WHERE city IS NOT NULL", "Города"),
            ("UPDATE fssp_reestr SET region = LTRIM(RTRIM(region)) WHERE region IS NOT NULL", "Регионы"),
            (
            "UPDATE fssp_reestr SET territory_of_service = LTRIM(RTRIM(territory_of_service)) WHERE territory_of_service IS NOT NULL",
            "Территория обслуживания"),

            ("UPDATE fssp_reestr SET city = REPLACE(REPLACE(city, '  ', ' '), '  ', ' ') WHERE city LIKE '%  %'",
             "Двойные пробелы в городах"),
            ("UPDATE fssp_reestr SET region = REPLACE(REPLACE(region, '  ', ' '), '  ', ' ') WHERE region LIKE '%  %'",
             "Двойные пробелы в регионах"),
            (
            "UPDATE fssp_reestr SET territory_of_service = REPLACE(REPLACE(territory_of_service, '  ', ' '), '  ', ' ') WHERE territory_of_service LIKE '%  %'",
            "Двойные пробелы в территории"),
        ]

        for query, description in trim_queries:
            cursor.execute(query)
            if cursor.rowcount > 0:
                print(f"  {description}: {cursor.rowcount} записей")

        conn.commit()

        print("\n✓ Стандартизация завершена!")


def show_statistics():
    """Показывает статистику после стандартизации"""

    print("\n=== СТАТИСТИКА ПОСЛЕ СТАНДАРТИЗАЦИИ ===")

    with get_db_connection() as conn:
        # Общая статистика
        stats_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT city) as unique_cities,
            COUNT(DISTINCT region) as unique_regions,
            COUNT(DISTINCT territory_of_service) as unique_territories,
            SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) as null_cities,
            SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) as null_regions,
            SUM(CASE WHEN territory_of_service IS NULL THEN 1 ELSE 0 END) as null_territories
        FROM fssp_reestr
        """

        stats = pd.read_sql(stats_query, conn)
        print(f"Всего записей: {stats['total_records'].iloc[0]}")
        print(f"Уникальных городов: {stats['unique_cities'].iloc[0]}")
        print(f"Уникальных регионов: {stats['unique_regions'].iloc[0]}")
        print(f"Уникальных территорий: {stats['unique_territories'].iloc[0]}")
        print(f"NULL в городах: {stats['null_cities'].iloc[0]}")
        print(f"NULL в регионах: {stats['null_regions'].iloc[0]}")
        print(f"NULL в территориях: {stats['null_territories'].iloc[0]}")

        # Топ-10 городов
        top_cities_query = """
        SELECT TOP 10 
            city,
            COUNT(*) as count
        FROM fssp_reestr
        WHERE city IS NOT NULL
        GROUP BY city
        ORDER BY count DESC
        """

        top_cities = pd.read_sql(top_cities_query, conn)
        print("\nТоп-10 городов:")
        for _, row in top_cities.iterrows():
            print(f"  {row['city']}: {row['count']}")

        # Топ-10 регионов
        top_regions_query = """
        SELECT TOP 10 
            region,
            COUNT(*) as count
        FROM fssp_reestr
        WHERE region IS NOT NULL
        GROUP BY region
        ORDER BY count DESC
        """

        top_regions = pd.read_sql(top_regions_query, conn)
        print("\nТоп-10 регионов:")
        for _, row in top_regions.iterrows():
            print(f"  {row['region']}: {row['count']}")

        # Топ-10 территорий
        top_territories_query = """
        SELECT TOP 10 
            territory_of_service,
            COUNT(*) as count
        FROM fssp_reestr
        WHERE territory_of_service IS NOT NULL
        GROUP BY territory_of_service
        ORDER BY count DESC
        """

        top_territories = pd.read_sql(top_territories_query, conn)
        print("\nТоп-10 территорий обслуживания:")
        for _, row in top_territories.iterrows():
            print(f"  {row['territory_of_service']}: {row['count']}")


def analyze_problems():
    """Анализирует оставшиеся проблемы в данных"""

    print("\n=== АНАЛИЗ ПРОБЛЕМ ===")

    with get_db_connection() as conn:
        # Города с префиксами
        city_problems_query = """
        SELECT DISTINCT city
        FROM fssp_reestr
        WHERE city IS NOT NULL
          AND (city LIKE 'г.%' 
               OR city LIKE 'гор.%' 
               OR city LIKE 'пгт.%'
               OR city LIKE 'с.%'
               OR city LIKE 'п.%'
               OR city LIKE 'поселок%'
               OR city LIKE 'а.%'
               OR city LIKE 'ст.%'
               OR city LIKE 'c.%'
               OR city LIKE 'Ст.%')
        ORDER BY city
        """

        city_problems = pd.read_sql(city_problems_query, conn)
        if len(city_problems) > 0:
            print(f"\nГорода с префиксами ({len(city_problems)}):")
            for _, row in city_problems.head(20).iterrows():
                print(f"  {row['city']}")

        # Регионы с проблемами
        region_problems_query = """
        SELECT DISTINCT region
        FROM fssp_reestr
        WHERE region IS NOT NULL
          AND (region LIKE '%  %'
               OR region LIKE '% - %'
               OR region LIKE '%г.%'
               OR region COLLATE Cyrillic_General_CS_AS <> UPPER(LEFT(region, 1)) + LOWER(SUBSTRING(region, 2, LEN(region)))
          )
        ORDER BY region
        """

        region_problems = pd.read_sql(region_problems_query, conn)
        if len(region_problems) > 0:
            print(f"\nРегионы с проблемами ({len(region_problems)}):")
            for _, row in region_problems.head(20).iterrows():
                print(f"  {row['region']}")

        # Территории с проблемами
        territory_problems_query = """
        SELECT DISTINCT territory_of_service
        FROM fssp_reestr
        WHERE territory_of_service IS NOT NULL
          AND (territory_of_service LIKE '%  %'
               OR territory_of_service LIKE '% - %'
               OR territory_of_service LIKE 'г.%'
               OR territory_of_service LIKE '%г.%'
               OR territory_of_service LIKE '% и %г.%'
               OR territory_of_service COLLATE Cyrillic_General_CS_AS <> UPPER(LEFT(territory_of_service, 1)) + LOWER(SUBSTRING(territory_of_service, 2, LEN(territory_of_service)))
          )
        ORDER BY territory_of_service
        """

        territory_problems = pd.read_sql(territory_problems_query, conn)
        if len(territory_problems) > 0:
            print(f"\nТерритории обслуживания с проблемами ({len(territory_problems)}):")
            for _, row in territory_problems.head(20).iterrows():
                print(f"  {row['territory_of_service']}")


def main():
    """Основная функция"""

    print("=" * 70)
    print("СТАНДАРТИЗАЦИЯ ДАННЫХ FSSP_REESTR")
    print("=" * 70)

    try:
        # Проверка подключения
        print("Проверка подключения к БД...")
        with get_db_connection() as conn:
            print("✓ Подключение успешно!")

        while True:
            print("\n" + "=" * 50)
            print("МЕНЮ")
            print("=" * 50)
            print("1. Применить стандартизацию ко всем колонкам")
            print("2. Показать статистику")
            print("3. Анализировать оставшиеся проблемы")
            print("4. Выйти")

            choice = input("\nВыберите действие (1-4): ").strip()

            if choice == '1':
                confirm = input("\nВы уверены, что хотите выполнить стандартизацию? (y/n): ")
                if confirm.lower() == 'y':
                    apply_standardization()
                    show_statistics()

            elif choice == '2':
                show_statistics()

            elif choice == '3':
                analyze_problems()

            elif choice == '4':
                print("Выход из программы.")
                break

            else:
                print("Неверный выбор. Попробуйте снова.")

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
