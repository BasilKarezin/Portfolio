import os
import zipfile
import shutil
from pathlib import Path
import re
import pandas as pd
import tempfile
import PyPDF2
from datetime import datetime
import time
import gc


def extract_contract_info(filename):
    """Извлекает номер контракта из имени файла"""
    match = re.match(r'(\d+-\d+-\d+)', filename)
    if match:
        return match.group(1)
    return None


def load_contracts(excel_path):
    """Загружает соответствие contract_number -> contract_id из Excel"""
    if not os.path.exists(excel_path):
        print(f"⚠ Файл с соответствиями не найден: {excel_path}")
        return {}

    df = pd.read_excel(excel_path)
    contracts = {}
    for _, row in df.iterrows():
        contracts[str(row['contract_number'])] = str(row['contract_id'])
    print(f"✅ Загружено {len(contracts)} соответствий")
    return contracts


def get_document_category(filename):
    """Определяет категорию документа по точным фразам"""
    filename_lower = filename.lower()

    # Расчет
    if 'расчет задолженности' in filename_lower:
        return 'Расчет'

    # Уведомление
    if 'уведомление об уступке прав' in filename_lower:
        return 'Уведомление об уступке цедента'

    # Заявление-согласие (Иное)
    if ('заявление-согласие' in filename_lower) or ('заявление согласие' in filename_lower):
        return 'Иное'

    # Согласие АСП (Досье для суда)
    if ('согласие асп' in filename_lower) or ('согласие_асп' in filename_lower):
        return 'Досье для суда'

    # Заявление-анкета (Досье для суда)
    if ('заявление-анкета' in filename_lower) or ('заявление анкета' in filename_lower):
        return 'Досье для суда'

    # Договор займа (Досье для суда)
    if 'договор займа' in filename_lower:
        return 'Досье для суда'

    # Справка выдача (Досье для суда)
    if 'справка выдача' in filename_lower:
        return 'Досье для суда'

    # Платежное поручение (Досье для суда)
    if ('платежное поручение' in filename_lower) or ('платежное_поручение' in filename_lower):
        return 'Досье для суда'

    # Скан паспорта
    if ('скан паспорта' in filename_lower) or ('паспорт' in filename_lower):
        return 'Скан паспорта'

    # Заявление страховки (Иное)
    if 'заявление страховки' in filename_lower:
        return 'Иное'

    # Договор-оферта страхования (Иное)
    if ('договор-оферта страхования' in filename_lower) or ('договор оферта страхования' in filename_lower):
        return 'Иное'

    return 'Прочее'


def get_document_order(filename, category):
    """Определяет порядок документа в категории"""
    filename_lower = filename.lower()

    if category == 'Досье для суда':
        if ('заявление-анкета' in filename_lower) or ('заявление анкета' in filename_lower):
            return 1
        elif 'договор займа' in filename_lower:
            return 2
        elif ('согласие асп' in filename_lower) or ('согласие_асп' in filename_lower):
            return 3
        elif 'справка выдача' in filename_lower:
            return 4
        elif ('платежное поручение' in filename_lower) or ('платежное_поручение' in filename_lower):
            return 5
        elif ('скан паспорта' in filename_lower) or ('паспорт' in filename_lower):
            return 6
        else:
            return 99

    elif category == 'Иное':
        if ('заявление-согласие' in filename_lower) or ('заявление согласие' in filename_lower):
            return 1
        elif 'заявление страховки' in filename_lower:
            return 2
        elif ('договор-оферта страхования' in filename_lower) or ('договор оферта страхования' in filename_lower):
            return 3
        else:
            return 99

    elif category == 'Расчет':
        return 1

    elif category == 'Уведомление об уступке цедента':
        return 1

    elif category == 'Скан паспорта':
        return 1

    return 999


def excel_to_pdf_safe(excel_path, output_pdf_path, max_retries=3):
    """
    Безопасная конвертация Excel в PDF с обработкой ошибок
    """
    for attempt in range(max_retries):
        try:
            print(f"      Конвертация Excel в PDF: {os.path.basename(excel_path)} (попытка {attempt + 1})")

            import win32com.client as win32
            import pythoncom

            pythoncom.CoInitialize()
            excel = win32.gencache.EnsureDispatch('Excel.Application')
            excel.Visible = False
            excel.DisplayAlerts = False
            excel.ScreenUpdating = False

            wb = excel.Workbooks.Open(os.path.abspath(excel_path))

            for ws in wb.Worksheets:
                try:
                    # Автоподбор ширины колонок
                    ws.Columns.AutoFit()

                    # Настройка страницы
                    ws.PageSetup.Zoom = False
                    ws.PageSetup.FitToPagesWide = 1
                    ws.PageSetup.FitToPagesTall = False
                    ws.PageSetup.Orientation = 2

                    # Уменьшаем поля
                    ws.PageSetup.LeftMargin = excel.CentimetersToPoints(0.5)
                    ws.PageSetup.RightMargin = excel.CentimetersToPoints(0.5)

                except:
                    pass

            # Сохраняем и конвертируем
            temp_excel = os.path.join(os.path.dirname(excel_path), f"temp_fixed_{os.path.basename(excel_path)}")
            wb.SaveAs(temp_excel, FileFormat=51)
            wb.Close()

            wb_temp = excel.Workbooks.Open(os.path.abspath(temp_excel))
            wb_temp.ExportAsFixedFormat(0, os.path.abspath(output_pdf_path))
            wb_temp.Close()

            excel.Quit()
            pythoncom.CoUninitialize()

            # Даем время на закрытие
            time.sleep(1)
            gc.collect()

            # Удаляем временный файл
            if os.path.exists(temp_excel):
                try:
                    os.remove(temp_excel)
                except:
                    pass

            if os.path.exists(output_pdf_path):
                size = os.path.getsize(output_pdf_path)
                print(f"      ✅ PDF создан ({size} байт)")
                return True

        except Exception as e:
            print(f"      ⚠ Ошибка (попытка {attempt + 1}): {e}")
            time.sleep(2)
            continue

    print(f"      ❌ Не удалось сконвертировать после {max_retries} попыток")
    return False


def merge_pdfs_safe(pdf_list, output_path, category_name):
    """
    Безопасное объединение PDF
    """
    if not pdf_list:
        print(f"      ⚠ Нет файлов для '{category_name}'")
        return False

    try:
        print(f"      Объединение {len(pdf_list)} PDF для '{category_name}'")

        valid_pdfs = []
        for pdf_file in pdf_list:
            if os.path.exists(pdf_file):
                size = os.path.getsize(pdf_file)
                if size > 1000:  # Минимум 1KB
                    valid_pdfs.append(pdf_file)
                    print(f"        + {os.path.basename(pdf_file)} ({size} байт)")
                else:
                    print(f"        ⚠ {os.path.basename(pdf_file)} - слишком маленький")
            else:
                print(f"        ⚠ {os.path.basename(pdf_file)} - файл не найден")

        if not valid_pdfs:
            print(f"      ⚠ Нет валидных PDF")
            return False

        merger = PyPDF2.PdfMerger()
        total_pages = 0

        for pdf_file in valid_pdfs:
            try:
                merger.append(pdf_file)
                with open(pdf_file, 'rb') as f:
                    pdf_reader = PyPDF2.PdfReader(f)
                    pages = len(pdf_reader.pages)
                    total_pages += pages
                    print(f"        📄 {os.path.basename(pdf_file)}: {pages} стр.")
            except Exception as e:
                print(f"        ⚠ Ошибка при добавлении: {e}")

        if total_pages > 0:
            merger.write(output_path)
            merger.close()
            print(f"      ✅ СОЗДАН {os.path.basename(output_path)} ({total_pages} стр.)")
            return True
        else:
            print(f"      ⚠ Нет страниц для сохранения")
            return False

    except Exception as e:
        print(f"      ⚠ Ошибка объединения: {e}")
        return False


def find_all_files(directory):
    """Рекурсивно находит все файлы, игнорируя временные"""
    all_files = []
    for root, dirs, files in os.walk(directory):
        # Пропускаем временные файлы Excel
        files = [f for f in files if not f.startswith('~$')]
        for file in files:
            file_path = os.path.join(root, file)
            all_files.append(file_path)
    return all_files


def select_output_structure():
    """
    Интерактивный выбор структуры вывода
    """
    print("\n" + "=" * 60)
    print("ВЫБОР СТРУКТУРЫ ВЫВОДА")
    print("=" * 60)

    print("\nДоступные категории документов:")
    categories = [
        '1. Досье для суда',
        '2. Иное',
        '3. Расчет',
        '4. Уведомление об уступке цедента',
        '5. Скан паспорта'
    ]

    for cat in categories:
        print(f"   {cat}")

    print("\nВыберите категории для обработки (введите номера через пробел):")
    print("   Например: 1 3 4 5")
    choice = input("> ").strip()

    category_map = {
        '1': 'Досье для суда',
        '2': 'Иное',
        '3': 'Расчет',
        '4': 'Уведомление об уступке цедента',
        '5': 'Скан паспорта'
    }

    selected = []
    if choice:
        for num in choice.split():
            if num in category_map:
                selected.append(category_map[num])
    else:
        selected = list(category_map.values())

    print(f"\n✅ Выбраны: {', '.join(selected)}")

    # Выбор структуры
    print("\n" + "=" * 60)
    print("ВЫБОР СТРУКТУРЫ ПАПОК")
    print("=" * 60)
    print("1. Одна папка с файлами contract_id_категория.pdf")
    print("2. Папки по категориям, внутри contract_id.pdf")
    print("3. Папки по contract_id, внутри файлы по категориям")

    structure_choice = input("\nВыберите структуру (1-3): ").strip()

    structure_map = {
        '1': 'flat',
        '2': 'by_category',
        '3': 'by_contract'
    }

    structure = structure_map.get(structure_choice, 'by_contract')

    # Включение скана паспорта в Досье для суда
    print("\n" + "=" * 60)
    include_passport = input("Включить скан паспорта в Досье для суда? (д/н): ").strip().lower()
    merge_passport = include_passport in ['д', 'да', 'y', 'yes']

    if merge_passport and 'Скан паспорта' in selected:
        selected.remove('Скан паспорта')
        print("✅ Скан паспорта будет добавлен в Досье для суда")

    return selected, structure, merge_passport


def process_archives(source_dir, output_dir, contracts_file):
    """Обрабатывает ZIP-архивы с выбранной структурой"""

    # Загружаем соответствия
    contracts = load_contracts(contracts_file)

    # Выбираем настройки
    selected_categories, structure, merge_passport = select_output_structure()

    print(f"\n🔍 Поиск ZIP-архивов в: {source_dir}")
    zip_files = list(Path(source_dir).glob("*.zip"))
    print(f"📦 Найдено архивов: {len(zip_files)}")

    processed = 0
    failed = 0
    skipped = 0

    # Создаем структуру папок
    if structure == 'flat':
        flat_dir = os.path.join(output_dir, "все_в_одной")
        os.makedirs(flat_dir, exist_ok=True)
    elif structure == 'by_category':
        categories_dir = os.path.join(output_dir, "по_категориям")
        os.makedirs(categories_dir, exist_ok=True)
    else:  # by_contract
        contracts_dir = os.path.join(output_dir, "по_контрактам")
        os.makedirs(contracts_dir, exist_ok=True)

    for idx, zip_path in enumerate(zip_files, 1):
        zip_filename = zip_path.name
        contract_number = extract_contract_info(zip_filename)

        if not contract_number:
            print(f"\n[{idx}/{len(zip_files)}] ⚠ Не определен номер: {zip_filename}")
            skipped += 1
            continue

        contract_id = contracts.get(contract_number, contract_number)

        print(f"\n{'=' * 60}")
        print(f"[{idx}/{len(zip_files)}] 📁 {contract_number} -> {contract_id}")

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Распаковка
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)

                # Поиск файлов
                all_files = find_all_files(temp_dir)

                # Группировка по категориям
                category_files = {}

                for file_path in all_files:
                    file_name = os.path.basename(file_path)
                    file_ext = file_name.lower().split('.')[-1] if '.' in file_name else ''

                    if file_ext not in ['pdf', 'xlsx', 'xls']:
                        continue

                    category = get_document_category(file_name)

                    # Обработка скана паспорта
                    if category == 'Скан паспорта' and merge_passport:
                        category = 'Досье для суда'

                    if category not in selected_categories:
                        continue

                    order = get_document_order(file_name, category)

                    if category not in category_files:
                        category_files[category] = []

                    if file_ext == 'pdf':
                        category_files[category].append({
                            'path': file_path,
                            'order': order,
                            'name': file_name
                        })
                    else:
                        # Конвертируем Excel
                        pdf_filename = file_name.rsplit('.', 1)[0] + '.pdf'
                        pdf_path = os.path.join(temp_dir, pdf_filename)

                        if excel_to_pdf_safe(file_path, pdf_path):
                            category_files[category].append({
                                'path': pdf_path,
                                'order': order,
                                'name': pdf_filename
                            })

                # Создаем PDF для каждой категории
                for category, files in category_files.items():
                    if not files:
                        continue

                    # Сортируем по порядку
                    files.sort(key=lambda x: x['order'])
                    pdf_paths = [f['path'] for f in files]

                    # Определяем имя выходного файла
                    if structure == 'flat':
                        output_filename = f"{contract_id}_{category}.pdf"
                        output_path = os.path.join(flat_dir, output_filename)
                    elif structure == 'by_category':
                        category_dir = os.path.join(categories_dir, category)
                        os.makedirs(category_dir, exist_ok=True)
                        output_path = os.path.join(category_dir, f"{contract_id}.pdf")
                    else:  # by_contract
                        contract_dir = os.path.join(contracts_dir, contract_id)
                        os.makedirs(contract_dir, exist_ok=True)

                        if category == 'Досье для суда':
                            output_filename = f"{contract_id}.pdf"
                        else:
                            output_filename = f"{contract_id}_{category}.pdf"
                        output_path = os.path.join(contract_dir, output_filename)

                    merge_pdfs_safe(pdf_paths, output_path, category)

                processed += 1
                print(f"\n  ✅ Контракт {contract_id} обработан")

            except Exception as e:
                print(f"  ❌ Ошибка: {e}")
                failed += 1

            # Принудительная сборка мусора
            gc.collect()

    # Итоги
    print(f"\n{'=' * 60}")
    print("ИТОГИ ОБРАБОТКИ")
    print(f"{'=' * 60}")
    print(f"✅ Успешно: {processed}")
    print(f"❌ Ошибок: {failed}")
    print(f"⚠ Пропущено: {skipped}")
    print(f"\n📁 Результаты сохранены в: {output_dir}")

    # Показываем структуру
    if structure == 'flat':
        print(f"\nСтруктура: все_в_одной/")
        print(f"  - {contract_id}_Категория.pdf")
    elif structure == 'by_category':
        print(f"\nСтруктура: по_категориям/")
        print(f"  ├── Досье для суда/")
        print(f"  │   └── {contract_id}.pdf")
        print(f"  ├── Расчет/")
        print(f"  │   └── {contract_id}.pdf")
        print(f"  └── ...")
    else:
        print(f"\nСтруктура: по_контрактам/")
        print(f"  ├── {contract_id}/")
        print(f"  │   ├── {contract_id}.pdf (Досье для суда)")
        print(f"  │   ├── {contract_id}_Расчет.pdf")
        print(f"  │   └── ...")
        print(f"  └── ...")


def main():
    # Пути
    source_dir = r"... "
    contracts_file = r"... "

    # Новая папка с датой
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(source_dir, f"ГОТОВО_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("ФИНАЛЬНАЯ ОБРАБОТКА ДОКУМЕНТОВ")
    print("=" * 60)
    print(f"📂 Исходная папка: {source_dir}")
    print(f"📂 Результат: {output_dir}")
    print("=" * 60)

    process_archives(source_dir, output_dir, contracts_file)

    print(f"\n{'=' * 60}")
    print("✅ РАБОТА ЗАВЕРШЕНА")
    print(f"{'=' * 60}")
    input("\nНажмите Enter для выхода...")


if __name__ == "__main__":
    # Проверка зависимостей
    required_packages = ['PyPDF2', 'pandas', 'openpyxl', 'pywin32']
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            print(f"Устанавливаем {package}...")
            os.system(f"pip install {package}")

    main()
