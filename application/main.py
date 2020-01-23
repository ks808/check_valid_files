import os
from datetime import datetime
from json import loads

DEBUG = os.environ.get('APP_KEY_DEBUG', 1)

# Указать путь для загрузки файлов
UPLOAD_DIR = os.path.relpath('../test_files/') if DEBUG else os.environ.get('APP_UPLOAD_FILES_DIR')


def find_files(top_dir, pattern):
    """
    Рекурсивный проход по всем вложенным директориям и поиск целевых файлов

    :param top_dir: str: содержит путь до целевой директории
    :param pattern: str: содержит расширение для целевых файлов
    :return:
    """
    for path, dirname, files_list in os.walk(top_dir):
        for name in files_list:
            if name.endswith(pattern):
                yield os.path.join(path, name)


def opener(filenames):
    """
    Открытие файла на чтение

    :param filenames: сопрограмма
    :return:
    """
    for name in filenames:
        with open(name) as f:
            yield f


def cat(file_list):
    """
    Чтение файла по строкам

    :param file_list: сопрограмма
    :return:
    """
    for f in file_list:
        for line in f:
            yield line


def validate(record, days):
    """
    Формирование ключа по дате и отчета о валидации данных

    :param record: dict
    :param days: dict
    :return: dict
    """
    find = set()
    date = f"{datetime.fromtimestamp(record['timestamp']):%Y-%m-%d}"

    if date not in days:
        days[date] = datetime.strptime(date, '%Y-%m-%d').timestamp()

    for val in record['query_string'].split('&'):
        if val.find('id=') >= 0:
            try:
                find.add(int(val[3:]))
            except ValueError:
                continue

    valid = 'valid' if set(record['ids']) == find else 'non_valid'

    return {'check': valid, 'day': days[date], 'event_type': record['event_type']}


def start():
    result = {
        "valid": {},
        "non_valid": {},
    }
    days = {}

    for line in cat(opener(find_files(UPLOAD_DIR, '.log'))):
        rec = validate(loads(line), days)
        if rec['day'] not in result[rec['check']]:
            result[rec['check']][rec['day']] = {
                "create": 0,
                "update": 0,
                "delete": 0,
            }
        result[rec['check']][rec['day']][rec['event_type']] += 1

    return result


if __name__ == '__main__':
    print(start())
