from datetime import datetime
from json import loads
from os.path import join, relpath
from os import walk


def find_files(target_path, pattern):
    """
    Рекурсивный проход по всем вложенным директориям и поиск целевых файлов

    :param target_path: str: содержит путь до целевой директории
    :param pattern: str: содержит расширение для целевых файлов
    :return:
    """
    for path, dirname, files in walk(target_path):
        for name in files:
            if name.endswith(pattern):
                yield join(path, name)


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


def validate(row, days):
    """
    Формирование ключа по дате и отчета о валидации данных

    :param row: dict
    :param days: dict
    :return: dict
    """
    search = set()
    date = f"{datetime.fromtimestamp(row['timestamp']):%Y-%m-%d}"

    if date not in days:
        days[date] = datetime.strptime(date, '%Y-%m-%d').timestamp()

    for val in row['query_string'].split('&'):
        if val.find('id=') >= 0:
            try:
                search.add(int(val[3:]))
            except ValueError:
                continue

    return days[date], ('valid' if set(row['ids']) == search else 'non_valid'), row['event_type']


def start(upload_dir, pattern):
    """
    Обработка файлов в целевой директории

    :param upload_dir: str: целевая директория
    :param pattern: str: расширение целевых файлов
    :return: dict
    """
    res = {"valid": {}, "non_valid": {}}
    days = {}

    for line in cat(opener(find_files(upload_dir, pattern))):
        date, valid, action = validate(loads(line), days)
        if date not in res[valid]:
            res[valid][date] = {
                "create": 0,
                "update": 0,
                "delete": 0,
            }
        res[valid][date][action] += 1

    return res


if __name__ == '__main__':
    result = start(relpath('../test_files/'), '.log')
    print(result)
