#!/usr/bin/env python3

import sys
import logging
import argparse
import traceback

from . import db
from . import www


DEFAULT_POSTGRES_FILE = "/etc/yan.yml"


def open_sqlite(filepath):
    try:
        dao = db.SQLiteDAO(filepath)
    except db.OpenError as exc:
        logging.fatal("Не удалось открыть файл БД {}: {}".format(filepath, str(exc)))
        sys.exit(3)
    else:
        return dao


def open_postgres(configfile):
    try:
        dao = db.PostgresDAO(configfile)
    except db.ConfigError as exc:
        logging.fatal("Ошибка файла конфигурации {}: {}"
                      .format(configfile, str(exc)))
        sys.exit(2)
    except db.OpenError as exc:
        logging.fatal("Не удалось открыть соединение с БД: {}".format(str(exc)))
        sys.exit(3)
    else:
        return dao


def main():

    # Определить аргументы командой строки
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--postgres", nargs="?", const=DEFAULT_POSTGRES_FILE,
        help="использовать PostgreSQL c указанным файлом конфигурации"
             "(по умолчанию /etc/yan.yml)")
    parser.add_argument("-s", "--sqlite",
        help="использовать указанный файл SQLite")
    parser.add_argument("-e", "--export", help="CSV-файл для экспорта базы данных")
    parser.add_argument("-m", "--migrate", nargs="?", const=True,
        help="перенести данные из одной БД в другую")
    parser.add_argument("-l", "--log", help="путь к лог-файлу")
    parser.add_argument("-f", "--forever", action="store_true",
        help="перезапускать парсер бесконечно до прерывания")
    args = parser.parse_args()

    # Настроить логирование
    logging.basicConfig(level=logging.INFO, filename=args.log,
        format='[%(asctime)s] %(levelname)s: %(message)s')

    # Проверить непротиворечивость аргументов командной строки
    if args.postgres and args.sqlite:
        logging.fatal("конфликтующие аргументы: --postgres и --slqite")
        sys.exit(1)
    if not args.postgres and not args.sqlite:
        args.postgres = DEFAULT_POSTGRES_FILE
    if args.postgres and args.migrate is True:
        logging.fatal("для миграции данных в SQLite необходимо указать путь к файлу (в аргументе --migrate)")
        sys.exit(2)
    if args.migrate is True:
        args.migrate = DEFAULT_POSTGRES_FILE

    # Открыть базу данных
    if args.sqlite:
        dao = open_sqlite(args.sqlite)
    else:
        dao = open_postgres(args.postgres)

    # Экспортировать данные в CSV
    if args.export:
        try:
            dao.exportToCsv(args.export)
        except IOError as exc:
            logging.fatal("Не удалось открыть для записи файл {}: {}".format(args.export, exc.strerror))
            sys.exit(4)

    # Перенести данные из одной БД в другую
    if args.migrate:
        if args.postgres:
            target_dao = open_sqlite(args.migrate)
        else:
            target_dao = open_postgres(args.migrate)
        target_dao.populateFrom(dao)
        target_dao.close()

    # Если были даны флаги -e или -m, то на этом заканчиваем
    if args.migrate or args.export:
        sys.exit(0)

    # Выкачать новости
    yanews = www.YandexNews()
    first_time = True

    # Если args.forever == True, то крутимся вечно.
    # Иначе делаем одну итерацию цикла и выходим.
    while args.forever or first_time:

        # Пройти по всем кластерам новостей
        for cluster, news_list in yanews.clusters():

            # Проверим, не присутствует ли какая-то новость из имеющихся в базе данных.
            # Если присутствует, обновим метку кластера.
            db_clusters = dao.getClustersByNewsTexts([news["text"] for news in news_list])
            if db_clusters:
                dao.renameClusters(db_clusters, cluster)

            # Сделать список новостей уникальным (на всякий случай)
            news_list = list({news["text"]: news for news in news_list}.values())

            # Составить список новостей, которых еще нет в БД
            to_insert = [news for news in news_list if not dao.newsTextExists(news["text"])]

            # Добавить все новости, которых в БД нет
            if to_insert:
                dao.addNews(to_insert)

        first_time = False

    # Ну вот и все
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Прервано пользователем.")
        sys.exit(5)
    except Exception:
        logging.fatal(traceback.format_exc())
        sys.exit(6)
