#!/usr/bin/env python3

import sys
import logging
import argparse

import db
import www


def main():

    # Определить аргументы командой строки
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="для SQLite - файл базы данных; "
                        "для PostgreSQL - файл с конфигурацией")
    parser.add_argument("-d", "--database", default="sqlite", choices=["sqlite", "postgres"],
        help="тип базы данных (sqlite или postgres)")
    parser.add_argument("-e", "--export", help="CSV-файл для экспорта базы данных")
    parser.add_argument("-l", "--log", help="путь к лог-файлу")
    parser.add_argument("-f", "--forever", type=bool, default=False,
        help="перезапускать парсер бесконечно до прерывания")
    args = parser.parse_args()

    # Настроить логирование
    logging.basicConfig(level=logging.INFO, filename=args.log,
        format='[%(asctime)s] %(levelname)s: %(message)s')


    # Открыть базу данных
    if args.database == "sqlite":
        try:
            dao = db.SQLiteDAO(args.file)
        except db.OpenError as exc:
            logging.fatal("Не удалось открыть файл БД {}: {}".format(args.file, str(exc)))
            sys.exit(1)
    else:
        try:
            dao = db.PostgresDAO(args.file)
        except db.ConfigError as exc:
            logging.fatal("Ошибка файла конфигурации {}: {}"
                          .format(args.file, str(exc)))
            sys.exit(2)
        except db.OpenError as exc:
            logging.fatal("Не удалось открыть соединение с БД: {}".format(str(exc)))
            sys.exit(3)

    # Если надо просто экспортировать данные в CSV, то делаем это и выходим
    if args.export:
        try:
            dao.exportToCsv(args.export)
        except IOError as exc:
            logging.fatal("Не удалось открыть для записи файл {}: {}".format(args.export, exc.strerror))
            sys.exit(4)
        else:
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
            cluster = db_clusters[0] if db_clusters else cluster
            for news in news_list:
                news["cluster"] = cluster

            # Составить список новостей, которые уже есть в БД
            # с этой меткой кластера
            db_texts = set(news["text"] for news in dao.getNewsByCluster(cluster))

            # Добавить все новости, которых в БД нет
            to_add = [news for news in news_list if news["text"] not in db_texts]
            if to_add:
                dao.addNews(to_add)

        first_time = False

    # Ну вот и все
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Прервано пользователем.")
        sys.exit(0)
