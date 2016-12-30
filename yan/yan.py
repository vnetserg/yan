#!/usr/bin/env python3

import sys
import logging
import argparse

import db
import www


def main():

    # Определить аргументы командой строки
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="SQLite file to write news into")
    parser.add_argument("-e", "--export", help="CSV file to export database")
    args = parser.parse_args()

    # Настроить логирование
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Открыть базу данных
    try:
        dao = db.SQLiteDAO(args.file)
    except db.OpenError as exc:
        logging.fatal("Не удалось открыть файл БД {}: {}".format(args.file, str(exc)))
        sys.exit(1)

    # Если надо просто экспортировать данные в CSV, то делаем это и выходим
    if args.export:
        try:
            dao.exportToCsv(args.export)
        except IOError as exc:
            logging.fatal("Не удалось открыть для записи файл {}: {}".format(args.export, exc.strerror))
            sys.exit(2)
        else:
            sys.exit(0)

    # Выкачать новости
    yanews = www.YandexNews()
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

    # Ну вот и все
    sys.exit(0)


if __name__ == "__main__":
    main()
