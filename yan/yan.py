#!/usr/bin/env python3

import sys
import logging
import agrapse

import db
import www


def main():

    # Определить аргументы командой строки
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="SQLite file to write news into")
    args = parser.parse_args()

    # Настроить логирование
    logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')

    # Открыть базу данных
    try:
        dao = db.SQLiteDAO(args.file)
    except db.OpenError as exc:
        logging.fatal("Не удалось открыть файл БД {}: {}".format(args.file, str(exc)))
        sys.exit(1)

    # Выкачать новости
    news = www.YandexNews()
    for cluster, news_list in news.clusters():

        # Проверим, не присутствует ли какая-то новость из имеющихся в базе данных.
        # Если присутствует, обновим метку кластера.
        cluster = dao.getClusterByNewsTexts([news["text"] for news in news_list])[0] or cluster

        # Составить список новостей, которые уже есть в БД
        # с этой меткой кластера
        db_texts = set(news["text"] for news in dao.getNewsByCluster(cluster))

        # Добавить все новости, которых в БД нет
        dao.addNews([news for news in news_list if news["text"] not in db_texts])

    # Ну вот и все
    sys.exit(0)


if __name__ == "__main__":
    main()
