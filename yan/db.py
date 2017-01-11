
import csv
import logging
import itertools

import yaml
from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, DateTime, Sequence

from sqlalchemy.sql import select


class OpenError(Exception): pass

class ConfigError(Exception): pass


class AbstractDAO:

    def __init__(self, engine_string):
        self._engine = create_engine(engine_string)
        self._metadata = MetaData()
        self._news = Table('news', self._metadata,
            Column('id', Integer, Sequence('news_id_seq'), primary_key=True),
            Column('title', String(100), nullable=False),
            Column('text', String(500), nullable=False, index=True, unique=True),
            Column('topic', String(30)),
            Column('cluster', String(100), nullable=False, index=True),
            Column('datetime', DateTime),
            Column('publisher', String(30)),
        )

        try:
            self._metadata.create_all(self._engine)
        except sqlalchemy.exc.OperationalError as exc:
            raise OpenError(str(exc))

        self._conn = self._engine.connect()

    def addNews(self, news_list):
        news_list = self._preprocessNews(news_list)
        self._conn.execute(self._news.insert(), news_list)

    def _preprocessNews(self, news_list):
        for news in news_list:
            # Проверить, что строковые значения не слишком длинные
            for column in self._news.columns:
                if isinstance(column, String):
                    name = column.name
                    if len(news.get(name, "")) > column.type.length:
                        logging.waning("обрезано значения для столбца '{}' (с {} до {})"
                            .format(name, len(news[column]), column.type.length))
                        news[column] = news[column][:column.type.length]
        return news_list

    def getClustersByNewsTexts(self, texts):
        s = select([self._news.c.cluster]).where(self._news.c.text.in_(texts))
        result = self._conn.execute(s)
        return [r[0] for r in result]

    def getNewsByCluster(self, cluster):
        s = select([self._news]).where(self._news.c.cluster == cluster)
        result = self._conn.execute(s)
        return [dict(row) for row in result]

    def exportToCsv(self, csvpath):
        rows = [dict(r) for r in self._conn.execute(select([self._news]))]
        with open(csvpath, "w", encoding="utf-8") as f:
            writer = csv.DictWriter(f, [col.name for col in self._news.columns])
            writer.writeheader()
            writer.writerows(rows)

    def populateFrom(self, other):
        for i in itertools.count(step=10000):
            news = other._conn.execute(select([other._news]).limit(10000).offset(i))
            if not news:
                return
            for n in news:
                news.pop("id", None)
            to_insert = [n for n in news if not self._newsExists(n["text"])]
            self._conn.execute(self._news.insert(), to_insert)

    def _newsExists(self, text):
        s = select([self._news.c.id]) \
                .where(self._news.c.text == news["text"]).limit(1)
        return bool(list(self._conn.execute(s)))

    def close(self):
        self._conn.close()


class SQLiteDAO(AbstractDAO):
    def __init__(self, filepath):
        super().__init__('sqlite:///' + filepath)


class PostgresDAO(AbstractDAO):

    DEFAULT_HOST = "127.0.0.1"
    DEFAULT_PORT = 5432

    def __init__(self, configpath):
        try:
            with open(configpath, "r") as f:
                config = yaml.load(f)
        except IOError as exc:
            raise ConfigError(exc.strerror)
        except yaml.YAMLError as exc:
            raise ConfigError("ошибка YAML при парсинге: {}".format(exc))

        if not isinstance(config, dict):
            raise ConfigError("файл с конфигурацией должен быть словарем")

        missing_keys = set(["user", "password", "database"]) - set(config.keys)
        if missing_keys:
            raise ConfigError("не найдены ключи: {}".format(", ".join(missing_keys)))

        engine_string = 'postgresql://{}:{}@{}:{}/{}'.format(config["user"],
            config["password"], config.get("host", self.DEFAULT_HOST),
            config.get("port", self.DEFAULT_PORT), config["database"])

        super().__init__(engine_string)
