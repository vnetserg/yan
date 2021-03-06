
import csv
import logging
import itertools

import yaml
from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, DateTime, Sequence

from sqlalchemy.sql import select
from sqlalchemy.exc import OperationalError


class OpenError(Exception): pass

class ConfigError(Exception): pass


class AbstractDAO:

    def __init__(self, engine_string):
        self._engine = create_engine(engine_string)
        self._metadata = MetaData()
        self._news = Table('news', self._metadata,
            Column('id', Integer, Sequence('news_id_seq'), primary_key=True),
            Column('title', String(300), nullable=False),
            Column('text', String(4000), nullable=False, index=True, unique=True),
            Column('topic', String(50)),
            Column('cluster', String(300), nullable=False, index=True),
            Column('datetime', DateTime),
            Column('publisher', String(100)),
        )

        try:
            self._metadata.create_all(self._engine)
        except OperationalError as exc:
            raise OpenError(str(exc))

        self._conn = self._engine.connect()

    def addNews(self, news_list):
        news_list = self._preprocessNews(news_list)
        self._conn.execute(self._news.insert(), news_list)

    def _preprocessNews(self, news_list):
        for news in news_list:
            # Проверить, что строковые значения не слишком длинные
            for column in self._news.columns:
                if isinstance(column.type, String):
                    name = column.name
                    if len(news.get(name, "")) > column.type.length:
                        logging.warning("обрезано значение для столбца '{}' (с {} до {})"
                            .format(name, len(news[name]), column.type.length))
                        news[name] = news[name][:column.type.length]
        return news_list

    def getClustersByNewsTexts(self, texts):
        texts = [t[:self._news.c.text.type.length] for t in texts]
        s = select([self._news.c.cluster]).where(self._news.c.text.in_(texts))
        result = self._conn.execute(s)
        return [r[0] for r in result]

    def renameClusters(self, old_names, new_name):
        s = self._news.update().where(self._news.c.cluster.in_(old_names)) \
                .values(cluster=new_name)
        self._conn.execute(s)

    def exportToCsv(self, csvpath):
        rows = [dict(r) for r in self._conn.execute(select([self._news]))]
        with open(csvpath, "w", encoding="utf-8") as f:
            writer = csv.DictWriter(f, [col.name for col in self._news.columns])
            writer.writeheader()
            writer.writerows(rows)

    def populateFrom(self, other):
        for i in itertools.count(step=10000):
            result = other._conn.execute(select([other._news]).limit(10000).offset(i))
            news = [dict(n) for n in result]
            if not news:
                return
            for n in news:
                n.pop("id", None)

            # На всякий случай: обрезать тексты, если они слишком длинные,
            # и убедиться, что тексты наших новостей уникальны
            news = self._preprocessNews(news)
            to_insert = list({n["text"]: n for n in news
                              if not self.newsTextExists(n["text"])}.values())

            if to_insert:
                self._conn.execute(self._news.insert(), to_insert)

    def newsTextExists(self, text):
        text = text[:self._news.c.text.type.length]
        s = select([self._news.c.id]) \
                .where(self._news.c.text == text).limit(1)
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

        missing_keys = set(["user", "password", "database"]) - set(config.keys())
        if missing_keys:
            raise ConfigError("не найдены ключи: {}".format(", ".join(missing_keys)))

        engine_string = 'postgresql://{}:{}@{}:{}/{}'.format(config["user"],
            config["password"], config.get("host", self.DEFAULT_HOST),
            config.get("port", self.DEFAULT_PORT), config["database"])

        super().__init__(engine_string)
