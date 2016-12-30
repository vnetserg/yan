
import logging

from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, DateTime, Sequence

from sqlalchemy.sql import select


class OpenError(Exception): pass


class SQLiteDAO:

    def __init__(self, filename):
        self._engine = create_engine('sqlite:///' + filename)
        self._metadata = MetaData()
        self._news = Table('news', self._metadata,
            Column('id', Integer, Sequence('news_id_seq'), primary_key=True),
            Column('title', String(100), nullable=False),
            Column('text', String(500), nullable=False, index=True),
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
        return [dict(row) for riw in result]
