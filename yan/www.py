
import time
import datetime
import logging
import requests

import feedparser
from bs4 import BeautifulSoup


class YandexNews:

    feeds = [
        "https://news.yandex.ru/index.rss",
    ]

    def clusters(self):
        for feedurl in self.feeds:
            logging.info("Парсим RSS-ленту '{}'".format(feedurl))
            rss = YandexRssPage(feedurl)
            for one_news in rss.news():
                logging.info("Прошли по RSS-ссылке '{}'".format(one_news.title))
                news_cluster = one_news.cluster()
                if news_cluster:
                    logging.info("Зашли во все источники '{}', спарсиили {} новостей"
                                 .format(news_cluster.title, news_cluster.news_count))
                    yield news_cluster.title, news_cluster.toJson()


class YandexRssPage:

    def __init__(self, url):
        time.sleep(1)
        self._feed = feedparser.parse(url)

    def news(self):
        for entry in self._feed["entries"]:
            if entry["links"]:
                yield YandexSingleNewsPage(entry["links"][0]["href"])


class YandexSingleNewsPage:

    def __init__(self, url):
        time.sleep(1)
        self._html = requests.get(url).text
        self._soup = BeautifulSoup(self._html, "html.parser")
        self.title = self._soup.find("h1", {"class": "story__head"}).text
        self._cluster_link = None
        for link in self._soup.find_all("a"):
            if "Все источники — " in link.text:
                self._cluster_link = link.get("href")
        if self._cluster_link is None:
            logging.warning("Не найдено ссылки на 'Все источники'")

    def cluster(self):
        if self._cluster_link:
            return YandexClusterNewsPage("https://news.yandex.ru" + self._cluster_link)


class YandexClusterNewsPage:

    month_map = {
        "января": 1,
        "февраля": 2,
        "марта": 3,
        "апреля": 4,
        "мая": 5,
        "июня": 6,
        "июля": 7,
        "августа": 8,
        "сентября": 9,
        "октября": 10,
        "ноября": 11,
        "декабря": 12,
    }

    def __init__(self, url):
        time.sleep(1)
        self._html = requests.get(url).text
        self._soup = BeautifulSoup(self._html, "html.parser")
        self.title = self._soup.find("h1", {"class": "story__head"}).text
        self.topic = self._soup.find("li", {"class": "tabs-menu__tab_active_yes"}).text
        self._news = []

        for div in self._soup.find_all("div", {"class" : "doc_for_instory"}):
            news = {
                "title": div.find("h2", {"class" : "doc__title"}).text,
                "text": div.find("div", {"class" : "doc__content"}).text,
                "publisher": div.find("div", {"class" : "doc__agency"}).text,
                "datetime": div.find("div", {"class" : "doc__time"}).text,
                "cluster": self.title,
                "topic": self.topic
            }
            news["datetime"] = self._parseDatetime(news["datetime"])
            self._news.append(news)

        self.news_count = len(self._news)

    def toJson(self):
        return [dict(news) for news in self._news]

    def _parseDatetime(self, timestr):
        words = timestr.lower().split()
        hour, minute = [int(x) for x in words[-1].split(":")]
        if len(words) == 1 or words[-3] == "сегодня":
            date = datetime.date.today()
        elif words[-3] == "вчера":
            date = datetime.date.today() - datetime.timedelta(days=1)
        elif "." in words[-3]:
            day, month, year = [int(x) for x in words[-3].split()]
            date = datetime.date(day=day, month=month, year=2000+year)
        elif words[-3] in self.month_map:
            day = int(words[-4])
            date = datetime.date(day=day, month=self.month_map[words[-3]],
                year=datetime.today().year)
            if date > datetime.date.today():
                date -= datetime.timedelta(years=1)
        else:
            logging.warning("Не удалось определить дату публикации: " + timestr)
            return None
        return datetime.datetime(minute=minute, hour=hour, day=date.day,
            month=date.month, year=date.year)
