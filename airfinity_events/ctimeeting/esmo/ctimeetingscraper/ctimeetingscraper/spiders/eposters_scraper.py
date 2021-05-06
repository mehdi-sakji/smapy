"""
E-posters scraper.
"""

import scrapy
import uuid
from nameparser import HumanName
from datetime import datetime


class EPostersScraper(scrapy.Spider):
    """
    Spider for scraping Ctimeeting ESMO 2020 E-posters.
    """

    name = "e_posters_scraper"
    eid = str(uuid.uuid4()) # TODO: Unify with eid from sessions scraper

    def start_requests(self):
        """
        Yields list of primary URLs to scrape.
        """

        lists_urls = [
            "https://cslide.ctimeetingtech.com/esmo2020/attendee/confcal_2/presentation?p={}".format(
                str(i + 1)) for i in range(154)]
        for i, url in enumerate(lists_urls):
            yield scrapy.Request(
                url=url, meta={"eprange": i + 1},
                callback=self.scrape_e_posters_range)

    def scrape_e_posters_range(self, response):
        """
        Yields scraped e_posters for each page.
        """

        eprange = response.meta["eprange"]
        epcards = response.css(".list-view")[0].css(".presentation")
        ep_contents = []
        for eporder, epcard in enumerate(epcards):
            ep_contents += self.scrape_e_poster(eprange, eporder, epcard)
        for ep_content in ep_contents:
            yield ep_content

    def scrape_e_poster(self, eprange, eporder, epcard):
        """
        Scrapes one e-poster's data, persons, and abstracts.
        """

        ep_contents = []
        epday, e_poster = self.scrape_e_poster_inner(eprange, eporder, epcard)
        ep_contents.append(e_poster)
        e_poster_persons = self.scrape_e_poster_persons(epcard, e_poster)
        ep_contents += e_poster_persons
        return ep_contents

    def scrape_e_poster_inner(self, eprange, eporder, epcard):
        """
        Scrapes one e_poster's inner data.
        """

        epid = str(uuid.uuid4())
        epdict = {
            "id": epid, "class": "eposter", "range": eprange, "order": eporder,
            "event_id": self.eid}
        epblock = epcard.css(".card-block")[0]
        epdict["title"] = epblock.css(".card-title::text").extract_first()
        ep_day = epblock.css(".internal")[2].css(".property::text").extract_first()
        epdict["start_time"], epdict["end_time"] = self.parse_date(ep_day)
        if len(epblock.css(".abstract")) >= 1:
            epdict["description"] = " ".join(epblock.css(".abstract *::text").extract())
        return ep_day, epdict

    def scrape_e_poster_persons(self, epcard, e_poster):
        """
        Scrapes one E-posters's persons (speaker and authors).
        """

        ep_persons = []
        block = epcard.css(".card-block")[0]
        if len(block.css(".internal")[1].css(".property")) >= 1:
            speakers_block = block.css(".internal")[1].css(
                ".property")[0].css("li")
            for speaker in speakers_block:
                ep_persons.append(self.scrape_person(
                    speaker, "speaker", "posterperson", e_poster["id"]))
        # TODO add persons from Disclosure
        return ep_persons

    def scrape_person(self, pblock, prole, pclass, p_cid):
        """
        Parses one person's name and location.
        """

        pdict = {"event_id": self.eid, "class": pclass, "content_id": p_cid, "role": prole}
        pfullname = pblock.css("*::text").extract()[0].strip()
        plocation = pblock.css("*::text").extract()[1].strip()
        pdict["full_name"] = pfullname
        pname = HumanName(pfullname)
        pdict["first_name"] = pname.first
        pdict["last_name"] = pname.last
        pdict["title"] = pname.title
        pdict["second_name"] = pname.middle
        pdict["city"] = plocation.split(",")[0].strip()[1:]
        if len(plocation.split(",")) > 1:
            pdict["country"] = plocation.split(",")[1].strip()[:-1]
        return pdict

    @staticmethod
    def parse_date(day):
        """
        Parses day and time to standard datetime format.
        """

        start_time = datetime.strptime(
            day.split(",")[0].strip(), "%d.%m.%Y").strftime("%Y-%m-%dT%H:%M:%S.%f")
        end_time = datetime.strptime(
            day.split(",")[0].strip(), "%d.%m.%Y").strftime("%Y-%m-%dT%H:%M:%S.%f")
        return start_time, end_time
