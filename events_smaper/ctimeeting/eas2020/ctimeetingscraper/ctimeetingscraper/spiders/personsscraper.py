import scrapy
from nameparser import HumanName
import json
import pandas as pd

class CTimeetinPersonScraper(scrapy.Spider):
    """ Spider for scraping Ctimeeting 2020 persons. """

    name = "ctimeeting_persons_scraper"
    

    def start_requests(self):
        """"""

        allcpdf = pd.read_json("../rawoutput/firstraw.json")
        pcdf = allcpdf[
            allcpdf["class"].isin(["sessionperson", "presentationperson"])][[
                "class", "event_id", "content_id", "role", "url"]]
        for index in range(len(pcdf)):
            pc = pcdf.iloc[index]
            yield scrapy.Request(
                url = pc["url"], meta={
                    "event_id": pc["event_id"], "content_id": pc["content_id"],
                    "role": pc["role"], "class": pc["class"]},
                callback = self.scrape_person)
    

    def scrape_person(self, response):
        """"""

        pdict = {}
        pdict["url"] = response.url
        pdict["event_id"] = response.meta["event_id"]
        pdict["class"] = "person"
        pdict["content_id"] = response.meta["content_id"]
        pdict["role"] = response.meta["role"]
        ptitle = response.css(".person-details")[0].css(
            "h3::text").extract_first().strip()
        pdict["full_name"] = ptitle.split(",")[0].strip().replace(" ", "_").lower()
        pname = HumanName(ptitle.split(",")[0].strip())
        pdict["first_name"] = pname.first
        pdict["last_name"] = pname.last
        pdict["title"] = pname.title
        pdict["second_name"] = pname.middle
        pdict["country"] = ptitle.split(",")[1].strip()
        university = response.css(".person-details")[0].css(
            "span::text").extract_first()
        if university is not None:
            pdict["university"] = university.strip()
        yield pdict






    