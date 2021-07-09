import scrapy
import uuid
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from nameparser import HumanName
import json


class CTimeetinScraper(scrapy.Spider):
    """ Spider for scraping Ctimeeting 2020 event. """

    name = "ctimeeting_scraper"
    eid = str(uuid.uuid4())

    def start_requests(self):
        """"""

        lists_urls = [
            "https://cslide.ctimeetingtech.com/eas20/attendee/confcal/session/list?p={}".format(
                str(i+1)) for i in range(5)]
        for i, url in enumerate(lists_urls):
            yield scrapy.Request(
                url = url, meta={"srange": i+1},
                callback = self.parse_session_range_urls)
    
    
    def parse_session_range_urls(self, response):
        """"""

        srange = response.meta["srange"]
        scards = response.css(".list-view-list")[0].css(".session")
        for sorder, scard in enumerate(scards):
            if len(scard.css(".card-block")[0].css(".session-title")[0].css(
                "a::attr(href)"))==0:
                with open("../rawoutput/breaks.json", "a+") as breakfile:
                    breaksession = self.scrape_session_inner(
                        sorder, srange, scard)[1]
                    json.dump(breaksession, breakfile)
                    breakfile.write('\n')
            else:
                surl = scard.css(".card-block")[0].css(".session-title")[0].css(
                    "a::attr(href)").extract_first()
                yield scrapy.Request(
                    url = surl, meta={"srange": srange, "sorder": sorder},
                    callback = self.scrape_session)

    

    def scrape_session(self, response):
        """"""          

        sorder = response.meta["sorder"]
        srange = response.meta["srange"]
        scontents = []
        scard = response.css(".session")[0]
        sday, session = self.scrape_session_inner(
            response.meta["sorder"], response.meta["srange"], scard)
        scontents.append(session)
        session_chairs = self.scrape_session_chairs(scard, session)
        scontents += session_chairs
        session_presentations = self.scrape_session_presentations(
            sorder, srange, scard, sday, session)
        scontents += session_presentations
        for content in scontents:
            yield content


    def scrape_session_inner(self, sorder, srange, scard):
        """"""

        sid = str(uuid.uuid4())
        sdict = {
            "id": sid, "class": "session", "range": srange, "order": sorder,
            "event_id": self.eid}
        sblock = scard.css(".card-block")[0]
        sdict["title"] = sblock.css(".session-title *::text").extract_first()
        sdict["session_type"] = sblock.css(".internal_type")[0].css(
            ".property::text").extract_first()
        sdict["location"] = sblock.css(".internal_room")[0].css(
            ".property::text").extract_first()
        sday = sblock.css(".internal_date")[0].css(".property::text").extract_first()
        stime = sblock.css(".internal_time")[0].css(".property::text").extract_first()
        sdict["start_time"], sdict["end_time"] = self.compute_date(sday, stime)
        if len(sblock.css(".internal_description"))>=1:
            sdict["description"] = sblock.css(".internal_description")[0].css(
                ".property::text").extract_first()
        return sday, sdict


    def scrape_session_chairs(self, scard, session):
        """"""

        schairs = []
        sblock = scard.css(".card-block")[0]
        if len(sblock.css(".internal_moderators")[0].css(".property"))==0:
            return schairs
        schairsblocks = sblock.css(".internal_moderators")[0].css(
            ".property")[0].css("li")
        for schair in schairsblocks:
            schair_url = schair.css("a::attr(href)").extract_first()
            schairs.append(self.scrape_person(
                schair_url, "chairperson", "sessionperson", session["id"]))
        return schairs

    """
    def scrape_session_chairs(self, scard, session):
        schairs = []
        sblock = scard.css(".card-block")[0]
        if len(sblock.css(".internal_moderators")[0].css(".property"))==0:
            return schairs
        schairsblocks = sblock.css(".internal_moderators")[0].css(
            ".property")[0].css("li")
        for schair in schairsblocks:
            schair_url = schair.css("a::attr(href)").extract_first()
            schair_page = requests.get(schair_url)
            schair_bpage = BeautifulSoup(schair_page.text, "html5lib")
            schairs.append(self.scrape_person(
                schair_bpage, "chairperson", "sessionperson", session["id"]))
        return schairs
    """

    def scrape_session_presentations(self, sorder, srange, scard, sday, session):
        """"""

        spcontents = []
        pcards = sblock = scard.css(
            ".card-block")[0].css(".item-content")[0].css(".presentation")
        for porder, pcard in enumerate(pcards):
            presentation = self.scrape_presentation_inner(
                sorder, srange, sday, session["id"], porder, pcard)
            spcontents.append(presentation)
            presenters = self.scrape_presenters(pcard, presentation)
            spcontents += presenters
        return spcontents


    def scrape_presentation_inner(self, sorder, srange, sday, sid, porder, pcard):
        """"""

        pid = str(uuid.uuid4())
        pdict = {
            "id": pid, "class": "presentation", "order": porder,
            "session_order": sorder, "session_range": srange,
            "event_id": self.eid, "session_id": sid}
        pblock = pcard.css(".card-block")[0]
        pdict["title"] = pblock.css(".card-title::text").extract_first()
        ptime = pblock.css(".p")[0].css(".property::text").extract_first()
        pdict["start_time"], pdict["end_time"] = self.compute_date(sday, ptime)
        if len(pblock.css(".abstract"))>=1:
            pdict["description"] = " ".join(pblock.css(".abstract *::text").extract()).strip()
        return pdict


    def scrape_presenters(self, pcard, presentation):
        """"""

        presenters = []
        pblock = pcard.css(".card-block")[0]
        if len(pblock.css(".p")[1].css(".property"))==0:
            return presenters
        presentersblock = pblock.css(".p")[1].css(".property")[0].css("li")
        for presenter in presentersblock:
            presenter_url = presenter.css("a::attr(href)").extract_first()
            presenters.append(self.scrape_person(
                presenter_url, "presenter", "presentationperson",
                presentation["id"]))
        return presenters

    """
    def scrape_presenters(self, pcard, presentation):
        presenters = []
        pblock = pcard.css(".card-block")[0]
        presentersblock = pblock.css(".p")[1].css(".property")[0].css("li")
        for presenter in presentersblock:
            presenter_url = presenter.css("a::attr(href)").extract_first()
            presenter_page = requests.get(presenter_url)
            presenter_bpage = BeautifulSoup(presenter_page.text, "html5lib")
            presenters.append(self.scrape_person(
                presenter_bpage, "presenter", "presentationperson",
                presentation["id"]))
        return presenters
    """

    def scrape_person(self, purl, prole, pclass, p_cid):
        """"""

        pdict = {}
        pdict["event_id"] = self.eid
        pdict["class"] = pclass
        pdict["content_id"] = p_cid
        pdict["role"] = prole
        pdict["url"] = purl
        return pdict

    """
    def scrape_person(self, person_bpage, prole, pclass, p_cid):
        pdict = {}
        pdict["event_id"] = self.eid
        pdict["class"] = pclass
        pdict["content_id"] = p_cid
        pdict["role"] = prole
        ptitle = person_bpage.find_all(
            "div", attrs={"class": "person-details"})[0].find_all("h3")[0].text.strip()
        pdict["full_name"] = ptitle.split(",")[0].strip().replace(" ", "_").lower()
        pname = HumanName(ptitle.split(",")[0].strip())
        pdict["first_name"] = pname.first
        pdict["last_name"] = pname.last
        pdict["title"] = pname.title
        pdict["second_name"] = pname.middle
        pdict["country"] = ptitle.split(",")[1].strip()
        pdict["university"] = person_bpage.find_all(
            "div", attrs={"class": "person-details"})[0].find_all("span")[0].text.strip()
        return pdict
    """

    def compute_date(self, day, time):
        """"""

        start_time = datetime.strptime(
            day.split(",")[0].strip() + " " + time.split("-")[0].strip(),
            "%d.%m.%Y %H:%M").strftime("%Y-%m-%dT%H:%M:%S.%f")
        end_time = datetime.strptime(
            day.split(",")[0].strip() + " " + time.split("-")[1].strip(),
            "%d.%m.%Y %H:%M").strftime("%Y-%m-%dT%H:%M:%S.%f")
        return start_time, end_time
