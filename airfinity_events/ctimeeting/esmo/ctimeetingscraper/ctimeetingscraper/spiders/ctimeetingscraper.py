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
    confcal_pages_mapping = {
        1:2, 2:1, 3:6, 4:6, 5:1}

    def start_requests(self):
        """"""
         
        lists_urls = [
            "https://cslide.ctimeetingtech.com/esmo2020/attendee/confcal/session/list?p={}".format(
                str(i+1)) for i in range(4)]
        for n in self.confcal_pages_mapping.keys():
            for p in range(self.confcal_pages_mapping[n]):
                lists_urls.append("https://cslide.ctimeetingtech.com/esmo2020/attendee/confcal_{}/session/list?p={}".format(
                str(n), str(p+1)))
        for i, url in enumerate(lists_urls):
            yield scrapy.Request(
                url = url, meta={"srange": i+1},
                callback = self.parse_session_range_urls)
    
    
    def parse_session_range_urls(self, response):
        """ Loops through sessions pages. """

        srange = response.meta["srange"]
        scards = response.css(".list-view-list")[0].css(".session")
        for sorder, scard in enumerate(scards):
            if len(scard.css(".card-block")[0].css(".session-title")[0].css(
                "a::attr(href)"))==0:
                """
                # Commented because each session has a link
                with open("../rawoutput/breaks.json", "a+") as breakfile:
                    breaksession = self.scrape_session_inner(
                        sorder, srange, scard)[1]
                    json.dump(breaksession, breakfile)
                    breakfile.write('\n')
                """
                print("Unavailable href")
            else:
                surl = scard.css(".card-block")[0].css(".session-title")[0].css(
                    "a::attr(href)").extract_first()
                yield scrapy.Request(
                    url = surl, meta={"srange": srange, "sorder": sorder},
                    callback = self.scrape_session)

    

    def scrape_session(self, response):
        """ Scrapes one session data, persons, presentations and their persons """          

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
        """ Scrapes one session inner data. """

        sid = str(uuid.uuid4())
        sdict = {
            "id": sid, "class": "session", "range": srange, "order": sorder,
            "event_id": self.eid}
        sblock = scard.css(".card-block")[0]
        sheader = scard.css(".card-header")[0]

        sdict["title"] = sblock.css(".session-title *::text").extract_first()
       
        slabels = sheader.css(".labels")[0].css("span::text").extract()
        
        sdict["session_type"] = slabels[0]
        try:
            sdict["location"] = sblock.css(".internal_room")[0].css(
                ".property::text").extract_first()
        except:
            pass
        try:
            sday = sblock.css(".internal_date")[0].css(".property::text").extract_first()
            stime = sblock.css(".internal_time")[0].css(".property::text").extract_first()
            sdict["start_time"], sdict["end_time"] = self.compute_date(sday, stime)
        except:
            pass
        if len(sblock.css(".internal_description"))>=1:
            sdict["description"] = sblock.css(".internal_description")[0].css(
                ".property::text").extract_first()
        return sday, sdict


    def scrape_session_chairs(self, scard, session):
        """ Scrapes one session's chaires."""

        schairs = []
        sblock = scard.css(".card-block")[0]
        if len(sblock.css(".internal_moderators")[0].css(".property"))==0:
            return schairs
        schairsblocks = sblock.css(".internal_moderators")[0].css(
            ".property")[0].css("li")
        for schair in schairsblocks:
            schairs.append(self.scrape_person(
                schair, "chairperson", "sessionperson", session["id"]))
        return schairs
    

    def scrape_session_presentations(self, sorder, srange, scard, sday, session):
        """ Scrapes one session's presentations and presenters. """

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
        """ Scrapes one presentation's inner data. """

        pid = str(uuid.uuid4())
        pdict = {
            "id": pid, "class": "presentation", "order": porder,
            "session_order": sorder, "session_range": srange,
            "event_id": self.eid, "session_id": sid}
        pblock = pcard.css(".card-block")[0]
        pdict["title"] = pblock.css(".card-title::text").extract_first()
        try:    
            ptime = pblock.css(".p")[1].css(".property::text").extract_first()
            pdict["start_time"], pdict["end_time"] = self.compute_date(sday, ptime)
        except:
            pass
        if len(pblock.css(".abstract"))>=1:
            pdict["description"] = " ".join(pblock.css(".abstract *::text").extract()).strip()
        return pdict


    def scrape_presenters(self, pcard, presentation):
        """ Scrapes one presentation's persons (presenters). """

        presenters = []
        pblock = pcard.css(".card-block")[0]
        try:
            presentersblock = pblock.css(".persons")[0].css("li")
            for presenter in presentersblock:
                presenters.append(self.scrape_person(
                    presenter, "presenter", "presentationperson",
                    presentation["id"]))
        except:
            pass
        return presenters


    def scrape_person(self, pblock, prole, pclass, p_cid):
        """ Parses one person's name and location. """

        pdict = {}
        pdict["event_id"] = self.eid
        pdict["class"] = pclass
        pdict["content_id"] = p_cid
        pdict["role"] = prole
        
        pfullname = pblock.css("*::text").extract()[0].strip()
        plocation = pblock.css("*::text").extract()[1].strip()
        pdict["full_name"] = pfullname
        pname = HumanName(pfullname)
        pdict["first_name"] = pname.first
        pdict["last_name"] = pname.last
        pdict["title"] = pname.title
        pdict["second_name"] = pname.middle
        pdict["country"] = plocation.split(",")[1].strip()[:-1]
        pdict["city"] = plocation.split(",")[0].strip()[1:]
        return pdict


    def compute_date(self, day, time):
        """ Parsesday and time to standard datetime format. """

        start_time = datetime.strptime(
            day.split(",")[0].strip() + " " + time.split("-")[0].strip(),
            "%d.%m.%Y %H:%M").strftime("%Y-%m-%dT%H:%M:%S.%f")
        end_time = datetime.strptime(
            day.split(",")[0].strip() + " " + time.split("-")[1].strip(),
            "%d.%m.%Y %H:%M").strftime("%Y-%m-%dT%H:%M:%S.%f")
        return start_time, end_time

