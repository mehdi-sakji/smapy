"""
Sessions and presentations scraper.
"""

import scrapy
import uuid
from nameparser import HumanName
from datetime import datetime


class SessionsScraper(scrapy.Spider):
    """
    Spider for scraping Ctimeeting ESMO 2021 event's sessions and underlying presentations.
    """

    name = "sessions_scraper"
    eid = str(uuid.uuid4())

    def start_requests(self):
        """
        Yields urls of sessions pages + scrape_session_page callback.
        """
         
        lists_urls = [
            "https://cslide.ctimeetingtech.com/eas21/attendee/confcal/session/list?p={}".format(
                str(i+1)) for i in range(4)]
        for i, url in enumerate(lists_urls):
            yield scrapy.Request(
                url = url, meta={"session_range": i+1},
                callback = self.scrape_session_page)

    def scrape_session_page(self, response):
        """
        Yields sessions URLs + scrape_session callback.
        """

        session_range = response.meta["session_range"]
        session_cards = response.css(".list-view-list")[0].css(".session")
        for session_order, session_card in enumerate(session_cards):
            session_url = session_card.css(".card-block")[0].css(".session-title")[0].css(
                "a::attr(href)").extract_first()
            """
            if session_url is None:
                print("Unavailable session URL.")
                import pdb
                pdb.set_trace()
                yield scrapy.Request(
                    url=response.url, meta={
                        "session_range": session_range, "session_order": session_order,
                        "session_card": session_card},
                    callback=self.scrape_unbrowsable_session)
            else:
            """
            if session_url is not None:
                yield scrapy.Request(
                    url=session_url, meta={"session_range": session_range, "session_order": session_order},
                    callback=self.scrape_session)

    def scrape_unbrowsable_session(self, response):
        """
        Yields one session data and persons if not browsable.
        (e.g.
        """

        session_order = response.meta["session_order"]
        session_range = response.meta["session_range"]
        session_contents = []
        session_card = response.meta["session_card"]
        session_day, session = self.scrape_session_inner_data(
            session_order, session_range, session_card)
        session_contents.append(session)
        session_chairs = self.scrape_session_chairs(session_card, session)
        session_contents += session_chairs
        for content in session_contents:
            yield content

    def scrape_session(self, response):
        """
        Yields one session data, persons, presentations and their persons.
        """

        session_order = response.meta["session_order"]
        session_range = response.meta["session_range"]
        # list: session + session persons + presentations + presentations persons
        session_contents = []
        session_card = response.css(".session")[0]
        session_day, session = self.scrape_session_inner_data(
            session_order, session_range, session_card)
        session_contents.append(session)
        session_chairs = self.scrape_session_chairs(session_card, session)
        session_contents += session_chairs
        session_presentations = self.scrape_session_presentations(
            session_order, session_range, session_card, session_day, session)
        session_contents += session_presentations
        for content in session_contents:
            yield content

    @staticmethod
    def scrape_session_location(session_dict, session_block):
        """
        Scrapes session's location.
        """

        session_dict["location"] = session_block.css(".internal_room")[0].css(
            ".property::text").extract_first().strip()
        return 1

    def scrape_session_time(self, session_dict, session_block):
        """
        Scrapes session's date and yime.
        """

        session_day = session_block.css(".internal_date")[0].css(".property::text").extract_first()
        session_time = session_block.css(".internal_time")[0].css(".property::text").extract_first()
        session_dict["start_time"], session_dict["end_time"] = self.parse_date(session_day, session_time)
        return session_day

    def scrape_session_inner_data(self, session_order, session_range, session_card):
        """
        Scrapes one session's inner data.
        """

        session_id = str(uuid.uuid4())
        session_dict = {
            "id": session_id, "class": "session", "range": session_range, "order": session_order,
            "event_id": self.eid}
        session_block = session_card.css(".card-block")[0]
        session_dict["title"] = session_block.css(".session-title *::text").extract_first().strip()
        self.scrape_session_location(session_dict, session_block)
        session_day = self.scrape_session_time(session_dict, session_block)
        return session_day, session_dict

    def scrape_session_chairs(self, session_card, session):
        """
        Scrapes one session's chairs.
        """

        session_chairs = []
        session_block = session_card.css(".card-block")[0]
        if len(session_block.css(".internal_moderators")[0].css(".property"))>0:
            session_chairs_blocks = session_block.css(".internal_moderators")[0].css(
                ".persons")[0].css("li")
            for session_chair in session_chairs_blocks:
                session_chairs.append(self.scrape_person(
                    session_chair, "chairperson", "session_person", session["id"]))
        return session_chairs

    def scrape_session_presentations(self, session_order, session_range, session_card, session_day, session):
        """
        Scrapes one session's presentations and presenters.
        """

        presentations_and_persons = []
        presentation_cards = session_card.css(
            ".card-block")[0].css(".item-content")[0].css(".presentation")
        for presentation_order, presentation_card in enumerate(presentation_cards):
            presentation = self.scrape_presentation_inner_data(
                session_order, session_range, session_day, session["id"], presentation_order, presentation_card)
            presentations_and_persons.append(presentation)
            presenters = self.scrape_presenters(presentation_card, presentation)
            presentations_and_persons += presenters
        return presentations_and_persons

    def scrape_presentation_time(self, presentation_dict, presentation_block, session_day):
        """
        Scrapes session's date and yime.
        """

        presentation_time = presentation_block.css(".details")[0].css(".p")[0].css(".property::text").extract_first()
        presentation_dict["start_time"], presentation_dict["end_time"] = self.parse_date(
            session_day, presentation_time)
        return 1

    def scrape_presentation_inner_data(
            self, session_order, session_range, session_day, sid, presentation_order, presentation_card):
        """
        Scrapes one presentation's inner data.
        """

        pid = str(uuid.uuid4())
        presentation_dict = {
            "id": pid, "class": "presentation", "order": presentation_order,
            "session_order": session_order, "session_range": session_range,
            "event_id": self.eid, "session_id": sid}
        presentation_block = presentation_card.css(".card-block")[0]
        presentation_dict["title"] = presentation_block.css(".card-title::text").extract_first().strip()
        self.scrape_presentation_time(presentation_dict, presentation_block, session_day)
        if len(presentation_block.css(".abstract")) >= 1:
            presentation_dict["description"] = " ".join(presentation_block.css(".abstract *::text").extract()).strip()
        return presentation_dict

    def scrape_presenters(self, presentation_card, presentation):
        """
        Scrapes one presentation's persons (presenters).
        """

        presenters = []
        try:
            presentation_block = presentation_card.css(".card-block")[0]
            presenters_block = presentation_block.css(".persons")[0].css("li")
            for presenter in presenters_block:
                presenters.append(self.scrape_person(
                    presenter, "presenter", "presentation_person", presentation["id"]))
        except:
            print("No presenters for {}".format(presentation['title']))
        return presenters

    def scrape_person(self, person_block, person_role, person_class, content_id):
        """
        Parses one person's name and location.
        """

        person_dict = {
            "event_id": self.eid, "class": person_class, "content_id": content_id, "role": person_role}
        person_fullname = person_block.css("*::text").extract()[0].strip()
        person_location = person_block.css("*::text").extract()[1].strip().replace("(", "").replace(")", "")
        person_dict["full_name"] = person_fullname
        person_name_obj = HumanName(person_fullname)
        person_dict["first_name"] = person_name_obj.first
        person_dict["last_name"] = person_name_obj.last
        person_dict["title"] = person_name_obj.title
        person_dict["second_name"] = person_name_obj.middle
        person_dict["country"] = person_location
        return person_dict

    @staticmethod
    def parse_date(day, time):
        """
        Parses day and time to standard datetime format.
        """

        start_time = datetime.strptime(
            day.split(",")[1].strip() + " " + time.split("-")[0].strip(),
            "%d.%m.%Y %H:%M").strftime("%Y-%m-%dT%H:%M:%S.%f")
        end_time = datetime.strptime(
            day.split(",")[1].strip() + " " + time.split("-")[1].strip(),
            "%d.%m.%Y %H:%M").strftime("%Y-%m-%dT%H:%M:%S.%f")
        return start_time, end_time
