"""
POC products scraping spider.
"""

import scrapy


class POCProductsSpider(scrapy.Spider):
    """
    POC Spider class for scraping one product example.
    """

    name = "poc_products_spider"

    def start_requests(self):
        """
        Requests list of products URLs (one product for POC code).

        :return: Generator of request jobs (one job per URL).
        """

        lists_urls = ["https://www.mrporter.com/en-us/mens/product/mr-p/clothing/"
                      + "winter-coats/checked-brushed-virgin-wool-and-llama-hair-blend-coat/33599693056299663"]
        for auto_inc_id, url in enumerate(lists_urls):
            yield scrapy.Request(url=url, meta={"id": auto_inc_id+1}, callback=self.scrape_product)

    def scrape_product(self, response):
        """
        Scrapes product.

        :param response: Product's URL requesting response.
        :return: Generator of dictionary with scraped product.
        """

        product_dict = {}
        brand, shop_more_items = self.scrape_level_0(response, product_dict)
        self.scrape_attributes(response, product_dict, brand, shop_more_items)
        self.scrape_additional_features(response, product_dict)
        yield product_dict

    @staticmethod
    def scrape_level_0(response, product_dict):
        """
        Updates product dictionary with level 0 fields (e.g. title, images etc.).

        :param response: Product's URL requesting response.
        :param product_dict: Dictionary with product's fields values.
        :return: product's brand and related categories to be used in following methods.
        """

        product_dict["url"] = response.url
        product_dict["title"] = response.css(
            ".ProductInformation83__name::text").extract_first()
        description_paragraphs = response.css(
            ".EditorialAccordion83__accordionContent--editors_notes *::text").extract()
        product_dict["description"] = "".join(description_paragraphs)
        product_dict["product_id"] = response.meta["id"]
        product_color = response.css(".ProductDetailsColours83__colourName::text").extract_first()
        product_dict["color"] = product_color
        images = response.css(".ImageCarousel83__viewport ")[0].css("li")
        image_sources = [
            item.css("img::attr(src)").extract()[1] for item in images
            if len(item.css("img::attr(src)").extract()) >= 1]
        image_urls = ["https:{}".format(item) for item in image_sources]
        product_dict["images"] = image_urls
        videos = response.css(".ImageCarousel83__viewport ")[0].css("video")
        video_urls = [item.css("source::attr(src)").extract_first() for item in videos]
        product_dict["videos"] = video_urls
        shop_more_items = response.css(".ShopMore83__links")[0].css(".ShopMore83__link::text").extract()
        category = shop_more_items[2]
        product_dict["category"] = category
        age_group = response.url.split("/")[4]
        product_dict["age_group"] = age_group
        brand = response.css(".ProductInformation83__designer")[0].css("span::text").extract_first()
        product_dict["brand"] = brand
        # NB TODO check if this code corresponds to UPC or something else
        product_dict["upc"] = response.url.split("/")[-1]
        # NB the following fields were not found on the product's web page
        product_dict["capacity"] = ""
        product_dict["SKU"] = ""
        return brand, shop_more_items

    @staticmethod
    def scrape_attributes(response, product_dict, brand, shop_more_items):
        """
        Updates product dictionary with attributes (e.g. manufacturer, price etc.).

        :param response: Product's URL requesting response.
        :param product_dict: Dictionary with product's fields values.
        :param brand: product's brand
        :param shop_more_items: product's related categories items.
        :return: 1 if processing succeeded.
        """

        product_dict["attributes"] = {"manufacturer": brand}
        was_price = response.css(".PriceWithSchema9__wasPrice::text").extract_first()
        product_dict["attributes"]["was_price"] = was_price
        price = response.css(".PriceWithSchema9__value")[0].css(
            'span[itemprop="price"]::text').extract_first()
        product_dict["attributes"]["price"] = price
        discount = response.css(".PriceWithSchema9__discount::text").extract_first()
        product_dict["attributes"]["discount"] = discount
        available_sizes = response.css(".multipleSizes")[0].css(
            ".CombinedSelect11__option::attr(value)").extract()
        product_dict["attributes"]["sizes"] = available_sizes
        all_categories = shop_more_items[1:]
        product_dict["attributes"]["categories"] = all_categories
        return 1

    @staticmethod
    def scrape_additional_features(response, product_dict):
        """
        Updates product dictionary with additional features.

        :param response: Product's URL requesting response.
        :param product_dict: Dictionary with product's fields values.
        :return: 1 if processing succeeded.
        """

        size_fit_features = response.css(
            ".EditorialAccordion83__accordionContent--size_and_fit")[0].css("li::text").extract()
        details_care = response.css(
            ".EditorialAccordion83__accordionContent--details_and_care")[0].css("li::text").extract()
        product_dict["additional_features"] = size_fit_features + details_care
        return 1
