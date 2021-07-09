"""
Post scraping POC processing script.
"""
import json


class PocProcessor:
    """
    POC class with post scrapping processing methods (e.g.data formatting).
    """

    @staticmethod
    def read_scraped_data(path):
        """
        Reads raw scraped data file and returns data as a json object.

        :param path: path to raw scraped data json file.
        :return: json object with raw scraped data.
        """

        with open(path) as raw_scraped_file:
            data = json.load(raw_scraped_file)
        return data

    @staticmethod
    def process_data(data):
        """
        Formats scraped data to match expected output format.

        :param data: raw scraped data as a json object.
        :return: processed scraped data as a json object.
        """

        processed_data = {"products": data}
        return processed_data

    @staticmethod
    def write_output(processed_data, output_path):
        """
        Writes processed data to processed scraped data output json file.

        :param processed_data:
        :param output_path:
        :return: 1 if processing succeeded.
        """

        with open(output_path, 'w+') as out_scraped_file:
            json.dump(processed_data, out_scraped_file)
        return 1


if __name__ == "__main__":
    poc_processor = PocProcessor()
    raw_data = poc_processor.read_scraped_data("../output/raw/products.json")
    processed_data = poc_processor.process_data(raw_data)
    poc_processor.write_output(processed_data, "../output/processed/products.json")
