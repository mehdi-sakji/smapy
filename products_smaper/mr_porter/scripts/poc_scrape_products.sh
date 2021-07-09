cd ../mr_porter_scrapy/
rm -rf ../output/raw/products.json
scrapy crawl poc_products_spider -o ../output/raw/products.json
python ../mr_porter_process/poc_process.py
