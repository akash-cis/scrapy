# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonScraperItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    price = scrapy.Field()
    image = scrapy.Field()
    link = scrapy.Field()
    asin = scrapy.Field()
    brand = scrapy.Field()
    rating = scrapy.Field()
    rating_breakdown = scrapy.Field()
    ratings_total = scrapy.Field()
    reviews_total = scrapy.Field()
    main_image = scrapy.Field()
    images = scrapy.Field()
    images_count = scrapy.Field()
    feature_bullets = scrapy.Field()
    feature_bullets_count = scrapy.Field()
    feature_bullets_flat = scrapy.Field()
    specifications = scrapy.Field()
    specifications_flat = scrapy.Field()
    bestsellers_rank = scrapy.Field()
    bestsellers_rank_flat = scrapy.Field()
    manufacturer = scrapy.Field()
    first_available = scrapy.Field()
    categories = scrapy.Field()
    variations = scrapy.Field()

class AmazonDepartmentItem(scrapy.Item):
    category = scrapy.Field()
    category_code = scrapy.Field()
    category_parent_code = scrapy.Field()
