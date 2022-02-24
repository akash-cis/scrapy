import scrapy
from amazon_scraper.items import AmazonScraperItem

class ProductSpider(scrapy.Spider):
    name = 'amazon_spider'
    start_urls = ['https://www.amazon.com/s?i=fashion-novelty&bbn=12035955011&rh=p_6%3AATVPDKIKX0DER&s=featured&hidden-keywords=ORCA', ]
    domain = ''
    def parse(self, response, **kwargs):
        # products = response.xpath('//div[contains(@class, "nav-flyout-anchor")]/div[contains(@data-nav-timeline-item, "s-result-item s-asin")]')
        self.domain = response.xpath('//a[contains(@class, "nav-progressive-attribute") and contains(@id, "nav-link-accountList")]/@href').extract_first().split('/')[2]
        print('DOMAIN:-------------------------', self.domain)
        products = response.xpath('//div[contains(@class, "s-main-slot s-result-list")]/div[contains(@class, "s-result-item s-asin")]')
        # for product in products:
        product = products[1]
        # title = product.xpath('.//div[contains(@class, "s-title-instructions-style")]/h2/a[contains(@class, "s-link-style a-text-normal")]/span[contains(@class, "a-size-base-plus")]/text()').extract_first()
        link = product.xpath('.//div[contains(@class, "s-title-instructions-style")]/h2/a[contains(@class, "s-link-style a-text-normal")]/@href').extract_first().split('/')
        asin = link[3] if len(link[1]) > 2 else link[2]
        print(link)
        link_short = f'https://{self.domain}/{link[2]}/{link[3]}' if len(link[1]) > 2 else f'https://www.amazon.com/{link[1]}/{link[2]}'
        yield scrapy.Request(link_short, callback=self.single_product_parse, cb_kwargs=dict(link= link_short, asin= asin))

    def single_product_parse(self, response, link=None, asin=None):
        item = AmazonScraperItem()
        title = response.xpath('//div[contains(@id,"titleSection")]//span/text()').extract_first().strip()

        # PRICE
        price_list = response.xpath('//div[contains(@id,"corePrice_desktop")]//span[contains(@class, "a-offscreen")]/text()').extract()
        brand_string = response.xpath('//a[contains(@id,"bylineInfo")]/text()').extract_first()
        brand = brand_string.split()[0] if brand_string.find('Brand:') == 0 else brand_string.split('Visit the ')[1] if brand_string.find('Visit the ') == 0 else brand_string

        # print(brand)
        cop = response.xpath('//a[contains(@id,"icp-touch-link-cop")]//span/text()').extract()
        prices = []
        for price in price_list:
            prices.append(
                {
                    'symbol': cop[0], 
                    'value': float(price.strip('$')), 
                    'currency': cop[1].split()[0], 
                    'raw': price,
                    'name': f'{price}{price}',
                }
            )

        # REVIEW
        review_details = response.xpath('//div[contains(@id,"reviewsMedley")]/div/div')[0]
        rating = review_details.xpath('.//span[contains(@data-hook, "rating-out-of-text")]/text()').extract_first().split()[0]
        ratings_total = review_details.xpath('.//div[contains(@class, "averageStarRatingNumerical")]/span/text()').extract_first().split()[0]
        rating_breakdown_table = review_details.xpath('.//table[@id="histogramTable"]')[1].xpath('./tr')

        rating_breakdown = []
        number_to_star = [ 'five_star', 'four_star', 'three_star', 'two_star', 'one_star' ]
        for index, rating_breakdown_single in enumerate(rating_breakdown_table):
            rating_percentage = rating_breakdown_single.xpath('.//div[contains(@class, "a-meter")]/@aria-valuenow').extract_first()
            rating_breakdown.append(
                {
                    number_to_star[index]: {
                        "percentage": rating_percentage,
                    }
                }
            )

        # IMAGES
        main_image = ''
        # main_image = response.xpath('.//div[@id="imgTagWrapperId"]/img/@src').extract_first()
        # images_list = response.xpath('.//div[@id="imageBlock_feature_div"]//div[contains(@class, "regularImageBlockViewLayout")]//ul//li[contains(@class, "image item")]')
        images_list = response.xpath('.//div[@id="altImages"]//ul/li[contains(@class, "item")]//img/@src').extract()
        # print(images_list)
        images = []
        images_count = len(images_list)
        for index, image in enumerate(images_list):
            try:
                image_parts = image.split('_')
                image_parts.pop(-2)
                og_image_link = '_'.join(image_parts)
                images.append(
                    {'link': og_image_link}
                )
                if index == 0:
                    main_image = og_image_link
            except Exception as e:
                print(f'Exception: {e}')


        # FEATURES
        feature_bullets = response.xpath('.//div[@id="feature-bullets"]/ul/li[not(@*)]/span/text()').extract()
        feature_bullets_count = len(feature_bullets)
        feature_bullets_flat = ', '.join(feature_bullets)
        # print("feature_bullets")
        # print(feature_bullets.extract())

        # SPECIFICATION
        print('specifications')
        specifications = []
        bestsellers_rank = []
        bestsellers_rank_flat = ""
        manufacturer = ""
        specifications_list = response.xpath('.//div[@id="detailBullets_feature_div"]/ul/li')
        if not specifications_list:
            specifications_list = response.xpath('.//div[@id="prodDetails"]//table/tr')
            for specification in specifications_list:
                name = specification.xpath('.//th/text()').extract_first().strip()
                value = specification.xpath('.//td/text()').extract_first().strip()
                if name == "Manufacturer":
                    manufacturer = value
                if name == "Best Sellers Rank":
                    value = '. '.join(specification.xpath('.//td//a/text()').extract())
                    bestsellers_rank_list = specification.xpath('.//td/span/span')
                    for bsr in bestsellers_rank_list:
                        _category = bsr.xpath('./a/text()').extract_first().lstrip('See Top 100 in ') 
                        _rank = bsr.xpath('./text()').extract_first().split()[0].strip('#')
                        _link = f'https://{self.domain}' + bsr.xpath('./a/@href').extract_first()
                        bestsellers_rank.append({
                            'category': _category,
                            'rank': _rank,
                            'link': _link,
                        })
                        bestsellers_rank_flat += f'Category: {_category} | Rank: {_rank} | link: {_link}'
                specifications.append({'name': name, 'value': value})
        else:
            for specification in specifications_list:
                name, value = specification.xpath('./span/span/text()').extract()
                import html2text
                # name = html2text.html2text(name).split(':')[0].strip().encode("ascii", "ignore").decode().strip()
                name = name.split(':')[0].encode("ascii", "ignore").decode().strip()
                value = value.strip()
                if name == "Manufacturer":
                    manufacturer = value
                if name == "Best Sellers Rank":
                    print('this is a best seller--------------------------------------------')
                specifications.append({'name': name, 'value': value})


        item['title'] = title
        item['link'] = link
        item['price'] = prices
        item['asin'] = asin
        item['brand'] = brand
        item['rating'] = rating
        item['ratings_total'] = ratings_total
        item['reviews_total'] = ratings_total
        item['rating_breakdown'] = rating_breakdown
        item['main_image'] = {'link': main_image}
        item['images'] = images
        item['images_count'] = images_count
        item['feature_bullets'] = feature_bullets
        item['feature_bullets_count'] = feature_bullets_count
        item['feature_bullets_flat'] = feature_bullets_flat
        item['specifications'] = specifications
        item['bestsellers_rank'] = bestsellers_rank
        item['bestsellers_rank_flat'] = bestsellers_rank_flat
        item['manufacturer'] = manufacturer
        # print(title, '|', link, '|', prices, '|', asin, '|', brand, '|')
        yield item
