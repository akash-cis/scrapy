from time import sleep
import scrapy
from amazon_scraper.items import AmazonScraperItem

class ProductSpider(scrapy.Spider):
    name = 'amazon_spider'
    # start_urls = ['https://www.amazon.com/s?i=fashion-novelty&bbn=12035955011&rh=p_6%3AATVPDKIKX0DER&s=featured&hidden-keywords=ORCA', ]
    # start_urls = ['https://www.amazon.com/s/?ie=UTF8&hidden-keywords=ORCA&field-keywords=mens%20tshirt%20winter&bbn=12035955011', ]
    start_urls = ['https://www.amazon.com/s?k=mens+tshirt+winter&i=fashion-novelty&bbn=12035955011&rh=n%3A7141123011%2Cn%3A7147445011%2Cn%3A12035955011%2Cn%3A9103696011%2Cn%3A9056985011&dc&hidden-keywords=ORCA&qid=1645791111&rnid=7141123011&ref=sr_nr_n_2', ]
    # start_urls = ['https://www.amazon.co.jp/s?k=tshirt&i=fashion-mens', ]
    # start_urls = ['https://www.amazon.co.jp/s?k=t-shirt&i=fashion-mens&rh=n%3A2229202051%2Cn%3A2230005051%2Cn%3A2131417051&dc&qid=1645793201&rnid=2229202051&ref=sr_nr_n_1', ]
    domain = ''
    site_uri = ''

    def parse(self, response, **kwargs):
        # products = response.xpath('//div[contains(@class, "nav-flyout-anchor")]/div[contains(@data-nav-timeline-item, "s-result-item s-asin")]')
        self.domain = response.xpath('//a[contains(@class, "nav-progressive-attribute") and contains(@id, "nav-link-accountList")]/@href').extract_first().split('/')[2]
        self.site_uri = f'https://{self.domain}'
        print('DOMAIN:-------------------------', self.domain)
        products = response.xpath('//div[contains(@class, "s-main-slot s-result-list")]/div[contains(@class, "s-result-item s-asin")]')
        # for product in products:
        for index in range(0,2):

            product = products[index]
            asin = product.xpath('@data-asin').extract_first()
            print('ASIN:::::::::::::::::::::::::::::::::::::::::::::', asin)
            link = self.site_uri + '/dp/' + asin
            print('LINK:::::::::::::::::::::::::::::::::::::::::::::', link)
            # link_short = f'{self.site_uri}/{link[2]}/{link[3]}' if len(link[1]) > 2 else f'{self.site_uri}/{link[1]}/{link[2]}'
            # yield scrapy.Request(link_short, callback=self.single_product_parse, cb_kwargs=dict(link= link_short, asin= asin))
            if self.domain == 'www.amazon.com':
                yield scrapy.Request(link, callback=self.single_product_parse, cb_kwargs=dict(link= link, asin= asin))
            if self.domain == 'www.amazon.co.jp':
                yield scrapy.Request(link, callback=self.jpn_single_product_parse, cb_kwargs=dict(link= link, asin= asin))
            sleep(5)


    def single_product_parse(self, response, link=None, asin=None):
        item = AmazonScraperItem()
        title = response.xpath('//div[contains(@id,"titleSection")]//span/text()').extract_first().strip()

        # PRICE
        price_list = response.xpath('//div[contains(@id,"corePrice_desktop")]//span[contains(@class, "a-offscreen")]/text()').extract()
        brand_string = response.xpath('//a[contains(@id,"bylineInfo")]/text()').extract_first()
        brand = brand_string.split(':')[1].strip() if brand_string.find('Brand:') == 0 else brand_string.replace('Visit the ', '') if brand_string.find('Visit the ') == 0 else brand_string
        cop = []
        cop = response.xpath('//a[contains(@id,"icp-touch-link-cop")]//span/text()').extract()
        prices = []
        for price in price_list:
            prices.append(
                {
                    'symbol': cop[0], 
                    'value': float(price[1:].replace(',','')), 
                    'currency': cop[1].split()[0], 
                    'raw': price,
                    'name': f'{price}{price}',
                }
            )

        # REVIEW
        review_details = response.xpath('//div[contains(@id,"reviewsMedley")]/div/div')[0]
        # rating = float(review_details.xpath('.//span[contains(@data-hook, "rating-out-of-text")]/text()').extract_first().split()[0])
        rating = review_details.xpath('.//span[contains(@data-hook, "rating-out-of-text")]/text()').extract_first()
        if rating:
            rating = float(rating.split()[0])
            ratings_total = int(review_details.xpath('.//div[contains(@class, "averageStarRatingNumerical")]/span/text()').extract_first().split()[0].replace(',',''))
            rating_breakdown_table = review_details.xpath('.//table[@id="histogramTable"]')[1].xpath('./tr')

            rating_breakdown = []
            number_to_star = [ 'five_star', 'four_star', 'three_star', 'two_star', 'one_star' ]
            for index, rating_breakdown_single in enumerate(rating_breakdown_table):
                rating_percentage = rating_breakdown_single.xpath('.//div[contains(@class, "a-meter")]/@aria-valuenow').extract_first()
                rating_breakdown.append(
                    {
                        number_to_star[index]: {
                            "percentage": int(rating_percentage.rstrip('%')),
                            "count": round(ratings_total * int(rating_percentage.rstrip('%')) / 100)
                        }
                    }
                )

        # IMAGES
        main_image = ''
        images_list = response.xpath('.//div[@id="altImages"]//ul/li[contains(@class, "item")]//img/@src').extract()
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

        # SPECIFICATION
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
                        _rank = int(bsr.xpath('./text()').extract_first().split()[0].replace(',','').strip('#'))
                        _link = self.site_uri + bsr.xpath('./a/@href').extract_first()
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
                name = name.split(':')[0].encode("ascii", "ignore").decode().strip()
                value = value.strip()
                specifications.append({'name': name, 'value': value})
                if name == "Manufacturer":
                    manufacturer = value
            bestsellers_rank_l = response.xpath('.//div[@id="detailBullets_feature_div"]/following-sibling::ul[1]/li/span//a')
            if bestsellers_rank_l:
                for bsr in bestsellers_rank_l:
                    _category = bsr.xpath('text()').extract_first().lstrip('See Top 100 in ')
                    _rank = bsr.xpath('../text()').extract()
                    _rank = int(_rank[0].split()[0].strip('#').replace(',', '')) if _rank[0].strip() != '' else int(_rank[1].split()[0].strip('#').replace(',', ''))
                    _link = self.site_uri + bsr.xpath('@href').extract_first()
                    bestsellers_rank.append({
                        'category': _category,
                        'rank': _rank,
                        'link': _link,
                    })
                    bestsellers_rank_flat += f'Category: {_category} | Rank: {_rank} | link: {_link}'
                specifications.append({'name': "Best Seller Rank", 'value': list(map(lambda x: x['category'], bestsellers_rank))})

        # CATEGORIES
        categories_list = response.xpath('.//div[@id="wayfinding-breadcrumbs_container"]//ul//a')
        categories = []
        for category in categories_list:
            c_name = category.xpath('text()').extract_first().strip()
            c_link = category.xpath('@href').extract_first()
            c_id = c_link.split('=')[-1]
            # if c_id.isnumeric():
            categories.append({
                'name': c_name,
                'link': self.site_uri + c_link,
                'category_id': c_id
            })

        variations = []
        # VARIATION: SIZE
        size_variations_list = response.xpath('.//div[@id="variation_size_name"]//select/option[position()>1]')
        # print("VARIATIONS_LIST_________________________________________________________")
        # print(size_variations_list.extract())
        for variation in size_variations_list:
            _asin = variation.xpath('@value').extract_first().split(',')[1]
            variations.append({
                "asin": _asin,
                "title": variation.xpath('@data-a-html-content').extract_first(),
                "is_current_product": False,
                "link": self.site_uri + '/dp/' + _asin,
            })

        # VARIATION: COLOR
        color_variations_list = response.xpath('.//div[@id="variation_color_name"]/ul/li')
        # print("VARIATIONS_LIST_________________________________________________________")
        # print(color_variations_list.extract())
        for variation in color_variations_list:
            _asin = variation.xpath('@data-defaultasin').extract_first()
            variations.append({
                "asin": _asin,
                "title": variation.xpath('@title').extract_first().replace('Click to select ', ''),
                "is_current_product": True if variation.xpath('@data-defaultasin').extract_first() == asin else False,
                "link": self.site_uri + '/dp/' + _asin,
            })
        
        # VARIATION: FIT
        fit_type_variations_list = response.xpath('.//div[@id="variation_fit_type"]/ul/li')
        # print("VARIATIONS_LIST_________________________________________________________")
        # print(fit_type_variations_list.extract())
        for variation in fit_type_variations_list:
            _asin = variation.xpath('@data-defaultasin').extract_first()
            variations.append({
                "asin": _asin,
                "title": variation.xpath('@title').extract_first().strip('Click to select'),
                "is_current_product": True if variation.xpath('@data-defaultasin').extract_first() == asin else False,
                "link": self.site_uri + '/dp/' + _asin,
            })
        # print(variations)


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
        item['categories'] = categories
        item['variations'] = variations
        yield item

    def jpn_single_product_parse(self, response, link=None, asin=None):
        item = AmazonScraperItem()
        title = response.xpath('//div[contains(@id,"titleSection")]//span/text()').extract_first().strip()
        print(title)

        # PRICE
        price_list = response.xpath('//div[contains(@id,"corePrice_desktop")]//span[contains(@class, "a-offscreen")]/text()').extract()
        brand_string = response.xpath('//a[contains(@id,"bylineInfo")]/text()').extract_first()
        brand = brand_string.split(':')[1].strip() if brand_string.find('Brand:') == 0 else brand_string.replace('Visit the ', '') if brand_string.find('Visit the ') == 0 else brand_string
        prices = []
        for price in price_list:
            prices.append(
                {
                    'symbol': price[0], 
                    'value': float(price[1:].replace(',','')), 
                    'currency': 'JPY', 
                    'raw': price[1:],
                    'name': f'{price}{price}',
                }
            )

        # REVIEW
        review_details = response.xpath('//div[contains(@id,"reviewsMedley")]/div/div')[0]
        rating = review_details.xpath('.//span[contains(@data-hook, "rating-out-of-text")]/text()').extract_first()
        ratings_total = 0
        rating_breakdown = []
        if rating:
            try:
                rating = float(rating.encode('ascii', "ignore").decode()[1:])
            except:
                rating = rating
            ratings_total = int(review_details.xpath('.//div[contains(@class, "averageStarRatingNumerical")]/span/text()').extract_first().split()[0].replace(',',''))
            rating_breakdown_table = review_details.xpath('.//table[@id="histogramTable"]')[1].xpath('./tr')

            rating_breakdown = []
            number_to_star = [ 'five_star', 'four_star', 'three_star', 'two_star', 'one_star' ]
            for index, rating_breakdown_single in enumerate(rating_breakdown_table):
                rating_percentage = rating_breakdown_single.xpath('.//div[contains(@class, "a-meter")]/@aria-valuenow').extract_first()
                rating_breakdown.append(
                    {
                        number_to_star[index]: {
                            "percentage": int(rating_percentage.rstrip('%')),
                            "count": round(ratings_total * int(rating_percentage.rstrip('%')) / 100)
                        }
                    }
                )

        # IMAGES
        main_image = ''
        images_list = response.xpath('.//div[@id="altImages"]//ul/li[contains(@class, "item")]//img/@src').extract()
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

        # SPECIFICATION
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
                        _rank = int(bsr.xpath('./text()').extract_first().split()[0].replace(',','').strip('#'))
                        _link = self.site_uri + bsr.xpath('./a/@href').extract_first()
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
                name = name.split(':')[0].strip()
                value = value.strip()
                specifications.append({'name': name, 'value': value})
                if name == "Manufacturer":
                    manufacturer = value
            bestsellers_rank_l = response.xpath('.//div[@id="detailBullets_feature_div"]/following-sibling::ul[1]/li/span//a')
            if bestsellers_rank_l:
                for bsr in bestsellers_rank_l:
                    _category = bsr.xpath('text()').extract_first().lstrip('See Top 100 in ')
                    _rank = bsr.xpath('../text()').extract()
                    _rank = int(_rank[0].encode("ascii", "ignore").decode().strip('-() ')) if _rank[0].strip() != '' else int(_rank[1].encode("ascii", "ignore").decode().strip('-() '))
                    _link = self.site_uri + bsr.xpath('@href').extract_first()
                    bestsellers_rank.append({
                        'category': _category,
                        'rank': _rank,
                        'link': _link,
                    })
                    bestsellers_rank_flat += f'Category: {_category} | Rank: {_rank} | link: {_link}'
                specifications.append({'name': "Best Seller Rank", 'value': list(map(lambda x: x['category'], bestsellers_rank))})


        # CATEGORIES
        categories_list = response.xpath('.//div[@id="wayfinding-breadcrumbs_container"]//ul//a')
        # print("||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
        # print(categories_list.extract())
        categories = []
        for category in categories_list:
            c_name = category.xpath('text()').extract_first().strip()
            c_link = category.xpath('@href').extract_first()
            c_id = c_link.split('=')[-1]
            if c_id.isnumeric():
                categories.append({
                    'name': c_name,
                    'link': self.site_uri + c_link,
                    'category_id': c_id
                })

        variations = []
        # VARIATION: SIZE
        size_variations_list = response.xpath('.//div[@id="variation_size_name"]//select/option[position()>1]')
        # print("VARIATIONS_LIST_________________________________________________________")
        # print(size_variations_list.extract())
        if size_variations_list:
            for variation in size_variations_list:
                _asin = variation.xpath('@value').extract_first().split(',')[1]
                variations.append({
                    "asin": _asin,
                    "title": variation.xpath('@data-a-html-content').extract_first(),
                    "is_current_product": False,
                    "link": self.site_uri + '/dp/' + _asin,
                })

        # # VARIATION: COLOR
        color_variations_list = response.xpath('.//div[@id="variation_color_name"]/ul/li')
        # print("VARIATIONS_LIST_________________________________________________________")
        # print(color_variations_list.extract())
        if color_variations_list:
            for variation in color_variations_list:
                _asin = variation.xpath('@data-defaultasin').extract_first()
                variations.append({
                    "asin": _asin,
                    "title": variation.xpath('@title').extract_first().replace('Click to select ', ''),
                    "is_current_product": True if variation.xpath('@data-defaultasin').extract_first() == asin else False,
                    "link": self.site_uri + '/dp/' + _asin,
                })
        
        # # VARIATION: FIT
        fit_type_variations_list = response.xpath('.//div[@id="variation_fit_type"]/ul/li')
        # print("FITS_VARIATIONS_LIST_________________________________________________________")
        # print(fit_type_variations_list.extract())
        if fit_type_variations_list:
            for variation in fit_type_variations_list:
                _asin = variation.xpath('@data-defaultasin').extract_first()
                variations.append({
                    "asin": _asin,
                    "title": variation.xpath('@title').extract_first().strip('Click to select'),
                    "is_current_product": True if variation.xpath('@data-defaultasin').extract_first() == asin else False,
                    "link": self.site_uri + '/dp/' + _asin,
                })
        # print(variations)


        item['title'] = title
        item['link'] = link
        item['asin'] = asin
        item['price'] = prices
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
        item['categories'] = categories
        item['variations'] = variations
        yield item