import json
from time import sleep
import scrapy
import urllib
# from amazon_scraper.amazon_scraper.items import AmazonScraperItem, AmazonDepartmentItem
from amazon_scraper.items import AmazonScraperItem, AmazonDepartmentItem
import pandas as pd
DF_ITEMS = []

class ProductSpider(scrapy.Spider):
    name = 'amazon_spider'
    # start_urls = ['https://www.amazon.com/s?i=fashion-novelty&bbn=12035955011&rh=p_6%3AATVPDKIKX0DER&s=featured&hidden-keywords=ORCA', ]
    # start_urls = ['https://www.amazon.com/s/?ie=UTF8&hidden-keywords=ORCA&field-keywords=mens%20tshirt%20winter&bbn=12035955011', ]
    # start_urls = ['https://www.amazon.com/s?k=mens+tshirt+winter&i=fashion-novelty&bbn=12035955011&rh=n%3A7141123011%2Cn%3A7147445011%2Cn%3A12035955011%2Cn%3A9103696011%2Cn%3A9056985011%2Cn%3A9056986011&dc&hidden-keywords=ORCA&qid=1646024756&rnid=7141123011&ref=sr_nr_n_1', ]
    # start_urls = ['https://www.amazon.com/Novelty-Clothing/b/ref=dp_bc_aui_C_4?ie=UTF8&node=9103696011', ]
    # start_urls = ['https://www.amazon.co.jp/s?k=tshirt&i=fashion-mens', ]
    # start_urls = ['https://www.amazon.co.jp/s?k=t-shirt&i=fashion-mens&rh=n%3A2229202051%2Cn%3A2230005051%2Cn%3A2131417051&dc&qid=1645793201&rnid=2229202051&ref=sr_nr_n_1', ]

    # popsocket url
    # start_urls = ['https://www.amazon.com/s?k=halloween+popsocket&i=mobile&rh=p_6%3AATVPDKIKX0DER&s=date-desc-rank&sprefix=%2Cmobile%2C797', ]
    # kdp url
    # start_urls = ['https://www.amazon.com/s?i=stripbooks-intl-ship&s=featured&hidden-keywords=independently+published&ref=nb_sb_noss', ]
    # phonecase url
    # start_urls = ['https://www.amazon.com/s?k=football&i=mobile&rh=p_6%3AATVPDKIKX0DER&s=featured&hidden-keywords=Two-part+protective+case+made+from+a+premium+scratch-resistant+polycarbonate+shell+and+shock+absorbent+TPU+liner+protects+against+drops&sprefix=%2Cmobile%2C797', ]
    # tanktops url
    start_urls = ['https://www.amazon.com/s?k=tank+top+for+women&i=fashion-novelty&rh=p_6%3AATVPDKIKX0DER&s=featured&hidden-keywords=%22Tank+Top%22+%2B+%22Solid+colors%3A+100%25+Cotton%22', ]
    domain = ''
    site_uri = ''

    def parse(self, response, **kwargs):
        self.domain = response.xpath('//a[contains(@class, "nav-progressive-attribute") and contains(@id, "nav-link-accountList")]/@href').extract_first().split('/')[2]
        self.site_uri = f'https://{self.domain}'
        print('DOMAIN:-------------------------', self.domain)
        # departments = []
        # departments_list = response.xpath('//div[@id="departments"]/ul/li[contains(@class, "s-navigation-indent-")]')
        # parent_category_code = ''
        # for index, department in enumerate(departments_list):
        #     adi = AmazonDepartmentItem()
        #     if index == 0:
        #         parent_category_code = department.xpath('@id').extract_first().lstrip('n/')
        #         departments.append({'category': department.xpath('./span/span/text()').extract_first(), 'category_code': parent_category_code})
        #         adi['category'] =  department.xpath('./span/span/text()').extract_first()
        #         adi['category_code'] =  parent_category_code
        #     else:
        #         departments.append({'category': department.xpath('./span/a/span/text()').extract_first(), 'category_code': department.xpath('@id').extract_first().lstrip('n/'), 'parent_category_code': parent_category_code})
        #         adi['category'] =  department.xpath('./span/a/span/text()').extract_first()
        #         adi['category_code'] =  department.xpath('@id').extract_first().lstrip('n/')
        #         adi['category_parent_code'] =  parent_category_code
        #         yield scrapy.Request(self.site_uri + department.xpath('./span/a/@href').extract_first(), callback=self.sub_department_parse)
        #         sleep(3)
        #     yield adi

        products = response.xpath('//div[contains(@class, "s-main-slot s-result-list")]/div[contains(@class, "s-result-item s-asin")]')
        # for product in products:
        for index in range(1,2):

            product = products[index]
            asin = product.xpath('@data-asin').extract_first()
            print('ASIN:::::::::::::::::::::::::::::::::::::::::::::', asin)
            link = self.site_uri + '/dp/' + asin
            print('LINK:::::::::::::::::::::::::::::::::::::::::::::', link)
            if self.domain == 'www.amazon.com':
                print('MARKETPLACE:::::::::::::::::::::::::::::::::::::::::::::', "www.amazon.com")
                yield scrapy.Request(link, callback=self.single_product_parse, cb_kwargs=dict(link= link, asin= asin))
                # yield scrapy.Request(link, callback=self.kdp_product_parse, cb_kwargs=dict(link= link, asin= asin))
            if self.domain == 'www.amazon.co.jp':
                yield scrapy.Request(link, callback=self.jpn_single_product_parse, cb_kwargs=dict(link= link, asin= asin))
            sleep(1)

    def sub_department_parse(self, response):
        departments = []
        departments_list = response.xpath('//div[@id="departments"]/ul/li[contains(@class, "s-navigation-indent-")]')
        # print(departments_list)
        parent_category_code = ''
        for index, department in enumerate(departments_list):
            adi = AmazonDepartmentItem()
            if index == 0:
                parent_category_code = department.xpath('@id').extract_first().lstrip('n/')
                departments.append({'category': department.xpath('./span/span/text()').extract_first(), 'category_code': parent_category_code})
                adi['category'] =  department.xpath('./span/span/text()').extract_first()
                adi['category_code'] =  parent_category_code
            else:
                departments.append({'category': department.xpath('./span/a/span/text()').extract_first(), 'category_code': department.xpath('@id').extract_first().lstrip('n/'), 'parent_category_code': parent_category_code})
                adi['category'] =  department.xpath('./span/a/span/text()').extract_first()
                adi['category_code'] =  department.xpath('@id').extract_first().lstrip('n/')
                adi['category_parent_code'] =  parent_category_code
                yield scrapy.Request(self.site_uri + department.xpath('./span/a/@href').extract_first(), callback=self.sub_department_parse)
                sleep(3)
            yield adi

        print(departments) 

    def single_product_parse(self, response, link=None, asin=None):
        item = AmazonScraperItem()
        title = response.xpath('//span[@id="productTitle"]/text() | //div[contains(@id,"titleSection")]//span/text()').extract_first()
        title = title.strip() if title else title

        # PRICE
        price_list = response.xpath('//div[contains(@id,"corePrice_desktop")]//span[contains(@class, "a-offscreen")]/text() | //div[@id="corePriceDisplay_desktop_feature_div"]//span[contains(@class, "a-offscreen")]/text()').extract()
        brand_string = response.xpath('//a[contains(@id,"bylineInfo")]/text()').extract_first()
        brand = None
        if brand_string:
            brand = brand_string.split(':')[1].strip() if brand_string.find('Brand:') == 0 else brand_string.replace('Visit the ', '') if brand_string.find('Visit the ') == 0 else brand_string
        prices = []
        if price_list:
            print('----1')
            for price in price_list:
                prices.append(
                    {
                        'symbol': price[0], 
                        'value': float(price[1:].replace(',','')), 
                        'currency': 'USD', 
                        'raw': price[1:],
                        'name': f'{price}{price}',
                    }
                )

        # REVIEW
        review_details = response.xpath('//div[contains(@id,"reviewsMedley")]/div/div')
        rating = ''
        ratings_total = 0
        rating_breakdown = []
        if review_details:
            review_details = review_details[0]
            rating = review_details.xpath('.//span[contains(@data-hook, "rating-out-of-text")]/text()').extract_first()
            if rating:
                rating = float(rating.split()[0])
                ratings_total = int(review_details.xpath('.//div[contains(@class, "averageStarRatingNumerical")]/span/text()').extract_first().split()[0].replace(',',''))
                rating_breakdown_table = review_details.xpath('.//table[@id="histogramTable"]')[1].xpath('./tr')

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
        print(categories_list.extract())
        categories = []
        for category in categories_list:
            try:
                c_name = category.xpath('text()').extract_first().strip()
                c_link = category.xpath('@href').extract_first()
                print('ID: ', urllib.parse.parse_qs(c_link.split('?')[1]))
                c_id = urllib.parse.parse_qs(c_link.split('?')[1])['node'][0]
                if c_id.isnumeric():
                    categories.append({
                        'name': c_name,
                        'link': self.site_uri + c_link,
                        'category_id': c_id
                    })
            except Exception as e:
                print("CategoriesNotFound:", e)

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

        # VARIATION: MODEL NAME
        model_name_variations_list = response.xpath('.//div[@id="variation_model_name"]/ul/li')
        # print("VARIATIONS_LIST_________________________________________________________")
        # print(fit_type_variations_list.extract())
        for variation in model_name_variations_list:
            _asin = variation.xpath('@data-defaultasin').extract_first()
            variations.append({
                "asin": _asin,
                "title": variation.xpath('@title').extract_first().replace('Click to select ', ''),
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

    def kdp_product_parse(self, response, link=None, asin=None):
        item = AmazonScraperItem()
        title = response.xpath('//span[@id="productTitle"]/text() | //div[contains(@id,"titleSection")]//span/text()').extract_first()
        title = title.strip() if title else title

        # PRICE
        brand = response.xpath('//div[contains(@id,"bylineInfo")]//a[@data-asin]/text()').extract_first()
        price_list = response.xpath('//div[@id="tmmSwatches"]//ul/li[contains(@class, " selected")]//a')
        print(price_list.extract())
        prices = []
        if price_list:
            for price in price_list:
                try:
                    # print(price.xpath('.//span[1]/text()').extract())
                    dump_price = price.xpath('.//span[1]/text()').extract()
                    if ''.join(dump_price).strip().find('Audiobook') != -1 :
                        _name = dump_price[5]
                        _price = dump_price[9].strip()
                        # print('name:', _name, 'price:', _price)
                        prices.append(
                            {
                                'symbol': _price[0], 
                                'value': float(_price[1:].replace(',','')), 
                                'currency': 'USD', 
                                'raw': _price[1:],
                                'name': f'{_name}',
                            }
                        )
                    elif ''.join(dump_price).strip().find('$') != -1 :
                        _name = dump_price[0]
                        _price = dump_price[1].strip()
                        # print('name:', _name, 'price:', _price)
                        prices.append(
                            {
                                'symbol': _price[0], 
                                'value': float(_price[1:].replace(',','')), 
                                'currency': 'USD', 
                                'raw': _price[1:],
                                'name': f'{_name}',
                            }
                        )
                except Exception as e:
                    print(e)

        # REVIEW
        review_details = response.xpath('//div[contains(@id,"reviewsMedley")]/div/div')
        rating = ''
        ratings_total = 0
        rating_breakdown = []
        if review_details:
            review_details = review_details[0]
            rating = review_details.xpath('.//span[contains(@data-hook, "rating-out-of-text")]/text()').extract_first()
            if rating:
                rating = float(rating.split()[0])
                ratings_total = int(review_details.xpath('.//div[contains(@class, "averageStarRatingNumerical")]/span/text()').extract_first().split()[0].replace(',',''))
                rating_breakdown_table = review_details.xpath('.//table[@id="histogramTable"]')[1].xpath('./tr')

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
        images_list = response.xpath('.//div[@id="imageBlockThumbs"]//img/@src | .//div[@id="imageBlockContainer"]//img[2]/@src | .//img[@id="ebooksImgBlkFront"]/@src').extract()
        print(images_list)
        images = []
        images_count = len(images_list)
        if images_list:
            for index, image in enumerate(images_list):
                try:
                    print('IMAGE:', image)
                    print('IMAGE:', image.split('_'))
                    image_l = image.split('_')
                    image_parts = [image_l[0].rstrip('.'), image_l[-1]]
                    # image_parts = image.split(',')[0] + image.split(',')[-1]
                    # image_parts = image_parts.split('_')
                    # image_parts.pop(-2)
                    # image_parts.pop(-2)
                    # og_image_link = '_'.join(image_parts)
                    og_image_link = ''.join(image_parts)
                    images.append(
                        {'link': og_image_link}
                    )
                    if index == 0:
                        main_image = og_image_link
                except Exception as e:
                    print(f'Exception: {e}')


        # FEATURES
        # feature_bullets = response.xpath('.//div[@data-a-expander-name="book_description_expander"]//text()').extract()
        # feature_bullets_count = 0
        # feature_bullets_flat = ""
        # if feature_bullets:
        #     feature_bullets = list(filter(lambda x: len(x.strip()) != 0 , feature_bullets))[0:-1]
        #     feature_bullets_count = len(feature_bullets)
        #     feature_bullets_flat = ', '.join(feature_bullets)

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
        # item['feature_bullets'] = feature_bullets
        # item['feature_bullets_count'] = feature_bullets_count
        # item['feature_bullets_flat'] = feature_bullets_flat
        item['specifications'] = specifications
        item['bestsellers_rank'] = bestsellers_rank
        item['bestsellers_rank_flat'] = bestsellers_rank_flat
        item['manufacturer'] = manufacturer
        # item['categories'] = categories
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
        review_details = response.xpath('//div[contains(@id,"reviewsMedley")]/div/div')
        print("review_details---------------")
        print(review_details.extract())
        rating = ''
        ratings_total = 0
        rating_breakdown = []
        if review_details:
            review_details = review_details[0]
            rating = review_details.xpath('.//span[contains(@data-hook, "rating-out-of-text")]/text()').extract_first()
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
            try:
                c_name = category.xpath('text()').extract_first().strip()
                c_link = category.xpath('@href').extract_first()
                c_id = urllib.parse(c_link.split('?')[1])['node']
                if c_id.isnumeric():
                    categories.append({
                        'name': c_name,
                        'link': self.site_uri + c_link,
                        'category_id': c_id
                    })
            except Exception as e:
                print(e)

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