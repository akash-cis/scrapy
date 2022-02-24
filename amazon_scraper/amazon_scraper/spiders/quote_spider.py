import scrapy

class QuoteSpider(scrapy.Spider):
    name = 'quotes'
    start_urls = ['http://quotes.toscrape.com', ]

    def parse(self, response, **kwargs):
        title = response.css('title::text').extract()
        yield {'title': title}

        return super().parse(response, **kwargs)