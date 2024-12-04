import requests
import json
import os
import random
import time
from datetime import datetime, timedelta
from typing import Iterable
from meesho_des.items import *
import pymysql
import scrapy
from scrapy import Request, Spider,Selector
from scrapy.cmdline import execute
import meesho_des.db_config as db
from PIL import ImageFile
import gzip



class ProductDataSpider(scrapy.Spider):
    name = "product_data_des"
    handle_httpstatus_list = [403, 429, 400]
    # folder_location = f"D:/Meesho/meesho_des"
    folder_location = f"C:/Meesho/meesho_des/pdp_page_save/"
    errors = list()

    DATE = str(datetime.now().strftime("%Y%m%d"))

    def __init__(self, name=None, start='', end='', **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = start
        self.end = end

        # Create folder if not exits
        os.makedirs(self.folder_location, exist_ok=True)

        # DATABASE CONNECTION
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, db=db.db_name)
        self.cursor = self.con.cursor()
        self.con.autocommit(True)

    def start_requests(self):
        query = f"select `product_url` FROM {db.db_links_table} where status='pending' and id between {self.start} and {self.end};"
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        self.logger.info(f"\n\n\nTotal Results ...{len(query_results)}\n\n\n", )

        for i in query_results:
            if not i[0]:
                continue
            url = i[0].strip()
            sku = url.split('/')[-1]

            if not os.path.exists(self.folder_location + sku + ".html.gz"):
                # url = "https://www.meesho.com/s/p/" + sku

                yield scrapy.Request(
                    url=url,
                    cb_kwargs={
                        "page_name": sku,
                        "url":url,
                        "page_read": False
                    },
                    meta={"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])},
                )
            else:
                yield scrapy.Request(url=f'file:///{self.folder_location + sku + ".html.gz"}',
                                     cb_kwargs={
                                         "page_name": i,
                                         "url":url,
                                         "page_read": True
                                     },
                                     meta={"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])})
            # break

    def parse(self, response, **kwargs):
        sku = kwargs['page_name']
        url = kwargs['url']
        if response.status in [403, 429]:
            request = response.request.copy()
            request.meta.update({"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])})
            request.dont_filter = True
            yield request
            return None
        if kwargs['page_read']:
            response = gzip.decompress(response.body)
            data = response.split(b'id="__NEXT_DATA__" type="application/json">')[1].split(b"</script>")[0]
        else:
            data = response.body.split(b'id="__NEXT_DATA__" type="application/json">')[1].split(b"</script>")[0]
            file_path = self.folder_location + sku + f".html.gz"
            # Writing the Gzip file safely
            with gzip.open(file_path, "wb") as f:
                f.write(response.body)

        # response = Selector(text=response_text)
        status = "pending"
        data = json.loads(data)['props']['pageProps']['initialState']['product']['details']['data']
        if data:
            if data['valid']:
                if 'suppliers' not in data:
                    return None
                supplier = data['suppliers'][0]
                supplier_id = str(supplier['id'])
                SKU_URL_MEESHO = data['meta_info']['canonical_url']
                product_price = 0
                status = "Done"
                product_price = data['price']
                mrp = data['price']
                if 'mrp_details' in data:
                    mrp = data['mrp_details']['mrp']
                discount = round((1 - (product_price / mrp)) * 100)

                rating_count_map = data['review_summary']['data']['rating_count_map']
                if rating_count_map:
                    # Mapping of keys to labels
                    key_mapping = {
                        "1": "Excellent",
                        "2": "Very Good",
                        "3": "Good",
                        "4": "Average",
                        "5": "Poor"
                    }
                    # Replace keys with labels
                    rating_count_map = {key_mapping[key]: value for key, value in rating_count_map.items()}

                seller_handle = data['suppliers'][0]['handle']
                seller_url = f'https://www.meesho.com/{seller_handle}?ms=2'
                # ['props']['pageProps']['initialState']['product']['details']['data']

                item = MeeshoDesItem()
                item['Product_Sku_Id'] = data['product_id']
                item['Product_Sku_Name'] = data['name'].strip().strip("\\")
                item['Product_Sku_Url'] = SKU_URL_MEESHO
                item['Product_Images_Urls'] = ' | '.join(data['images'])
                item['Product_Count_of_Images'] = len(data['images'])
                item['Product_Avg_Rating'] = data['review_summary']['data']['average_rating']
                item['Product_Rating_Count'] = data['review_summary']['data']['rating_count']
                item['Product_Review_Count'] = data['review_summary']['data']['review_count']
                item['Product_Rating_Count_Map'] = str(rating_count_map)
                item['Product_Size'] = ' | '.join(data['variations'])
                item['Product_Details'] = data['description']
                item['Product_Delivery_Charges'] = data['shipping']['charges']
                item['Product_Discount_Percent'] = discount
                item['Product_Display_Price_On_PDP_Price_After_Discount'] = product_price
                item['Product_In_Stock_Status'] = str(data['in_stock']).lower()
                item['Product_Mrp'] = mrp
                item['Seller_Id'] = supplier_id
                item['Seller_Name'] = data['supplier_name']
                item['Seller_Url'] = seller_url
                item['Seller_Avg_Rating'] = data['suppliers'][0]['average_rating']
                item['Seller_Rating_Count'] = data['suppliers'][0]['rating_count']
                item['Seller_Followers_Count'] = data['suppliers'][0]['shop_value_props'][1]['follower']['count']
                item['Seller_Product_Count'] = data['suppliers'][0]['shop_value_props'][2]['product']['count']
                item['Product_Delivery_Date'] = ''
                item['Product_Pincode'] = ''
                item['Product_City'] = ''
                item['Product_No_Delivery_Days_from_Scrape_Date'] = ''
                item['Product_Price_in_Cart__After_Applying_Coupon'] = float(data['shipping']['charges']) + float(product_price)
                print(item)
                yield item
            else:
                status = "This product is out of stock"
                update_query = f"update {db.db_links_table} set `status` = '{status}' where `product_url` = '{url}'"
                self.cursor.execute(update_query)
                self.con.commit()
        else:
            status = "Not Found page"
            update_query = f"update {db.db_links_table} set `status` = '{status}' where `product_url` = '{url}'"
            self.cursor.execute(update_query)
            self.con.commit()


if __name__ == '__main__':
    execute("scrapy crawl product_data_des -a start=1 -a end=1".split())
