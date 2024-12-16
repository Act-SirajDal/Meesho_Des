import json
import os
import random
from datetime import datetime, timedelta
from typing import Union
from scrapy import Spider
from twisted.internet.defer import Deferred
from meesho_des.items import *
import pymysql
import scrapy
from scrapy.cmdline import execute
import meesho_des.db_config as db
import gzip
from meesho_des.file_genration import file_generate


class ProductDataSpider(scrapy.Spider):
    name = "product_data_des"
    handle_httpstatus_list = [403, 429, 400]
    # folder_location = f"D:/Meesho/meesho_des"
    # folder_location = f"C:/Meesho/meesho_des/pdp_page_save/"
    errors = list()

    DATE = str(datetime.now().strftime("%Y%m%d"))
    # DATE = '20241206'
    folder_location = f"C:/Meesho/meesho_des/pdp_page_save/{DATE}/"

    pxs = [
        "185.188.76.152",
        "104.249.0.116",
        "185.207.96.76",
        "185.205.197.4",
        "185.199.117.103",
        "185.193.74.119",
        "185.188.79.150",
        "185.195.223.146",
        "181.177.78.203",
        "185.207.98.115",
        "186.179.10.253",
        "185.196.189.131",
        "185.205.199.143",
        "185.195.222.22",
        "186.179.20.88",
        "185.188.79.126",
        "185.195.213.198",
        "185.207.98.192",
        "186.179.27.166",
        "181.177.73.165",
        "181.177.64.160",
        "104.233.53.55",
        "185.205.197.152",
        "185.207.98.200",
        "67.227.124.192",
        "104.249.3.200",
        "104.239.114.248",
        "181.177.67.28",
        "185.193.74.7",
        "216.10.5.35",
        "104.233.55.126",
        "185.195.214.89",
        "216.10.1.63",
        "104.249.1.161",
        "186.179.27.91",
        "185.193.75.26",
        "185.195.220.100",
        "185.205.196.226",
        "185.195.221.9",
        "199.168.120.156",
        "181.177.69.174",
        "185.207.98.8",
        "185.195.212.240",
        "186.179.25.90",
        "199.168.121.162",
        "185.199.119.243",
        "181.177.73.168",
        "199.168.121.239",
        "185.195.214.176",
        "181.177.71.233",
        "104.233.55.230",
        "104.249.6.234",
        "104.249.3.87",
        "67.227.125.5",
        "104.249.2.53",
        "181.177.64.15",
        "104.249.7.79",
        "186.179.4.120",
        "67.227.120.39",
        "181.177.68.19",
        "186.179.12.120",
        "104.233.52.54",
        "104.239.117.252",
        "181.177.77.65",
        "185.195.223.56",
        "185.207.99.39",
        "104.249.7.103",
        "185.207.99.11",
        "186.179.3.220",
        "181.177.72.117",
        "185.205.196.180",
        "104.249.2.172",
        "185.207.98.181",
        "185.205.196.255",
        "104.239.113.239",
        "216.10.1.94",
        "181.177.77.2",
        "104.249.6.84",
        "104.239.115.50",
        "185.199.118.209",
        "104.233.55.92",
        "185.207.99.117",
        "104.233.54.71",
        "185.199.119.25",
        "181.177.78.82",
        "104.239.113.76",
        "216.10.7.90",
        "181.177.78.202",
        "104.239.119.189",
        "181.177.64.245",
        "185.199.118.216",
        "185.199.116.219",
        "185.188.77.64",
        "185.199.116.185",
        "185.188.78.176",
        "186.179.12.162",
        "185.205.197.193",
        "181.177.74.161",
        "67.227.126.121",
        "181.177.79.185",

    ]

    proxies = [
        f"http://kunal_santani577-9elgt:QyqTV6XOSp@{random.choice(pxs)}:3199",
        "http://9dbe950ef6284a5da9e7749db9f7cbd1:@api.zyte.com:8011/",
        "http://scraperapi:de51e4aafe704395654a32ba0a14494d@proxy-server.scraperapi.com:8001",
    ]

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
        query = f"select * FROM {db.db_links_table} where status='pending' and id between {self.start} and {self.end};"
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        self.logger.info(f"\n\n\nTotal Results ...{len(query_results)}\n\n\n", )

        for i in query_results:
            id = i[0]
            Supercategory_Name = i[1]
            Category_name = i[2]
            Subcategory_Name = i[3]
            category_URL = i[4]
            product_link = i[6]
            if not product_link:
                continue
            product_url = product_link.strip()
            sku = product_url.split('/')[-1].replace('?utm_source=s','')

            if not os.path.exists(self.folder_location + sku + ".html.gz"):
                url = "https://www.meesho.com/s/p/" + sku

                yield scrapy.Request(
                    url=url,
                    cb_kwargs={
                        "page_name": sku,
                        "product_url":product_url,
                        "page_read": False,
                        "Supercategory_Name":Supercategory_Name,
                        "Category_name":Category_name,
                        "Subcategory_Name":Subcategory_Name,
                        "category_URL" : category_URL
                    },
                    meta={"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])},
                )
            else:
                yield scrapy.Request(url=f'file:///{self.folder_location + sku + ".html.gz"}',
                                     cb_kwargs={
                                         "page_name": sku,
                                         "product_url": product_url,
                                         "page_read": True,
                                         "Supercategory_Name": Supercategory_Name,
                                         "Category_name": Category_name,
                                         "Subcategory_Name": Subcategory_Name,
                                         "category_URL": category_URL
                                     },
                                     meta={"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])})
            # break

    def parse(self, response, **kwargs):
        sku = kwargs['page_name']
        product_url = kwargs['product_url']
        Supercategory_Name = kwargs['Supercategory_Name']
        Category_name = kwargs['Category_name']
        Subcategory_Name = kwargs['Subcategory_Name']
        category_URL = kwargs['category_URL']
        if response.status in [403, 429]:
            # Copy the request for retry
            request = response.request.copy()
            # Rotate proxy
            new_proxy = random.choice(self.proxies)
            request.meta['proxy'] = new_proxy
            self.logger.info(f"Retrying with new proxy: {new_proxy}")
            # Rotate user agent (impersonate)
            request.meta.update({"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])})
            request.dont_filter = True  # Avoid Scrapy duplicate request filtering
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
                try:
                    mrp = data['mrp_details']['mrp']
                except:
                    mrp = "N/A"
                try:
                    original_price = data['original_price']
                except:
                    original_price = "N/A"
                try:
                    discounted_price = data['price']
                except:
                    discounted_price = "N/A"
                try:
                    discount_percentage = data['discount']
                except:
                    discount_percentage = "N/A"

                if not original_price and discounted_price:
                    original_price = discounted_price
                elif mrp and not original_price and not discounted_price:
                    original_price = mrp
                    discounted_price = mrp

                try:
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
                except Exception as e:
                    print(e)
                    rating_count_map = "N/A"

                try:
                    Product_Avg_Rating = data['review_summary']['data']['average_rating']
                except:
                    Product_Avg_Rating = "N/A"

                try:
                    Product_Rating_Count = data['review_summary']['data']['rating_count']
                except:
                    Product_Rating_Count = "N/A"
                try:
                    Product_Review_Count = data['review_summary']['data']['rating_count']
                except:
                    Product_Review_Count = "N/A"
                try:
                    Seller_Avg_Rating = data['suppliers'][0]['average_rating']
                except:
                    Seller_Avg_Rating = "N/A"
                try:
                    Seller_Rating_Count = data['suppliers'][0]['rating_count']
                except:
                    Seller_Rating_Count = "N/A"
                try:
                    Seller_Followers_Count = data['suppliers'][0]['shop_value_props'][1]['follower']['count']
                except:
                    Seller_Followers_Count = "N/A"
                try:
                    Seller_Product_Count = data['suppliers'][0]['shop_value_props'][2]['product']['count']
                except:
                    Seller_Product_Count = "N/A"


                seller_handle = data['suppliers'][0]['handle']
                seller_url = f'https://www.meesho.com/{seller_handle}?ms=2'
                # ['props']['pageProps']['initialState']['product']['details']['data']

                inventory_list = data['suppliers'][0]['inventory']
                if inventory_list:
                    size_variant = []
                    for i in range(len(inventory_list)):
                        size = inventory_list[i]['variation']['name']
                        size_variant.append(size)
                    size_variant = ' | '.join(size_variant)
                else:
                    size_variant = ' | '.join(data['variations'])


                item = MeeshoDesItem()
                item['Product_Sku_Id'] = data['product_id']
                item['Product_Sku_Name'] = data['name'].strip().strip("\\")
                item['Product_Sku_Url'] = SKU_URL_MEESHO
                item['Product_Images_Urls'] = ' | '.join(data['images'])
                item['Product_Count_of_Images'] = len(data['images'])
                item['Product_Rating'] = Product_Avg_Rating
                item['Product_Rating_Count'] = Product_Rating_Count
                item['Product_Review_Count'] = Product_Review_Count
                item['Product_Rating_Count_Map'] = str(rating_count_map)
                item['Product_Size'] = size_variant
                item['Product_Details'] = data['description']
                item['Product_Delivery_Charges'] = data['shipping']['charges']
                item['Product_In_Stock_Status'] = str(data['in_stock']).upper()
                item['Product_Mrp'] = mrp
                item['Product_Original_Price'] = original_price
                item['Product_Display_Price'] = discounted_price
                item['Product_Discount_Percent'] = discount_percentage
                item['Seller_Id'] = supplier_id
                item['Seller_Name'] = data['supplier_name']
                item['Seller_Url'] = seller_url
                item['Seller_Rating'] = Seller_Avg_Rating
                item['Seller_Rating_Count'] = Seller_Rating_Count
                item['Seller_Followers_Count'] = Seller_Followers_Count
                item['Seller_Product_Count'] = Seller_Product_Count
                item['Product_Delivery_Date'] = 'NA'
                item['Product_Pincode'] = '110001'
                item['Product_City'] = 'DELHI'
                item['Super_Category_Name'] = Supercategory_Name
                item['Category_name'] = Category_name
                item['Sub_Category_Name'] = Subcategory_Name
                item['Category_URL'] = category_URL
                print(item)
                yield item
                update_query = f"update {db.db_links_table} set `status` = 'Done' where `product_link` = '{product_url}' AND `Subcategory_Name`='{Subcategory_Name}'"
                try:
                    self.cursor.execute(update_query)
                    self.con.commit()
                except Exception as e:
                    print(e)
            else:
                status = "This product is out of stock"
                update_query = f"update {db.db_links_table} set `status` = '{status}' where `product_link` = '{product_url}' AND `Subcategory_Name`='{Subcategory_Name}'"
                try:
                    self.cursor.execute(update_query)
                    self.con.commit()
                except Exception as e:
                    print(e)
        else:
            status = "Not Found page"
            update_query = f"update {db.db_links_table} set `status` = '{status}' where `product_link` = '{product_url}' AND `Subcategory_Name`='{Subcategory_Name}'"
            try:
                self.cursor.execute(update_query)
                self.con.commit()
            except Exception as e:
                print(e)

    def close(self, reason):
        """Called when the spider finishes to export the final data to Excel."""

        # Call the method to export collected data to an Excel file
        file_generate()


if __name__ == '__main__':
    # execute("scrapy crawl product_data_des -a start=1 -a end=1".split())
    execute("scrapy crawl product_data_des -a start=1 -a end=5000".split())
