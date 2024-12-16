# import json
# import os
# import random
# from datetime import datetime, timedelta
# from typing import Union
# from scrapy import Spider
# from twisted.internet.defer import Deferred
# from meesho_des.items import *
# import pymysql
# import scrapy
# from scrapy.cmdline import execute
# import meesho_des.db_config as db
# import gzip
# from meesho_des.file_genration import file_generate
#
#
# class ProductDataSpider(scrapy.Spider):
#     name = "product_data_des"
#     handle_httpstatus_list = [403, 429, 400]
#     # folder_location = f"D:/Meesho/meesho_des"
#     DATE = str(datetime.now().strftime("%Y%m%d"))
#     folder_location = f"C:/Meesho/meesho_des/pdp_page_save/"
#     errors = list()
#
#     DATE = str(datetime.now().strftime("%Y%m%d"))
#
#     def __init__(self, name=None, start='', end='', **kwargs):
#         super().__init__(name, **kwargs)
#         # DATABASE SPECIFIC VALUES
#         self.start = start
#         self.end = end
#
#         # Create folder if not exits
#         os.makedirs(self.folder_location, exist_ok=True)
#
#         # DATABASE CONNECTION
#         self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, db=db.db_name)
#         self.cursor = self.con.cursor()
#         self.con.autocommit(True)
#
#     def start_requests(self):
#         query = f"select `product_link` FROM {db.db_links_table} where status='pending' and id between {self.start} and {self.end};"
#         self.cursor.execute(query)
#         query_results = self.cursor.fetchall()
#         self.logger.info(f"\n\n\nTotal Results ...{len(query_results)}\n\n\n", )
#
#         for i in query_results:
#             if not i[0]:
#                 continue
#             url = i[0].strip()
#             sku = url.split('/')[-1].replace('?utm_source=s','')
#
#             if not os.path.exists(self.folder_location + sku + ".html.gz"):
#                 url = "https://www.meesho.com/s/p/" + sku
#
#                 yield scrapy.Request(
#                     url=url,
#                     cb_kwargs={
#                         "page_name": sku,
#                         "url":url,
#                         "page_read": False
#                     },
#                     meta={"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])},
#                 )
#             else:
#                 yield scrapy.Request(url=f'file:///{self.folder_location + sku + ".html.gz"}',
#                                      cb_kwargs={
#                                          "page_name": i,
#                                          "url":url,
#                                          "page_read": True
#                                      },
#                                      meta={"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])})
#             # break
#
#     def parse(self, response, **kwargs):
#         sku = kwargs['page_name']
#         url = kwargs['url']
#         if response.status in [403, 429]:
#             request = response.request.copy()
#             request.meta.update({"impersonate": random.choice(["chrome110", "edge99", "safari15_5"])})
#             request.dont_filter = True
#             yield request
#             return None
#         if kwargs['page_read']:
#             response = gzip.decompress(response.body)
#             data = response.split(b'id="__NEXT_DATA__" type="application/json">')[1].split(b"</script>")[0]
#         else:
#             data = response.body.split(b'id="__NEXT_DATA__" type="application/json">')[1].split(b"</script>")[0]
#             file_path = self.folder_location + sku + f".html.gz"
#             # Writing the Gzip file safely
#             with gzip.open(file_path, "wb") as f:
#                 f.write(response.body)
#
#         # response = Selector(text=response_text)
#         status = "pending"
#         data = json.loads(data).get('props', {}).get('pageProps', {}).get('initialState', {}).get('product', {}).get('details', {}).get('data', {})
#         if data:
#             if data['valid']:
#                 if 'suppliers' not in data:
#                     return None
#                 supplier = data.get('suppliers', [{}])[0]
#                 supplier_id = str(supplier.get('id', 'NA'))
#                 SKU_URL_MEESHO = data.get('meta_info', {}).get('canonical_url', 'NA')
#                 product_price = data.get('price', 0)
#                 status = "Done"
#                 mrp = data.get('mrp_details', {}).get('mrp', product_price)
#                 discount = round((1 - (product_price / mrp)) * 100) if mrp else 0
#
#                 rating_count_map = "NA"
#                 try:
#                     rating_data = data.get('review_summary', {}).get('data', {})
#                     rating_count_map = rating_data.get('rating_count_map', {})
#                     if rating_count_map:
#                         key_mapping = {"1": "Excellent", "2": "Very Good", "3": "Good", "4": "Average", "5": "Poor"}
#                         rating_count_map = {key_mapping.get(key, key): value for key, value in rating_count_map.items()}
#                 except Exception as e:
#                     print(e)
#
#                 Product_Avg_Rating = data.get('review_summary', {}).get('data', {}).get('average_rating', "NA")
#                 Product_Rating_Count = data.get('review_summary', {}).get('data', {}).get('rating_count', "NA")
#                 Product_Review_Count = data.get('review_summary', {}).get('data', {}).get('review_count', "NA")
#                 Seller_Avg_Rating = supplier.get('average_rating', "NA")
#                 Seller_Rating_Count = supplier.get('rating_count', "NA")
#                 Seller_Followers_Count = supplier.get('shop_value_props', [{}])[1].get('follower', {}).get('count',"NA")
#                 Seller_Product_Count = supplier.get('shop_value_props', [{}])[2].get('product', {}).get('count', "NA")
#
#
#                 seller_handle = supplier.get('handle', '')
#                 seller_url = "NA"
#                 if seller_handle:
#                     seller_url = f'https://www.meesho.com/{seller_handle}?ms=2'
#                 # ['props']['pageProps']['initialState']['product']['details']['data']
#
#                 inventory_list = supplier.get('inventory', [])
#                 size_variant = ' | '.join([item.get('variation', {}).get('name', '') for item in inventory_list]) if inventory_list else ' | '.join(data.get('variations', []))
#
#
#                 item = MeeshoDesItem()
#                 item['Product_Sku_Id'] = data.get('product_id', 'NA')
#                 item['Product_Sku_Name'] = data.get('name', 'NA').strip().strip("\\")
#                 item['Product_Sku_Url'] = SKU_URL_MEESHO
#                 item['Product_Images_Urls'] = ' | '.join(data.get('images', []))
#                 item['Product_Count_of_Images'] = len(data.get('images', []))
#                 item['Product_Avg_Rating'] = Product_Avg_Rating
#                 item['Product_Rating_Count'] = Product_Rating_Count
#                 item['Product_Review_Count'] = Product_Review_Count
#                 item['Product_Rating_Count_Map'] = str(rating_count_map)
#                 item['Product_Size'] = size_variant
#                 item['Product_Details'] = data.get('description', 'NA')
#                 item['Product_Delivery_Charges'] = data.get('shipping', {}).get('charges', 0)
#                 item['Product_Discount_Percent'] = discount
#                 item['Product_Display_Price_On_PDP_Price_After_Discount'] = product_price
#                 item['Product_In_Stock_Status'] =  str(data.get('in_stock', '')).lower()
#                 item['Product_Mrp'] = mrp
#                 item['Seller_Id'] = supplier_id
#                 item['Seller_Name'] = data.get('supplier_name', 'NA')
#                 item['Seller_Url'] = seller_url
#                 item['Seller_Avg_Rating'] = Seller_Avg_Rating
#                 item['Seller_Rating_Count'] = Seller_Rating_Count
#                 item['Seller_Followers_Count'] = Seller_Followers_Count
#                 item['Seller_Product_Count'] = Seller_Product_Count
#                 item['Product_Delivery_Date'] = "NA"
#                 item['Product_Pincode'] = '110001'
#                 item['Product_City'] = 'DELHI'
#                 item['Product_No_Delivery_Days_from_Scrape_Date'] = "NA"
#                 item['Product_Price_in_Cart__After_Applying_Coupon'] = float(data.get('shipping', {}).get('charges', 0)) + float(product_price)
#                 print(item)
#                 yield item
#             else:
#                 status = "This product is out of stock"
#                 update_query = f"update {db.db_links_table} set `status` = '{status}' where `product_link` = '{url}'"
#                 self.cursor.execute(update_query)
#                 self.con.commit()
#         else:
#             status = "Not Found page"
#             update_query = f"update {db.db_links_table} set `status` = '{status}' where `product_link` = '{url}'"
#             self.cursor.execute(update_query)
#             self.con.commit()
#
#     def close(self, reason):
#         """Called when the spider finishes to export the final data to Excel."""
#
#         # Call the method to export collected data to an Excel file
#         file_generate()
#
#
# if __name__ == '__main__':
#     execute("scrapy crawl product_data_des -a start=1 -a end=1".split())
