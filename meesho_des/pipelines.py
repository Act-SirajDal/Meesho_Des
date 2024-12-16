# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from datetime import datetime
import meesho_des.db_config as db
from meesho_des.items import *
DATE = str(datetime.now().strftime("%Y%m%d"))
# DATE = "20241206"


class MeeshoDesPipeline:
    def process_item(self, item, spider):

        create_table_pdp = f'''
            CREATE TABLE IF NOT EXISTS `meesho_pdp_data_{DATE}` (
                `id` INT NOT NULL AUTO_INCREMENT,
                `Product_Sku_Id` VARCHAR(255) DEFAULT NULL,
                `Product_Sku_Name` text DEFAULT NULL,
                `Product_Sku_Url` text DEFAULT NULL,
                `Product_Images_Urls` text DEFAULT NULL,
                `Product_Count_of_Images` VARCHAR(255) DEFAULT NULL,
                `Product_Rating` VARCHAR(255) DEFAULT NULL,
                `Product_Rating_Count` VARCHAR(255) DEFAULT NULL,                    
                `Product_Review_Count` VARCHAR(255) DEFAULT NULL,                  
                `Product_Rating_Count_Map` VARCHAR(255) DEFAULT NULL,                  
                `Product_Size` text DEFAULT NULL,                  
                `Product_Details` text DEFAULT NULL,  
                `Product_Delivery_Charges` VARCHAR(255) DEFAULT NULL,
                `Product_In_Stock_Status` VARCHAR(255) DEFAULT NULL,
                `Product_Mrp` VARCHAR(255) DEFAULT NULL,                 
                `Product_Original_Price` VARCHAR(255) DEFAULT NULL,                 
                `Product_Display_Price` VARCHAR(255) DEFAULT NULL, 
                `Product_Discount_Percent` VARCHAR(255) DEFAULT NULL,                
                `Seller_Id` VARCHAR(255) DEFAULT NULL,
                `Seller_Name` VARCHAR(255) DEFAULT NULL,
                `Seller_Url` VARCHAR(255) DEFAULT NULL,
                `Seller_Rating` VARCHAR(255) DEFAULT NULL,   
                `Seller_Rating_Count` VARCHAR(255) DEFAULT NULL,
                `Seller_Followers_Count` VARCHAR(255) DEFAULT NULL,
                `Seller_Product_Count` VARCHAR(255) DEFAULT NULL,                 
                `Product_Delivery_Date` VARCHAR(255) DEFAULT NULL,
                `Product_Pincode` VARCHAR(255) DEFAULT NULL,
                `Product_City` VARCHAR(255) DEFAULT NULL,
                `Super_Category_Name` VARCHAR(255) DEFAULT NULL,
                `Category_name` VARCHAR(255) DEFAULT NULL,
                `Sub_Category_Name` VARCHAR(255) DEFAULT NULL,
                `Category_URL` text DEFAULT NULL,
                `Product_Pin_PageSave_Status` VARCHAR(255) DEFAULT 'pending',
                PRIMARY KEY (`id`)
            );
        '''
        spider.cursor.execute(create_table_pdp)
        spider.con.commit()

        if isinstance(item, MeeshoDesItem):
            try:
                # Id = item.pop('Id')
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_db = f"insert into {db.db_data_table}( " + fields + " ) values ( " + values + " )"

                try:
                    # status = "Done"
                    values_data = item.values()
                    row_dict = dict(zip(item.keys(), values_data))
                    print(row_dict)
                    Product_Sku_Id = row_dict.get('Product_Sku_Id')
                    spider.cursor.execute(insert_db, tuple(item.values()))
                    spider.logger.info(f'Data Inserted...')
                    print(f'https://www.meesho.com/s/p/{Product_Sku_Id}?utm_source=s')
                    url = f'https://www.meesho.com/s/p/{Product_Sku_Id}?utm_source=s'

                except Exception as e:
                    print(e)
            except Exception as e:
                print(e)
