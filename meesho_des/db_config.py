from datetime import datetime

# DATABASE DETAILS
db_host = 'localhost'
db_user = 'root'
db_password = 'actowiz'
db_name = 'meesho_des'


current_week = str(datetime.now().strftime("%Y%m%d"))
# current_week = '20241118'
db_links_table = f'product_links_{current_week}'
db_data_table = f'meesho_pdp_data_{current_week}'
db_pin_data_table = f'meesho_pc_data_{current_week}'

template_table = f"template_{current_week}"