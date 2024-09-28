import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns
import sys

# Move two levels up (to the project root) and append the `src` folder
src_path = os.path.abspath(os.path.join(os.getcwd(), '..', '..'))

# Append src to sys.path
sys.path.append(src_path)

from src import query_engines

q = query_engines.QueryEngines()

DAYS_IN_ADVANCE = "8"

params = [
    {'name':'days_in_advance', 'value': DAYS_IN_ADVANCE},]

# =====================================
# General data products
# =====================================

# calendar
print('calendar - General data products - 1/20')
q.prepare_query(query_file='ddps/calendar.sql', params=params)
df1 = q.query_run_starburst()

# active_partners
print('active_partners - General data products - 2/20')
q.prepare_query(query_file='ddps/active_partners.sql', params=params)
df2 = q.query_run_starburst()

# bought_products_v2
print('bought_products_v2 - General data products - 3/20')
q.prepare_query(query_file='ddps/bought_products_v2.sql', params=params)
df3 = q.query_run_starburst()

# dynamic_sessions_v1
print('dynamic_sessions_v1 - General data products - 4/20')
q.prepare_query(query_file='ddps/dynamic_sessions_v1.sql', params=params)
df4 = q.query_run_starburst()

# fct_contact_intent
print('fct_contact_intent - General data products - 5/20')
q.prepare_query(query_file='ddps/fct_contact_intent.sql', params=params)
df5 = q.query_run_starburst()

# first_order_levels
print('first_order_levels - General data products - 6/20')
q.prepare_query(query_file='ddps/first_order_levels.sql', params=params)
df6 = q.query_run_starburst()

# groceries_top_partners
print('groceries_top_partners - General data products - 7/20')
q.prepare_query(query_file='ddps/groceries_top_partners.sql', params=params)
df7 = q.query_run_starburst()

# order_descriptors_v2
print('order_descriptors_v2 - General data products - 8/20')
q.prepare_query(query_file='ddps/order_descriptors_v2.sql', params=params)
df8 = q.query_run_starburst()

# orders_gmv_variation
print('orders_gmv_variation - General data products - 9/20')
q.prepare_query(query_file='ddps/orders_gmv_variation.sql', params=params)
df9 = q.query_run_starburst()

# products_gmv_variation
print('products_gmv_variation - General data products - 10/20')
q.prepare_query(query_file='ddps/products_gmv_variation.sql', params=params)
df10 = q.query_run_starburst()

# order_refund_incidents
print('order_refund_incidents - General data products - 11/20')
q.prepare_query(query_file='ddps/order_refund_incidents.sql', params=params)
df11 = q.query_run_starburst()

# pna_orders_info
print('pna_orders_info - General data products - 12/20')
q.prepare_query(query_file='ddps/pna_orders_info.sql', params=params)
df12 = q.query_run_starburst()

# pna_products_info
print('pna_products_info - General data products - 13/20')
q.prepare_query(query_file='ddps/pna_products_info.sql', params=params)
df13 = q.query_run_starburst()

# product_availability_v2
print('product_availability_v2 - General data products - 14/20')
q.prepare_query(query_file='ddps/product_availability_v2.sql', params=params)
df14 = q.query_run_starburst()

# product_collections_flattened_v2
print('product_collections_flattened_v2 - General data products - 15/20')
q.prepare_query(query_file='ddps/product_collections_flattened_v2.sql', params=params)
df15 = q.query_run_starburst()

# products_v2_daily
print('products_v2_daily - General data products - 16/20')
q.prepare_query(query_file='ddps/products_v2_daily.sql', params=params)
df16 = q.query_run_starburst()

# retention_order_info
print('retention_order_info - General data products - 17/20')
q.prepare_query(query_file='ddps/retention_order_info.sql', params=params)
df17 = q.query_run_starburst()

# sessions_nc_rc
print('sessions_nc_rc - General data products - 18/20')
q.prepare_query(query_file='ddps/sessions_nc_rc.sql', params=params)
df18 = q.query_run_starburst()

# spm_ddp_info
print('spm_ddp_info - General data products - 19/20')
q.prepare_query(query_file='ddps/spm_ddp_info.sql', params=params)
df19 = q.query_run_starburst()

# store_address_product_stockout
print('store_address_product_stockout - General data products - 20/20')
q.prepare_query(query_file='ddps/store_address_product_stockout.sql', params=params)
df20 = q.query_run_starburst()

dataframes = [df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18, df19, df20]

merged_df = df1.copy()

for df in dataframes:
    if not df.empty:
        merged_df = pd.merge(merged_df, df, on='p_creation_date', how='left')

for col in merged_df.columns:
    if merged_df[col].dtype == 'float64':
        merged_df[col] = merged_df[col].fillna(0).astype(int)
        
pivot_table = merged_df.set_index('p_creation_date').T

pivot_table.to_csv('outputs/general_data_products.csv')

# =====================================
# Data products with only one picture
# =====================================

# pna_bot_info
print('pna_bot_info - Data products with only one picture 1/4')
q.prepare_query(query_file='ddps/pna_bot_info.sql', params=params)
df_1 = q.query_run_starburst()

# pna_bot_gmv_info
print('pna_bot_gmv_info - Data products with only one picture 2/4')
q.prepare_query(query_file='ddps/pna_bot_gmv_info.sql', params=params)
df_2 = q.query_run_starburst()

# replacement_engine_info
print('replacement_engine_info - Data products with only one picture 3/4')
q.prepare_query(query_file='ddps/replacement_engine_info.sql', params=params)
df_3 = q.query_run_starburst()

# replacement_engine_info_dh
print('replacement_engine_info_dh - Data products with only one picture 4/4')
q.prepare_query(query_file='ddps/replacement_engine_info_dh.sql', params=params)
df_4 = q.query_run_starburst()

dataframes = [df_1, df_2, df_3, df_4]

merged_df = pd.concat(dataframes, axis=1)

merged_df.to_csv('outputs/only_one_picture_data_products.csv', index=False)