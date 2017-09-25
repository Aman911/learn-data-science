
# coding: utf-8

# In[181]:

import pymysql
from pandas import Series,DataFrame
import pandas as pd
import numpy as np
con = pymysql.connect(host='localhost',
                             user='root',
                             password='',
                             db='demo',
                
                     )


# In[182]:

def sql_to_df(sql_query):

    
    df = pd.read_sql(sql_query, con)
    return df


# In[183]:

query1 = ''' SELECT *
            FROM products
            WHERE status = 1; '''
dframe1 = sql_to_df(query1)
dframe1.sort_values('category_id')


# In[ ]:




# In[184]:

query2 ='''SELECT * FROM categories;''' 
sql_to_df(query2)
dframe2 = sql_to_df(query2)
dframe2 = dframe2.rename(columns={'id': 'category_id'})
dframe2
query = ''' SELECT id,location
            FROM images'''
dframe = sql_to_df(query)
dframe = dframe.rename(columns={'id':'category_id','location':'subcat_image'})
dframe2 = pd.merge(dframe2, dframe, on='category_id')
query6 = ''' SELECT id,location
            FROM images'''
dframe6 = sql_to_df(query6)
dframe6 = dframe6.rename(columns={'id':'category_id','location':'cat_image'})
dframe2 = pd.merge(dframe2, dframe6, on='category_id')


# In[185]:

query3 = ''' SELECT id,name
            FROM products
            WHERE category_id = 5; '''
sql_to_df(query3)


# In[186]:

result = pd.merge(dframe1, dframe2, on='category_id')
result.drop(['deleted','compare_price','user_id_x','slug_x','created_at_x','updated_at_x','deal','compare_price','compare_string','slug_y','details','shipping_details','featured',],axis=1,inplace = True)
result.drop(['user_id_y','parent_id','created_at_y','updated_at_y','excerpt'], axis=1, inplace=True)


# In[187]:

query4 ='''SELECT * FROM product_variants;''' 
sql_to_df(query4)
dframe4 = sql_to_df(query4)


# In[188]:

query4 = ''' SELECT product_id, sum(remaining)
             FROM product_variants
             GROUP BY product_id; '''
dframe4 = sql_to_df(query4)
dframe4 = dframe4.rename(columns={'product_id': 'id','sum(remaining)':'quantity'})


# In[189]:

result2 = pd.merge(result, dframe4, on='id')


# In[ ]:




# In[190]:

query5 = ''' SELECT id,location
            FROM images'''
dframe5 = sql_to_df(query5)
dframe5 = dframe5.rename(columns={'id':'featuredImage_id'})


# In[191]:

result3 = pd.merge(result2, dframe5, on='featuredImage_id')


# In[192]:

result3.drop(['category_id','featuredImage_id'], axis=1, inplace=True)
result4 = result3.sort_values('id')
result4


# In[193]:

query8 = ''' SELECT id
            FROM products
            WHERE status = 1;'''
dframe8 = sql_to_df(query8)
dframe8.sort_values('id')
arr1 = np.array(dframe8,dtype = str)
for i in range(0,len(arr1)):
      arr1[i][0]=('sku00'+(arr1[i][0]))
        



# In[217]:

sku = pd.DataFrame(arr1,columns=['sku'])
df9 = dframe8.combine_first(sku)
result5 = pd.merge(result3, df9, on='id')
pd.DataFrame(result6,columns = ['sort_product_alphabatically','custom_options','weight_unit','weight_value','video_name','is_youtube_video'])
result5 = result5.rename(columns={'location':'image_name','specs':'description','name_x':'ptitle','name_y':'cat_name'})
result6 = result5.set_index(['cat_name','ptitle','description','sku','quantity','price','status','image_name','subcat_image','cat_image'])
result7 = pd.DataFrame(result6,columns = ['sort_product_alphabatically','custom_options','weight_unit','weight_value','video_name','is_youtube_video'])
result7.to_csv('a2z.csv')

