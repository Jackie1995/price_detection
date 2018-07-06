
# coding: utf-8

# ### 分析依赖的数据：837717.sql 产生的数据：pt=0619 北京-合租-第三方品牌-在线-强命中房源- 863条信息。

# In[1]:


import os
import sys
import pandas as pd
reload(sys)
sys.setdefaultencoding('utf-8')


# In[2]:

py_dir = sys.path[0]
#os.chdir(py_dir)
csv_file_name = sys.argv[1]
# full_path_name = os.path.join(py_dir, csv_file_name)
# ### 读入数据集：bj_he_mingzhong_df
# 这里需要事先把要导入的csv 文件：用notepad转存成 utf-8 编码格式。

# In[3]:
# 当前py文件所在目录：py_dir

bj_he_mingzhong_df = pd.read_csv(csv_file_name, encoding='utf-8')


# In[4]:


# bj_he_mingzhong_df.info()


# ### 添加4个新列：挂牌价/商圈规则最低价（price_low_bizcircle_bili） 与 挂牌价/品牌规则最低价（price_low_brand_bili）

# In[5]:


def add_bili(df):
    df['price_low_bizcircle_bili'] = df['rent_price_listing']/df['low_price_bizcircle_beijing']
    df['price_low_brand_bili'] = df['rent_price_listing']/df['low_price_brand_beijing']
    df['price_high_bizcircle_bili'] = df['rent_price_listing']/df['high_price_bizcircle_beijing']
    df['price_high_brand_bili'] = df['rent_price_listing']/df['high_price_brand_beijing']


# In[6]:


add_bili(bj_he_mingzhong_df)


# 【新建数据集】 bj_he_mingzhong_low_df ：只保留强命中房源中价格过低的房源。
bj_he_mingzhong_low_df = bj_he_mingzhong_df[(bj_he_mingzhong_df['price_low_bizcircle_bili']<1) & (bj_he_mingzhong_df['price_low_brand_bili']<1)]

# ### 【新建数据集】sample_qiang_low_80 对全量的强命中价格过低房源进行随机抽样，简单随机抽取其中的80套房源。

# In[7]:


def create_random_df(df, n_sample=80):
    sample_qiang_low_80 = df.sample(n=n_sample)
    sample_qiang_low_80['if_qiang'] = 1
    sample_qiang_low_80['if_di'] = 0
    return sample_qiang_low_80


# In[8]:


sample_qiang_low_80 = create_random_df(bj_he_mingzhong_low_df)

# sample_qiang_low_80


# ### 【新建数据集】price_low_20_bili 选出子集：强命中房源中低价格比例小于0.8

# In[9]:


def create_low_bili_df(df):
	price_low_20_bili = df.loc[(df['price_low_bizcircle_bili'] < 0.8) & (df['price_low_brand_bili'] < 0.8),:]
	price_low_20_bili.loc[:,'if_qiang'] = 0
	price_low_20_bili.loc[:,'if_di'] = 1
	return price_low_20_bili


# In[10]:


price_low_20_bili = create_low_bili_df(bj_he_mingzhong_df)

# price_low_20_bili


# ### 【新建数据集】chanchu_df : 将 sample_qiang_low_80 和 price_low_20_bili 去重后merge在一起。

# In[11]:


# 先求两个数据集的共同索引：duplicated_index
# duplicated_index = sample_qiang_low_80.index.intersection(price_low_20_bili.index)
# 将sample_qiang_low_80中的共同索引位置的 if_di 设置为1。
# sample_qiang_low_80.loc[duplicated_index, 'if_di'] = 1
# 删掉 price_low_20_bili 中共同索引位置所在的行
# price_low_20_bili.drop(index=duplicated_index, inplace=True)
# 将两个数据框concat在一起：得到 chanchu_df
# chanchu_df = pd.concat([sample_qiang_low_80, price_low_20_bili], axis=0, verify_integrity=True).reset_index(drop=True)
chanchu_df = bj_he_mingzhong_df


# In[12]:


# chanchu_df.dtypes


# ### 导出数据集新增中文字段  chanchu_df

# In[13]:


chanchu_df['城市'] = '北京市'
chanchu_df['出租类型'] = '合租'
chanchu_df['C端链接'] = chanchu_df['houseid_url']
chanchu_df['B端房源ID'] = chanchu_df['rent_unit_code']
chanchu_df['商家名称'] = chanchu_df['full_name']
chanchu_df['品牌名称'] = chanchu_df['brand_name']
chanchu_df['门店ID'] = chanchu_df['apartment_code']
chanchu_df['管家'] = chanchu_df['main_contacts_name']
chanchu_df['BD联系人'] = chanchu_df['contact_name']
chanchu_df['联系电话'] = chanchu_df['contact_number']
chanchu_df['页面出租面积'] = chanchu_df['rent_area']
chanchu_df['页面价格'] = chanchu_df['rent_price_listing']
chanchu_df['策略价格区间-商圈下界'] = chanchu_df['low_price_bizcircle_beijing']
chanchu_df['策略价格区间-商圈上界'] = chanchu_df['high_price_bizcircle_beijing']
chanchu_df['策略价格区间-品牌下界'] = chanchu_df['low_price_brand_beijing']
chanchu_df['策略价格区间-品牌上界'] = chanchu_df['high_price_brand_beijing']
chanchu_df['页面价格/商圈下界'] = chanchu_df['price_low_bizcircle_bili']
chanchu_df['页面价格/商圈上界'] = chanchu_df['price_high_bizcircle_bili']
chanchu_df['页面价格/品牌下界'] = chanchu_df['price_low_brand_bili']
chanchu_df['页面价格/品牌上界'] = chanchu_df['price_high_brand_bili']
chanchu_df['所在商圈的在线房源总数'] = chanchu_df['house_number_bizcircle_beijing']
chanchu_df['所属品牌的北京在线房源总数'] = chanchu_df['house_number_brand_beijing']
#chanchu_df['是否方案1'] = chanchu_df['if_qiang']
#chanchu_df['是否方案2'] = chanchu_df['if_di']
chanchu_df['平台上架时间'] = chanchu_df['app_plat_ctime']
chanchu_df['平台最后修改时间'] = chanchu_df['app_plat_mtime']


# In[14]:


# list(chanchu_df.columns[-20:])
# 设置输出字段为：valid_columns
valid_columns = ['城市',
 '出租类型',
 'C端链接',
 'B端房源ID',
 '商家名称',
 '品牌名称',
 '门店ID',
 '管家',
 'BD联系人',
 '联系电话',
 '页面出租面积',
 '页面价格',
 '策略价格区间-商圈下界',
 '策略价格区间-商圈上界',
 '策略价格区间-品牌下界',
 '策略价格区间-品牌上界',
 '页面价格/商圈下界',
 '页面价格/商圈上界',
 '页面价格/品牌下界',
 '页面价格/品牌上界',
 '所在商圈的在线房源总数',
 '所属品牌的北京在线房源总数',
# '是否方案1',
# '是否方案2',
 '平台上架时间',
 '平台最后修改时间',
 'pt']


# In[15]:


chanchu_df = chanchu_df.reindex(columns= valid_columns)


# ### 将数据集写出到csv文件。
# 数据集命名：bj_he_pt

# In[16]:


row_number = chanchu_df.shape[0]
pt = str(chanchu_df['pt'][1])[:8]
file_name = '价格异常_规则策略命中{0}套北京合租房源_pt{1}.csv'.format(row_number, pt)


# In[17]:


chanchu_df.to_csv(file_name, index=False)

print('successfully!{0} has been saved in {1}'.format(file_name, py_dir))

