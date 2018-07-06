# coding: utf-8

# 来源数据：get_all_shot_house.sql 产生的数据：pt=?入参 北京-合租-第三方品牌-在线-强命中房源- 大约1000条样本信息。

import os
import sys
import pandas as pd

reload(sys)
sys.setdefaultencoding('utf-8')


# 读入数据集 在python中以DataFrame的对象类型存储。
def read_input_as_df(dir, file):
    """
    This function is used to read external csv file(usually get from HIVE) into python as DataFrame
    :param dir: type = string; the directory where the script located.
    :param file: type = string; the name of input csv file.
    :return: type = pandas.DataFrame.
    """
    if file in os.listdir(dir):
        bj_he_mingzhong_df = pd.read_csv(input_csv_name, encoding='utf-8')
    else:
        bj_he_mingzhong_df = pd.read_csv('../data/' + input_csv_name, encoding='utf-8')
    return bj_he_mingzhong_df


# 添加新列：页面价格/各个策略参考价格值 比例
def add_bili(df):
    """
    This function is uesd to add 4 bili columns to the original DataFrme
    :param df: type = pandas.DataFrme
    :return: a mutated DataFrame
    """
    df['price_low_bizcircle_bili'] = df['rent_price_listing'] / df['low_price_bizcircle_beijing']
    df['price_low_brand_bili'] = df['rent_price_listing'] / df['low_price_brand_beijing']
    df['price_high_bizcircle_bili'] = df['rent_price_listing'] / df['high_price_bizcircle_beijing']
    df['price_high_brand_bili'] = df['rent_price_listing'] / df['high_price_brand_beijing']
    return df


# 只保留强命中房源中价格过低的房源。
def keep_only_price_low_houses(df):
    """
    This function is used to keep the qiang mingzhong houses whose price is lower than rule price.
    implement filter action along with row( filter samples)
    :param df: type = pandas.DataFrame
    :return: a mutated Dataframe
    """
    return df[(df['price_low_bizcircle_bili'] < 1) & (df['price_low_brand_bili'] < 1)]


# 对全量的强命中价格过低房源进行随机抽样，简单随机抽取其中的80套房源。
def get_sample(df, n_sample=80):
    """
    This function is used to get a simple random sample from original DataFrame.
    the redturned new sampled DataFrame has 2 new flag-columns: if_qiang & if_di

    :param df: type = pandas.DataFrame; usually the qiang-low house df.
                this parameter is usually the return of keep_only_price_low_houses()
    :param n_sample: type = int;
    :return: type = pandasa.DataFrame; a new DataFrame
    """
    sample_qiang_low_80 = df.sample(n=n_sample)
    sample_qiang_low_80['if_qiang'] = 1
    sample_qiang_low_80['if_di'] = 0
    return sample_qiang_low_80


# 选出子集：强命中房源中低价格比例小于0.8
def get_low_bili_df(df):
    """
    This function is used to get houses which has extremely low prices that is less than 80% of  rule's low_price.
    Note: like get_sample() function , this function is also added two flag_columns: if_qiang & if_di
    :param df: type = pandas.DataFrame; usually take the return of keep_only_price_low_houses() as param df.
    :return: type = pandas.DataFrame; return a new DataFrame
    """
    price_low_20_bili = df.loc[(df['price_low_bizcircle_bili'] < 0.8) & (df['price_low_brand_bili'] < 0.8), :]
    price_low_20_bili.loc[:, 'if_qiang'] = 0
    price_low_20_bili.loc[:, 'if_di'] = 1
    return price_low_20_bili


# 将两种方案得到的数据集 sample_qiang_low_80 和 price_low_20_bili 去重后merge在一起。
def merge_two_df(sample_qiang_low_80, price_low_20_bili):
    """
    This function is to merge 2 dataframes which are derived from different sample strategy.
    :param sample_qiang_low_80: type = pandas.DataFrame; the generated df usually the return of get_sample()
    :param price_low_20_bili: type = pandas.DataFrame; the generated df usually the return of  get_low_bili_df()
    :return: type = pandas.DataFrame; a new df.
    """
    # 先求两个数据集的共同索引：duplicated_index
    duplicated_index = sample_qiang_low_80.index.intersection(price_low_20_bili.index)
    # 将sample_qiang_low_80中的共同索引位置的 if_di 设置为1。
    sample_qiang_low_80.loc[duplicated_index, 'if_di'] = 1
    # 删掉 price_low_20_bili 中共同索引位置所在的行
    price_low_20_bili.drop(index=duplicated_index, inplace=True)
    # 将两个数据框concat在一起：得到 chanchu_df
    chanchu_df = pd.concat([sample_qiang_low_80, price_low_20_bili], axis=0, verify_integrity=True).reset_index(
        drop=True)
    return chanchu_df


# 添加中文名称列
def add_chinese_colname_to_df(chanchu_df):
    """
    This function is used to add new chinese column names to original DataFrme.
    Why need chinese name? Because we hope the head of output-excel file is friendly to read.
    :param chanchu_df: type = pandas.DataFrame; chanchu is usually a df which has finished processing.just need to adjust its out columns.
    :return: type = pandas.DataFrame; a mutated DataFrame.
    """
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
    # chanchu_df['是否方案1'] = chanchu_df['if_qiang']
    # chanchu_df['是否方案2'] = chanchu_df['if_di']
    chanchu_df['平台上架时间'] = chanchu_df['app_plat_ctime']
    chanchu_df['平台最后修改时间'] = chanchu_df['app_plat_mtime']
    return chanchu_df


# 定义输出字段：VALID_COLUMNS
VALID_COLUMNS = ['城市',
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


# 给输出的csv文件按照一定的规则起名字。
def get_file_name(chanchu_df):
    """
    This function is used to regulate the name of output-csv.
    :param chanchu_df: type = pandas.DataFrame; usually the completed and perfect DataFrame which we need.
    :return: type = str.
    """
    row_number = chanchu_df.shape[0]
    pt = str(chanchu_df['pt'][1])[:8]
    file_name = '价格异常_规则策略命中{0}套北京合租房源_pt{1}.csv'.format(row_number, pt)
    return file_name


if __name__ == '__main__':
    py_dir = sys.path[0]
    input_csv_name = sys.argv[1]
    bj_he_mingzhong_df = read_input_as_df(py_dir, input_csv_name)
    bj_he_mingzhong_df = add_bili(bj_he_mingzhong_df)
    bj_he_mingzhong_df = keep_only_price_low_houses(bj_he_mingzhong_df)
    chanchu = add_chinese_colname_to_df(bj_he_mingzhong_df)
    output_csv_name = get_file_name(chanchu)
    chanchu[VALID_COLUMNS].to_csv(output_csv_name, index=False)
    print('Successfully!!!!!!\n {0} has been successfully saved in {1}'.format(output_csv_name, py_dir))
