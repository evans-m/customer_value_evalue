# 作者 evan   
# 日期 2018/10/11   
# 时间 7:30   
# PyCharm



#导入需要使用的包
import numpy as np
import pandas as pd
import time
import math
import os
import gc
from sqlalchemy import create_engine

#连接数据库的配置
os.environ['NLS_LANG'] ='AMERICAN_AMERICA.ZHS16GBK'
SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or '****'
engine = create_engine(SQLALCHEMY_DATABASE_URI,encoding='utf8')#create engine with sqlalchemy
def main():
    specified_path = r'E:\evan'
    #NET_df_PATH=Combine_df('net',specified_path)
    net_df = read_OverSize_data(r'E:\evan\Combined_NET_20181009.csv')
    net_df = SCORE(net_df, 'net', None)
    tag_net_df_path = Tag(net_df,'net',specified_path)
    #net_sample = read_OverSize_data(r'E:\evan\net_sample.csv')
    #net_sample = SCORE(net_sample,'net',None)
    #tag_net_sample = Tag(net_sample,'net',specified_path)


def Combine_df(level, out_path=os.getcwd()):
    '''
    :param level: type:str ['net','dealer']
    :param out_file_name: type: str ,specify the name of the output dataset
    :param out_path: type: str,specify the save path
    :return: type:str. the specific path of the output file
    '''
    LEVEL = level.upper()
    if LEVEL not in ['NET','DEALER']:
        print("please input 'net' or 'dealer'!")

    df_name_dict={'NET':[
                            'cv_group1a_mini_net',
                            'cv_group1b_S12_net_new',
                            'cv_group2_S34X12_net_new',
                            'cv_group3_S5X34Z_net_new',
                            'cv_group4_S678X567_net_new',
                            'cv_group5_NEVZinoro_net',
                            'cv_group6_M_net'
                        ],
                  'DEALER':[
                               'cv_group1a_mini',
                               'cv_group1b_S12_new',
                               'cv_group2_S34X12_new',
                               'cv_group3_S5X34Z_new',
                               'cv_group4_S678X567_new',
                               'cv_group5_NEVZinoro',
                               'cv_group6_M'
                            ]
                 }

    los_p_dict = {
                    'NET':'LOS_P_NET',
                    'DEALER':'LOS_P_DL'
                 }
    #df_name中包含了数据库7张数据表的表名
    DF_NAME = df_name_dict[LEVEL]
    #LOS_P_NAME为流失概率的名称
    LOS_P_NAME = los_p_dict[LEVEL]
    car_group_name = [x.split('_')[1] for x in DF_NAME]
    all_df = []
    for i,j in zip(DF_NAME,car_group_name):
        sql_text = "select * from " + str(i)
        i = pd.read_sql(sql_text, engine)
        i.columns = list(map(str.upper, i.columns))
        i['CAR_GROUP'] = j
        all_df.append(i)

    #纵向拼接7张表
    df = pd.concat(all_df, axis=0, copy=False)
    #将LOS_P_DL的缺失值替换为1（业务含义：）
    df[LOS_P_NAME] = df[LOS_P_NAME].fillna(1)
    out_file_name = 'Combined_'+ LEVEL + '_' + str(time.strftime("%Y%m%d"))
    out_file_path = out_path + '\\'+str(out_file_name) + '.csv'

    #保存拼接后的数据集
    df.to_csv(out_file_path)
    print("{} data saved as '{}'.".format(LEVEL,out_file_path))
    return out_file_path


def read_OverSize_data(path, Chunksize=1000000):
    reader = pd.read_csv(path, index_col=0, iterator=True, low_memory=False)
    loop = True
    chunkSize = Chunksize
    chunks = []
    i = 1
    while loop:
        try:
            chunk = reader.get_chunk(chunkSize)
            chunks.append(chunk)
            print(Chunksize * i)
            i = i + 1
        except StopIteration:
            loop = False
            print ("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    gc.collect()
    return df

# var_list_dict定义了使用不同评分函数的变量列表
var_list_dict = {
        'NET': {
            'f_score_var_list': [
                'LOY_IND_DL',
                'LOY_IND_CT',
                'NET_C_R_SUM',
                'NET_INSURANCE_SUM',
                'NET_WARR_SUM',
                'NET_VAL_PARTS_ACCIDENT',
                'NET_VAL_PARTS_ELECTRONICS',
                'NET_VAL_PARTS_MAINTENANCE',
                'NET_VAL_PARTS_REPAIR',
                'NET_VAL_PARTS_WEAR',
                'NET_VAL_ACCESSORIES',
                'NET_VAL_LIFESTYLE',
                'NET_NONMAINT_CLEANSING_SUM',
                'NET_TOTAL_SUM',
                'NET_AVG_SUM_MILE',
                'NET_AVG_SUM_AGE',
                'NET_LAST_TOTAL_SUM',
                'NET_LAST_C_R_SUM',
                'NET_LAST_INSURANCE_SUM',
                'NET_LAST_WARR_SUM',
                'TOTAL_COUNT_NET',
                'NET_C_R_COUNT',
                'NET_INSURANCE_COUNT',
                'NET_WARR_COUNT',
                'NET_LAST_TOTAL_COUNT',
                'SRP_NET',
                'NEW_RETAIL_PRICE'
            ],
            'g_score_var_list': [
                'TOTAL_DEALER_NET',
                'TOTAL_CITY_NET',
                'TOTAL_DEALER_L1',
                'TOTAL_CITY_L1',
                'LOS_P_NET'
            ]
        },
        'DEALER': {
            'f_score_var_list': [
                'SRP_PCT',
                'C_R_SUM',
                'INSURANCE_SUM',
                'WARR_SUM',
                'VAL_PARTS_ACCIDENT',
                'VAL_PARTS_ELECTRONICS',
                'VAL_PARTS_MAINTENANCE',
                'VAL_PARTS_REPAIR',
                'VAL_PARTS_WEAR',
                'VAL_ACCESSORIES',
                'VAL_LIFESTYLE',
                'NONMAINT_CLEANSING_SUM',
                'TOTAL_SUM',
                'AVG_SUM_MILE',
                'AVG_SUM_AGE',
                'LAST_TOTAL_SUM',
                'LAST_C_R_SUM',
                'LAST_INSURANCE_SUM',
                'LAST_WARR_SUM',
                'TOTAL_COUNT',
                'C_R_COUNT',
                'INSURANCE_COUNT',
                'WARR_COUNT',
                'LAST_TOTAL_COUNT',
                'NEW_RETAIL_PRICE',
                'TOTAL_COUNT_NET',
                'SRP_NET'
            ],
            'g_score_var_list': [
                'TOTAL_DEALER_NET',
                'TOTAL_CITY_NET',
                'TOTAL_DEALER_L1',
                'TOTAL_CITY_L1',
                'LOS_P_DL'
            ],
            'nodup_var_list':[
                'TOTAL_COUNT_NET',
                'SRP_NET',
                'TOTAL_DEALER_NET',
                'TOTAL_CITY_NET',
                'TOTAL_DEALER_L1',
                'TOTAL_CITY_L1'
            ]

        }

    }
# scored_var_dict定义了各变量权重
scored_var_dict = {
        'NET': {'Loyalty_vars': [
            'rn_LOY_IND_DL',
            'rn_LOY_IND_CT',
            'rn_TOTAL_DEALER_NET',
            'rn_TOTAL_CITY_NET',
            'rn_TOTAL_DEALER_L1',
            'rn_TOTAL_CITY_L1'
        ],
            'Consumption_vars': [
                'rn_NET_C_R_SUM',
                'rn_NET_INSURANCE_SUM',
                'rn_NET_WARR_SUM',
                'rn_NET_VAL_PARTS_ACCIDENT',
                'rn_NET_VAL_PARTS_ELECTRONICS',
                'rn_NET_VAL_PARTS_MAINTENANCE',
                'rn_NET_VAL_PARTS_REPAIR',
                'rn_NET_VAL_PARTS_WEAR',
                'rn_NET_VAL_ACCESSORIES',
                'rn_NET_VAL_LIFESTYLE',
                'rn_NET_NONMAINT_CLEANSING_SUM'
            ],
            'Consumption_Ability_vars': [
                'rn_NET_TOTAL_SUM',
                'rn_NET_AVG_SUM_MILE',
                'rn_NET_AVG_SUM_AGE',
                'rn_NET_LAST_TOTAL_SUM',
                'rn_NET_LAST_C_R_SUM',
                'rn_NET_LAST_INSURANCE_SUM',
                'rn_NET_LAST_WARR_SUM',
                'rn_TOTAL_COUNT_NET',
                'rn_NET_C_R_COUNT',
                'rn_NET_INSURANCE_COUNT',
                'rn_NET_WARR_COUNT',
                'rn_NET_LAST_TOTAL_COUNT',
                'rn_SRP_NET'
            ],
            'Loss_var': 'rn_LOS_P_NET',
            'Loyalty_weights': [5, 4, 2, 0, 3, 1],
            'Consumption_weights': [10, 7, 5, 6, 3, 9, 4, 8, 1, 0, 2],
            'Consumption_Ability_weights': [12, 10, 11, 4, 3, 1, 0, 9, 8, 6, 5, 2, 7]
        },
        'DEALER': {
            'Loyalty_vars': [
                'rn_SRP_PCT',
                'rn_TOTAL_DEALER_NET',
                'rn_TOTAL_CITY_NET',
                'rn_TOTAL_DEALER_L1',
                'rn_TOTAL_CITY_L1'
            ],
            'Consumption_vars': [
                'rn_C_R_SUM',
                'rn_INSURANCE_SUM',
                'rn_WARR_SUM',
                'rn_VAL_PARTS_ACCIDENT',
                'rn_VAL_PARTS_ELECTRONICS',
                'rn_VAL_PARTS_MAINTENANCE',
                'rn_VAL_PARTS_REPAIR',
                'rn_VAL_PARTS_WEAR',
                'rn_VAL_ACCESSORIES',
                'rn_VAL_LIFESTYLE',
                'rn_NONMAINT_CLEANSING_SUM'
            ],
            'Consumption_Ability_vars': [
                'rn_TOTAL_SUM',
                'rn_AVG_SUM_MILE',
                'rn_AVG_SUM_AGE',
                'rn_LAST_TOTAL_SUM',
                'rn_LAST_C_R_SUM',
                'rn_LAST_INSURANCE_SUM',
                'rn_LAST_WARR_SUM',
                'rn_TOTAL_COUNT_NET',
                'rn_TOTAL_COUNT',
                'rn_C_R_COUNT',
                'rn_INSURANCE_COUNT',
                'rn_WARR_COUNT',
                'rn_LAST_TOTAL_COUNT',
                'rn_SRP_NET'
            ],
            'Loss_var': 'rn_LOS_P_DL',
            'Loyalty_weights': [4, 2, 0, 3, 1],
            'Consumption_weights': [10, 7, 5, 6, 3, 9, 4, 8, 1, 0, 2],
            'Consumption_Ability_weights': [13, 11, 12, 4, 3, 1, 0, 10, 9, 8, 6, 5, 2, 7]
        }
    }

dr_var_dict = {
        'NET': {
            'Loss_Name': 'dr_Loss_net',
            'Loyalty_Name': 'dr_Loyalty_net',
            'Consumption_Name': 'dr_Consumption_Category_net',
            'Consumption_Ability_Name': 'dr_Consumption_Ability_net',
            'Loss_Weight': 1,
            'Loyalty_Weight': 1 / 15,
            'Consumption_Weight': 1 / 55,
            'Consumption_Ability_Weight': 1 / 78
        },
        'DEALER': {
            'Loss_Name': 'dr_Loss_dl',
            'Loyalty_Name': 'dr_Loyalty_dl',
            'Consumption_Name': 'dr_Consumption_Category_dl',
            'Consumption_Ability_Name': 'dr_Consumption_Ability_dl',
            'Loss_Weight': 1,
            'Loyalty_Weight': 0.1,
            'Consumption_Weight': 1 / 55,
            'Consumption_Ability_Weight': 1 / 91
        }
    }


def Combine_Dealer_with_NET(dealer_df, net_df_path):
    f = open('features_from_net.txt')
    net_df_for_use_col = []
    for i in f.readlines():
        net_df_for_use_col.append(i.strip('\n'))
    f.close()
    net_df_for_use_col.append('CHASSISNO')
    net_df = read_OverSize_data(net_df_path, 1000000)
    df = pd.merge(dealer_df, net_df[net_df_for_use_col], how='left', on='CHASSISNO')
    return df

def SCORE(df,level,NET_df_PATH = None):
    LEVEL = level.upper()
    f_score_var_list = var_list_dict[LEVEL]['f_score_var_list']
    g_score_var_list = var_list_dict[LEVEL]['g_score_var_list']
    score_var_list = f_score_var_list + g_score_var_list

    def get_quantile_stat(x):
        pmin = x.min()
        pmax = x.max()
        p25 = x.quantile(0.25)
        p50 = x.quantile(0.5)
        p75 = x.quantile(0.75)
        return p25, p50, p75, pmin, pmax

    def f_score(x,p25, p50, p75, pmin, pmax):
        '''

        :param x:
        :param p25:
        :param p50:
        :param p75:
        :param pmin:
        :param pmax:
        :return:
        '''
        score = 0
        if np.isnan(x) or x == 0:
                score = 0
        else:
            # 1段分，1种情况
            if (pmin == p25) and (p25 == p50) and (p50 == p75) and (p75 == pmax):
                score = 2.5
            # 2段分，2种情况
            elif (pmin == p25) and (p25 == p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p75):
                    score = 1
                if (x > p75) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 == p50) and (p50 == p75) and (p75 == pmax):
                if (x >= pmin) and (x < p25):
                    score = 1
                if (x >= p25) and (x <= pmax):
                    score = 4
            # 3段分，5种情况
            elif (pmin == p25) and (p25 == p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p50):
                    score = 1
                if (x > p50) and (x <= p75):
                    score = 3
                if (x > p75) and (x <= pmax):
                    score = 4

            elif (pmin == p25) and (p25 == p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p50):
                    score = 1
                if (x > p50) and (x <= p75):
                    score = 3
                if (x > p75) and (x <= pmax):
                    score = 4

            elif (pmin == p25) and (p25 < p50) and (p50 == p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x < p50):
                    score = 2
                if (x >= p50) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 < p50) and (p50 == p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x < p50):
                    score = 2
                if (x >= p50) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 == p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x < p25):
                    score = 1
                if (x >= p25) and (x <= p75):
                    score = 2.5
                if (x > p75) and (x <= pmax):
                    score = 4
            # 4段分，8种情况
            elif (pmin == p25) and (p25 < p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x <= p50):
                    score = 2
                if (x > p50) and (x < p75):
                    score = 3
                if (x >= p75) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 < p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x <= p50):
                    score = 2
                if (x > p50) and (x < p75):
                    score = 3
                if (x >= p75) and (x <= pmax):
                    score = 4

            elif (pmin == p25) and (p25 < p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x <= p50):
                    score = 2
                if (x > p50) and (x < p75):
                    score = 3
                if (x >= p75) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 < p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x <= p50):
                    score = 2
                if (x > p50) and (x < p75):
                    score = 3
                if (x >= p75) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 == p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x < p25):
                    score = 1
                if (x >= p25) and (x <= p50):
                    score = 2
                if (x > p50) and (x < p75):
                    score = 3
                if (x >= p75) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 == p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x < p25):
                    score = 1
                if (x >= p25) and (x <= p50):
                    score = 2
                if (x > p50) and (x < p75):
                    score = 3
                if (x >= p75) and (x <= pmax):
                    score = 4

            elif (pmin == p25) and (p25 < p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x < p50):
                    score = 2
                if (x >= p50) and (x <= p75):
                    score = 3
                if (x > p75) and (x <= pmax):
                    score = 4

            elif (pmin < p25) and (p25 < p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 1
                if (x > p25) and (x < p50):
                    score = 2
                if (x >= p50) and (x <= p75):
                    score = 3
                if (x > p75) and (x <= pmax):
                    score = 4
        return score

    def g_score(x, p25, p50, p75, pmin, pmax):
        '''

        :param x:
        :param p25:
        :param p50:
        :param p75:
        :param pmin:
        :param pmax:
        :return:
        '''
        score = 0
        if np.isnan(x) or x == 0:
            score = 0
        else:
            # 1段分，1种情况
            if (pmin == p25) and (p25 == p50) and (p50 == p75) and (p75 == pmax):
                score = 2.5
            # 2段分，2种情况
            elif (pmin == p25) and (p25 == p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p75):
                    score = 4
                if (x > p75) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 == p50) and (p50 == p75) and (p75 == pmax):
                if (x >= pmin) and (x < p25):
                    score = 4
                if (x >= p25) and (x <= pmax):
                    score = 1
            # 3段分，5种情况
            elif (pmin == p25) and (p25 == p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p50):
                    score = 4
                if (x > p50) and (x <= p75):
                    score = 2
                if (x > p75) and (x <= pmax):
                    score = 1

            elif (pmin == p25) and (p25 == p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p50):
                    score = 4
                if (x > p50) and (x <= p75):
                    score = 2
                if (x > p75) and (x <= pmax):
                    score = 1

            elif (pmin == p25) and (p25 < p50) and (p50 == p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x < p50):
                    score = 3
                if (x >= p50) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 < p50) and (p50 == p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x < p50):
                    score = 3
                if (x >= p50) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 == p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x < p25):
                    score = 4
                if (x >= p25) and (x <= p75):
                    score = 2.5
                if (x > p75) and (x <= pmax):
                    score = 1
            # 4段分，8种情况
            elif (pmin == p25) and (p25 < p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x <= p50):
                    score = 3
                if (x > p50) and (x < p75):
                    score = 2
                if (x >= p75) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 < p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x <= p50):
                    score = 3
                if (x > p50) and (x < p75):
                    score = 2
                if (x >= p75) and (x <= pmax):
                    score = 1

            elif (pmin == p25) and (p25 < p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x <= p50):
                    score = 3
                if (x > p50) and (x < p75):
                    score = 2
                if (x >= p75) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 < p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x <= p50):
                    score = 3
                if (x > p50) and (x < p75):
                    score = 2
                if (x >= p75) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 == p50) and (p50 < p75) and (p75 == pmax):
                if (x >= pmin) and (x < p25):
                    score = 4
                if (x >= p25) and (x <= p50):
                    score = 3
                if (x > p50) and (x < p75):
                    score = 2
                if (x >= p75) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 == p50) and (p50 < p75) and (p75 < pmax):
                if (x >= pmin) and (x < p25):
                    score = 4
                if (x >= p25) and (x <= p50):
                    score =  3
                if (x > p50) and (x < p75):
                    score = 2
                if (x >= p75) and (x <= pmax):
                    score = 1

            elif (pmin == p25) and (p25 < p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x < p50):
                    score = 3
                if (x >= p50) and (x <= p75):
                    score = 2
                if (x > p75) and (x <= pmax):
                    score = 1

            elif (pmin < p25) and (p25 < p50) and (p50 == p75) and (p75 < pmax):
                if (x >= pmin) and (x <= p25):
                    score = 4
                if (x > p25) and (x < p50):
                    score = 3
                if (x >= p50) and (x <= p75):
                    score = 2
                if (x > p75) and (x <= pmax):
                    score = 1
        return score

    def cal_threshold(df):
        '''
        计算所有需要评分的指标的各分位数
        :param df:
        :return:
        '''

        Thresholds = {}
        if LEVEL == 'DEALER':
            nodup_df = df.drop_duplicates(subset=['CHASSISNO'], keep='first')
            nodup_vars = var_list_dict[LEVEL]['nodup_var_list']

            for i in score_var_list:
                if i in nodup_vars:
                    tmp = list(get_quantile_stat(nodup_df[i]))
                elif i =='LOS_P_DL':
                    tmp_df = df[df['LOS_P_DL']!=1][i]
                    tmp = list(get_quantile_stat(tmp_df))
                else:
                    tmp = list(get_quantile_stat(df[i]))
                Thresholds[i] = tmp

        if LEVEL =='NET':
            for i in score_var_list:
                if i =='LOS_P_NET':
                    tmp_df = df[df['LOS_P_NET']!=1][i]
                    tmp = list(get_quantile_stat(tmp_df))
                else:
                    tmp = list(get_quantile_stat(df[i]))
                Thresholds[i] = tmp
        return Thresholds



    def remark(Thresholds,var_name,x):
        '''
        打分
        :param var_name:
        :return:
        '''
        threashold_stat = Thresholds[var_name]
        if var_name in f_score_var_list:
            scores = f_score(x,*threashold_stat)
        else:
            scores =  g_score(x,*threashold_stat)
        return scores
    if LEVEL == 'DEALER':
        df = Combine_Dealer_with_NET(df,net_df_path=NET_df_PATH)

    df['NEW_RETAIL_PRICE'] = [x if x > 150000 else 0 for x in df['RETAIL_PRICE']]
    Thresholds = cal_threshold(df)

    after_score_var_list = []
    for var in score_var_list:
        df['rn_'+str(var)]=df[var].map(lambda x:remark(Thresholds,var,x))
        after_score_var_list.append('rn_'+str(var))


    Loss_var = scored_var_dict[LEVEL]['Loss_var']
    Loyalty_vars = scored_var_dict[LEVEL]['Loyalty_vars']
    Consumption_vars = scored_var_dict[LEVEL]['Consumption_vars']
    Consumption_Ability_vars = scored_var_dict[LEVEL]['Consumption_Ability_vars']
    Loyalty_weights = scored_var_dict[LEVEL]['Loyalty_weights']
    Consumption_weights = scored_var_dict[LEVEL]['Consumption_weights']
    Consumption_Ability_weights = scored_var_dict[LEVEL]['Consumption_Ability_weights']
    Loss_Name = dr_var_dict[LEVEL]['Loss_Name']
    Loyalty_Name = dr_var_dict[LEVEL]['Loyalty_Name']
    Consumption_Name = dr_var_dict[LEVEL]['Consumption_Name']
    Consumption_Ability_Name =dr_var_dict[LEVEL]['Consumption_Ability_Name']
    Loss_dr_Weight = dr_var_dict[LEVEL]['Loss_Weight']
    Loyalty_dr_Weight = dr_var_dict[LEVEL]['Loyalty_Weight']
    Consumption_dr_Weight = dr_var_dict[LEVEL]['Consumption_Weight']
    Consumption_Ability_dr_Weight = dr_var_dict[LEVEL]['Consumption_Ability_Weight']

    df[Loss_var] = list(map(lambda x,y: 0 if y == 1 else x,df[Loss_var],df[Loss_var[3:]]))
    df[Loss_Name] = Loss_dr_Weight * df[Loss_var]
    df[Loyalty_Name] = Loyalty_dr_Weight * np.dot(df[Loyalty_vars].values,Loyalty_weights)
    df[Consumption_Name] = Consumption_dr_Weight * np.dot(df[Consumption_vars].values,Consumption_weights)
    df[Consumption_Ability_Name] = Consumption_Ability_dr_Weight * np.dot(df[Consumption_Ability_vars].values,Consumption_Ability_weights)


    after_score_var_list.extend(list(dr_var_dict[LEVEL].values())[:4])

    df[after_score_var_list]=df[after_score_var_list].applymap(lambda x: float('%.1f' %x))
    df=df.drop('NEW_RETAIL_PRICE',axis=1)
    df.rename(columns = {'rn_NEW_RETAIL_PRICE': 'remarking1_retail_price'}, inplace = True)
    #df['remarking1_retail_price'] = df['rn_NEW_RETAIL_PRICE']
    return df

import os
import pandas as pd
import numpy
def Cluster_for_NET(data,cluster_number=6):
    from sklearn.cluster import KMeans
    Loss_var = 'LOS_P_NET'
    cluster_feature = ['dr_Loyalty_net', 'dr_Loss_net', 'dr_Consumption_Category_net', 'dr_Consumption_Ability_net']
    # 聚类
    kmeans = KMeans(n_clusters=cluster_number, n_jobs=4)  # n_jobs是并行数，一般等于CPU数较好
    train = data[data[Loss_var]!= 1][cluster_feature]
    model = kmeans.fit(train)
    # 统计各个类别的数目
    r1 = pd.Series(model.labels_).value_counts()
    # 找出聚类中心
    r2 = pd.DataFrame(model.cluster_centers_)
    # 横向连接（0是纵向），得到聚类中心对应的类别下的数目
    r = pd.concat([r2, r1], axis=1)
    r.columns = ['dr_Loyalty_net',
                 'dr_Loss_net',
                 'dr_Consumption_Category_net',
                 'dr_Consumption_Ability_net',
                 'sample_count']
    # 增加一列，表明聚类数目
    r['cluster_group'] = cluster_number

    data['category'] = list(
        map(lambda x, y: model.predict(y.reshape(1, 4))[0] if x < 1 else 'loss', data['LOS_P_NET'],
            data[cluster_feature].values))
    r.to_csv('Clustered_central.csv')
    return r,data

def get_mean_of_var(data):
    varlist=['dr_Loyalty_net','dr_Loss_net','dr_Consumption_Category_net','dr_Consumption_Ability_net', 'remarking1_retail_price']
    outfile = 'category_mean'
    data.groupby('category')[varlist].mean().to_csv(str(outfile)+'.csv')
    #总体均值
    data[varlist].mean()
    df_type = pd.DataFrame(data['category'].value_counts())
    #获得var的名单
    var_list = pd.read_table('features_for_cluster.txt',header=None).iloc[:,0]
    #将list中的变量统一大写，以匹配宽表变量。
    var_list = list(var_list.map(lambda x: x.upper()))
    #对age_year变量重新分组
    bins = [min(data.AGE_YEAR)-1, 1, 3, 5, 6, max(data.AGE_YEAR)+1]#分组的数组
    labels = ['1年以下','1年到3年','3年到5年','5年到6年','6年以上']
    AGE_YEAR_GROUP= pd.cut(data.AGE_YEAR, bins, right = True, labels=labels)#right=true表示该区间右边闭合
    data['AGE_YEAR_GROUP'] = AGE_YEAR_GROUP
    return data


def Combine_for_tag(data, add_table_list):
    for i in add_table_list:
        sql_text = "select * from " + str(i)
        i = pd.read_sql(sql_text, engine)
        i.columns = list(map(str.upper, i.columns))
        data = data.merge(i, how='left', on='CHASSISNO', copy=False)
        del i
        gc.collect()
    for i in add_table_list:
        del i
    gc.collect()
    return data


Add_table_list = ['srp_cnt_net_0909_table', 'QTY_TIRES_NET_0909', 'QTY_BATTERY_NET_0909']
def Tag_for_NET(df):
    def execute_label(data, org_var, func_label):
        a = data[org_var].apply(func_label)
        gc.collect()
        return a
    # 车龄
    def age_year_label(x):
        if x < 0:
            return '<0'
        elif np.isnan(x) or x <= 3:
            return '0-3'
        elif x <= 7:
            return '4-7'
        elif x <= 10:
            return '8-10'
        elif x >= 11:
            return '>11'
        else:
            return 'other'

    # 总里程
    def mileage_label(x):
        if x < 0:
            return '<0'
        elif np.isnan(x) or x <= 30000:
            return '<30k'
        elif x <= 50000:
            return '30k~50k'
        elif x <= 80000:
            return '50k~80k'
        elif x <= 100000:
            return '80k~100k'
        elif x <= 150000:
            return '100k~150k'
        elif x > 150000:
            return '>150k'
        else:
            return 'other'

    # 年均里程
    def mileage_avg_label(x):
        if x < 0:
            return '<0'
        elif np.isnan(x) or x <= 5000:
            return '<5k'
        elif x <= 10000:
            return '5k~10k'
        elif x <= 15000:
            return '10k~15k'
        elif x <= 20000:
            return '15k~20k'
        elif x <= 25000:
            return '20k~25k'
        elif x <= 30000:
            return '25k~30k'
        elif x > 30000:
            return '>30k'
        else:
            return 'other'

    # 忠诚度
    def TOTAL_DEALER_NET_label(x):
        if x == 1:
            return '1'
        elif x == 2:
            return '2'
        elif x == 3:
            return '3'
        elif x > 3:
            return '>3'
        else:
            return 'other'

    # 历史光顾全网经销商城市数量
    def TOTAL_CITY_NET_label(x):
        if x == 1:
            return '1'
        elif x == 2:
            return '2'
        elif x == 3:
            return '3'
        elif x > 3:
            return '>3'
        else:
            return 'other'

    # 近一年光顾经销商数量
    def total_dealer_l1_label(x):
        if np.isnan(x) or x == 0:
            return '0'
        elif x == 1:
            return '1'
        elif x == 2:
            return '2'
        elif x == 3:
            return '3'
        elif x > 3:
            return '>3'
        else:
            return 'other'

    # 近一年光顾城市数量
    def total_city_l1_label(x):
        if np.isnan(x) or x == 0:
            return '0'
        elif x == 1:
            return '1'
        elif x == 2:
            return '2'
        elif x == 3:
            return '3'
        elif x > 3:
            return '>3'
        else:
            return 'other'

    # 是否BSI
    def if_BSI(x):
        if x == 'BSI':
            return 'Yes'
        else:
            return 'No'

    # 是否为二手车
    def if_second(x):
        if pd.isnull(x):
            return 'New'
        else:
            return 'Used'

    # 是否购买精品
    def if_net_val_accessories(x):
        if np.isnan(x):
            return 'No'
        else:
            return 'Yes'

    # 是否购买附件
    def if_net_val_lifestyle(x):
        if np.isnan(x):
            return 'No'
        else:
            return 'Yes'

    ## 保养Complete度 - 轮胎
    def SRP_tires_label(x):

        try:
            a = math.ceil(x)
            if a == 0:
                return '0'
            elif a == 1:
                return '1'
            elif a == 2:
                return '2'
            elif a == 3:
                return '3'
            elif a == 4:
                return '4'
            elif a > 4:
                return '>4'
            else:
                return 'other'
        except:
            return 'other'
            ## 保养Complete度 - 电瓶

    def SRP_battery_label(x):
        try:
            a = math.ceil(x)
            if a == 0:
                return '0'
            elif a == 1:
                return '1'
            elif a == 2:
                return '2'
            elif a >= 3:
                return '>=3'
            else:
                return 'other'
        except:
            return 'other'

    def SRP_001_label(x, y, z):
        try:
            judge_value = math.floor(max(x / 10000, y / 365))
            if np.isnan(z) :
                z=0
            if z >= judge_value:
                return 'Complete'
            elif z >= judge_value * 0.8:
                return 'Middle-complete'
            elif z < judge_value * 0.8 :
                return 'Incomplete'
        except:
            return 'other'

    def SRP_filter_label(x, y, z):
        try:
            judge_value = max(x, y, z)
            a = sum([x, y, z])
            if 1 / 2 * a <= judge_value:
                return 'Complete'
            elif 1 / 2 * a > judge_value:
                return 'Incomplete'
            else:
                return 'other'
        except:
            return 'other'

    def SRP_zhidongpian_label(x, y, z):
        try:
            judge_value = math.floor(z / 40000)
            a = sum([x, y])
            if np.isnan(a):
                a=0
            if a >= 2 * judge_value:
                return 'Complete'
            elif a >= judge_value:
                return 'Middle-complete'
            elif a < judge_value:
                return 'Incomplete'
            else:
                return 'other'
        except:
            return 'other'

    def SRP_zhidongpan_label(x, y, z):
        try:
            judge_value = math.floor(z / 90000)
            a = sum([x, y])
            if np.isnan(a):
                a=0
            if a >= 2 * judge_value:
                return 'Complete'
            elif a >= judge_value:
                return 'Middle-complete'
            elif a < judge_value:
                return 'Incomplete'
            else:
                return 'other'
        except:
            return 'other'

    def SRP_huohuasai_label(x, z):
        try:
            judge_value = math.floor(z / 37500)
            if np.isnan(x):
                x = 0
            if x >= judge_value:
                return 'Complete'
            elif x < judge_value:
                return 'Incomplete'
            else:
                return 'other'
        except:
            return 'other'

    def SRP_zhidongye_label(x, z):
        try:
            judge_value = math.floor(z / 365 / 3)
            if np.isnan(x):
                x = 0
            if x >= judge_value:
                return 'Complete'
            elif x < judge_value:
                return 'Incomplete'
            else:
                return 'other'
        except:
            return 'other'

    def SRP_yuguapian_label(x, z):
        try:
            judge_value = math.floor(z / 365 / 2)
            if np.isnan(x):
                x = 0
            if x >= judge_value:
                return 'Complete'
            elif x < judge_value:
                return 'Incomplete'
            else:
                return 'other'
        except:
            return 'other'


    label_func_dict = {
                        'AGE_YEAR': age_year_label,
                        'LAST_MILEAGE_ALLITEM': mileage_label,
                        'PRE_YEAR_MILEAGE': mileage_avg_label,
                        'TOTAL_DEALER_NET': TOTAL_DEALER_NET_label,
                        'TOTAL_CITY_NET': TOTAL_CITY_NET_label,
                        'TOTAL_DEALER_L1': total_dealer_l1_label,
                        'TOTAL_CITY_L1': total_city_l1_label,
                        'BSI_CAR': if_BSI,
                        'MAX_CHANGEDT': if_second,
                        'NET_VAL_ACCESSORIES': if_net_val_accessories,
                        'NET_VAL_LIFESTYLE': if_net_val_lifestyle,
                        'QTY_TIRES_NET': SRP_tires_label,
                        'QTY_BATTERY_NET': SRP_battery_label
                        }






    _,df = Cluster_for_NET(df)
    df = get_mean_of_var(df)
    df = Combine_for_tag(df,Add_table_list)

    for i in label_func_dict.items():
        df[i[0] + '_LABEL'] = execute_label(df, i[0], i[1])

    df['SRP001' + '_if_complete_LABEL'] = list(map(lambda x, y, z: SRP_001_label(x, y, z), df['LAST_MILEAGE_ALLITEM'], df['AGE_DAY'], df['SRP001__CNT_NET']))
    df['filter' + '_if_complete_LABEL'] = list(map(lambda x, y, z: SRP_filter_label(x, y, z), df['SRP002__CNT_NET'], df['SRP003__CNT_NET'],df['SRP011__CNT_NET']))
    df['brake_pad' + '_if_complete_LABEL'] = list(map(lambda x, y, z: SRP_zhidongpian_label(x, y, z), df['SRP006__CNT_NET'], df['SRP008__CNT_NET'],df['LAST_MILEAGE_ALLITEM']))
    df['brake_disc' + '_if_complete_LABEL'] = list(map(lambda x, y, z: SRP_zhidongpan_label(x, y, z), df['SRP007__CNT_NET'], df['SRP009__CNT_NET'],df['LAST_MILEAGE_ALLITEM']))
    df['brake_fluid_LABEL'] = list(map(lambda x, y: SRP_zhidongye_label(x, y), df['SRP010__CNT_NET'], df['AGE_DAY']))
    df['wiper_blade_LABEL'] = list(map(lambda x, y: SRP_yuguapian_label(x, y), df['SRP005__CNT_NET'], df['AGE_DAY']))
    df['spark_plug_LABEL'] = list(map(lambda x, y: SRP_huohuasai_label(x, y), df['SRP004__CNT_NET'], df['LAST_MILEAGE_ALLITEM']))

    return df

def Tag_for_Dealer(df):
    def excute_label(df, org_var, func_label):
        return df[org_var].apply(func_label)

    def excute_label_pct(df, org_var, func_label):
        return df[org_var].apply(func_label)

    # 创建标签执行代码
    def run_execute_label(df, org_var, FUNC):
        '''
        i, the execute order, nothing usefulness
        org_var, orginal variable
        FUNC, Label Function
        '''
        var = org_var + '_LABEL'
        df[var] = excute_label(df, org_var=org_var, func_label=FUNC)

    def run_execute_label_pct(df, org_var, FUNC):
        '''
        i, the execute order, nothing usefulness
        org_var, orginal variable
        FUNC, Label Function
        var, the pct_label
        var_pct, pct
        var
        '''
        var_pct = org_var + '_PCT'
        if org_var == 'TOTAL_COUNT':
            var_net = org_var + '_NET'
        else:
            var_net = 'NET_' + org_var

        # 算占比
        df[var_pct] = df[org_var] / df[var_net]
        var = org_var + '_PCT_LABEL'
        df[var] = excute_label_pct(df, org_var=var_pct, func_label=FUNC)

    # 过去一年进店次数
    def LAST_COUNT_LABEL(x):
        if np.isnan(x):
            return '0'
        elif x == 1:
            return '1'
        elif x == 2:
            return '2'
        elif x > 2:
            return '>2'
        else:
            return 'Other'

    org_var_list = ['LAST_TOTAL_COUNT',
                    'LAST_OIL_COUNT',
                    'LAST_INSURANCE_COUNT',
                    'LAST_WARR_COUNT',
                    'LAST_INTERNAL_COUNT',
                    'LAST_BSI_COUNT',
                    'LAST_C_R_COUNT',
                    'LAST_TOTALALL_COUNT', ]

    for each_item in org_var_list:
        run_execute_label(df, each_item, LAST_COUNT_LABEL)

    # 过去一年总额
    def LAST_SUM_LABEL(x):
        if np.isnan(x) or x == 0:
            return '0'
        elif x < 1000:
            return '<1k'
        elif x <= 3000:
            return '1k~3k'
        elif x <= 5000:
            return '3k~5k'
        elif x <= 10000:
            return '5k~10k'
        elif x > 10000:
            return '>10k'
        else:
            return 'Other'

    org_var_list = ['LAST_TOTAL_SUM',
                    'LAST_OIL_SUM',
                    'LAST_INSURANCE_SUM',
                    'LAST_WARR_SUM',
                    'LAST_INTERNAL_SUM',
                    'LAST_BSI_SUM',
                    'LAST_C_R_SUM',
                    'LAST_TOTALALL_SUM']

    for each_item in org_var_list:
        run_execute_label(df, each_item, LAST_SUM_LABEL)

        # 赋值
    org_var = 'SRP_PCT'
    var = org_var + '_LABEL'

    # 在本店保养百分比占比

    def SRP_PCT_LABEL(x):
        if np.isnan(x) or x <= 0.25:
            return '0~25%'
        elif x <= 0.5:
            return '25%~50%'
        elif x <= 0.75:
            return '50%~75%'
        elif x <= 1:
            return '75%~100%'
        else:
            return 'Other'

    # 执行
    df[var] = excute_label(df=df, org_var=org_var, func_label=SRP_PCT_LABEL)

    # 流失率
    def LOS_LABEL(x):
        if np.isnan(x) or x > 0.85:
            return '******'
        elif x > 0.68:
            return '*****'
        elif x > 0.51:
            return '****'
        elif x > 0.34:
            return '***'
        elif x > 0.17:
            return '**'
        elif x > 0:
            return '*'
        else:
            return 'Other'

    df['LOS_LEVEL_DL'] = df['LOS_P_DL'].apply(LOS_LABEL)

    # 过去一年占比
    def PCT_LABEL(x):
        if np.isnan(x) or x <= 0.25:
            return '0~25%'
        elif x <= 0.5:
            return '25%~50%'
        elif x <= 0.75:
            return '50%~75%'
        elif x <= 1:
            return '75%~100%'
        else:
            return 'Other'

    # 所有占比
    org_var_list = ['TOTAL_SUM',
                    'C_R_SUM',
                    'INSURANCE_SUM',
                    'WARR_SUM',
                    'TOTALALL_SUM',
                    'BSI_SUM',
                    'OIL_SUM',
                    'INTERNAL_SUM',
                    'LAST_TOTAL_SUM',
                    'LAST_C_R_SUM',
                    'LAST_INSURANCE_SUM',
                    'LAST_WARR_SUM',
                    'LAST_TOTALALL_SUM',
                    'LAST_BSI_SUM',
                    'LAST_OIL_SUM',
                    'LAST_INTERNAL_SUM',
                    'TOTAL_COUNT',
                    'C_R_COUNT',
                    'INSURANCE_COUNT',
                    'WARR_COUNT',
                    'TOTALALL_COUNT',
                    'BSI_COUNT',
                    'OIL_COUNT',
                    'INTERNAL_COUNT',
                    'LAST_TOTAL_COUNT',
                    'LAST_C_R_COUNT',
                    'LAST_INSURANCE_COUNT',
                    'LAST_WARR_COUNT',
                    'LAST_TOTALALL_COUNT',
                    'LAST_BSI_COUNT',
                    'LAST_OIL_COUNT',
                    'LAST_INTERNAL_COUNT']

    for each_item in org_var_list:
        run_execute_label_pct(df, each_item, PCT_LABEL)

    def AVR_COUNT_LABEL(x):
        if np.isnan(x):
            result = 'missing'
        elif 0 <= x and x <= 1:
            result = '0~1'
        elif 1 < x and x <= 2:
            result = '1~2'
        elif 2 < x and x <= 3:
            result = '2~3'
        elif 3 < x and x <= 4:
            result = '3~4'
        elif 4 < x and x <= 5:
            result = '4~5'
        elif 5 < x:
            result = '>5'
        else:
            result = 'Other'
        return result

    def AVR_SUM_LABEL(x):
        if np.isnan(x):
            result = 'missing'
        elif x == 0:
            result = 0
        elif 0 < x and x <= 1000:
            result = '0~1K'
        elif 1000 < x and x <= 3000:
            result = '1k~3k'
        elif 3000 < x and x <= 5000:
            result = '3k~5k'
        elif 5000 < x and x <= 10000:
            result = '5k~10k'
        elif 10000 < x:
            result = '>10k'
        else:
            result = 'Other'
        return result

    org_var_list = ['TOTAL',
                      'C_R',
                      'INSURANCE',
                      'WARR',
                      'TOTALALL',
                      'BSI',
                      'OIL',
                      'INTERNAL'
                    ]

    for i in org_var_list:
        df[i + '_COUNT_AVR_LABEL'] = (df[i + '_COUNT'] / df['AGE_YEAR']).apply(AVR_COUNT_LABEL)
        df[i+'_SUM_AVR_LABEL'] = (df[i + '_SUM'] / df['AGE_YEAR']).apply(AVR_SUM_LABEL)

    return df


output_var_dict = {
                    'NET': 'final_output_order_net.txt',
                    'DEALER': 'final_output_order_dealer.txt'
                    }


def Tag(df, level, out_path=os.getcwd()):

    LEVEL = level.upper()
    if LEVEL == 'NET':
        tagged_df = Tag_for_NET(df)
    if LEVEL == 'DEALER':
        tagged_df = Tag_for_Dealer(df)
    out_name = 'TAG_'+LEVEL

    output_var_order = list(pd.read_table(output_var_dict[LEVEL],header=None).iloc[:,0])

    tag_data_path = out_path + '\\' + out_name + '.csv'
    tagged_df[output_var_order].to_csv(tag_data_path)
    print("{} saved at {}".format(out_name, tag_data_path))
    return tag_data_path

if __name__ == '__main__':
    main()