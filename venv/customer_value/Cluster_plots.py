# 作者 evan   
# 日期 2018/10/11   
# 时间 7:29   
# PyCharm

class Customer_value_cluster():
    Var_list = ['AGE_YEAR',
                'LAST_MILEAGE_ALLITEM',
                'LOY_IND_DL',
                'LOY_IND_CT',
                'TOTAL_DEALER_NET',
                'TOTAL_CITY_NET',
                'TOTAL_DEALER_L1',
                'TOTAL_CITY_L1',
                'LOS_P_NET',
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
                'category']
    def __init__(self,data,var_list=Var_list):
        self.var_list = var_list
        self.data = data

    def Fill_missing(self):
        self.data[self.var_list].fillna(0, inplace = True)

    def Cal_percentage(self, var, category_list = [0,1,2,3,4,5,'loss']):
        import numpy as np
        """

        :param var: variable
        :param data: dataset
        :param category_list:  
        :return: 
        """
        # 第一步：总体排序并计算分位数
        result = self.data[[var, 'category']].sort_values(by = [var])

        Q1 = np.percentile(result[var], 25)
        Q2 = np.percentile(result[var], 50)
        Q3 = np.percentile(result[var], 75)


        # 第二步：计算每个子类的数据，落入各分位数区间的个数
        per_list = []
        for i in range(len(category_list)):
            # print(i)
            df_by_type = self.data[result['category'] == category_list[i]]
            # print(len(df_by_type))

            cnt_less_than_Q1 = len(df_by_type[df_by_type[var] < Q1])
            cnt_less_than_Q2 = len(df_by_type[(df_by_type[var] >= Q1) & (df_by_type[var] < Q2)])
            cnt_less_than_Q3 = len(df_by_type[(df_by_type[var] >= Q2) & (df_by_type[var] < Q3)])
            cnt_less_than_Q4 = len(df_by_type[df_by_type[var] >= Q3])

            per_Q1 = cnt_less_than_Q1 / len(df_by_type)
            per_Q2 = cnt_less_than_Q2 / len(df_by_type)
            per_Q3 = cnt_less_than_Q3 / len(df_by_type)
            per_Q4 = cnt_less_than_Q4 / len(df_by_type)

            per_list.append([i, per_Q1, per_Q2, per_Q3, per_Q4])

        return per_list

    def Get_out_df(self):
        import pandas as pd
        percentage_list = {}
        for i in self.var_list:
            per_list = self.Cal_percentage(i, category_list=[0, 1, 2, 3, 4, 5])
            percentage_list[i] = per_list

        out_df_list = []

        for i in percentage_list.items():
            temp = pd.DataFrame(i[1], columns=['category', '0%~25%', '25%~50%', '50%~75%', '75%~100%'])
            temp['variable'] = i[0]
            out_df_list.append(temp)
        return out_df_list


    def Plot(self,var,df):

        import time
        import matplotlib.pyplot as plt
        import pylab
        # 画图大小
        pylab.rcParams['figure.figsize'] = (15.0, 8.0)

        # 字体设置
        font = {'family': 'normal',
                'weight': 'bold',
                'size': 20}
        plt.rc('font', **font)
        # 柱子大小
        barWidth = 0.85

        # 显示标签名称
        names = ('Group1', 'Group2', 'Group3', 'Group4', 'Group5', 'Group6')
        tmp = df[var]
        plt.bar(tmp['category'], tmp['0%~25%'], color='#667283', width=barWidth)
        plt.bar(tmp['category'], tmp['25%~50%'], bottom=tmp['0%~25%'], color='#91a1bd',
                width=barWidth)
        plt.bar(tmp['category'], tmp['50%~75%'],
                bottom=[i + j for i, j in zip(tmp['0%~25%'], tmp['25%~50%'])], color='#adb9cf',
                width=barWidth)
        plt.bar(tmp['category'], tmp['75%~100%'],
                bottom=[i + j + z for i, j, z in zip(tmp['0%~25%'], tmp['25%~50%'], tmp['50%~75%'])],
                color='#c9d1df', width=barWidth)

        # Custom x axis
        plt.xticks(tmp['category'], names)
        plt.xlabel(tmp['variable'][0])
        TIME = str(time.strftime("%Y%m%d"))
        pig_name = '_'.join([TIME,tmp['variable'][0],'.png'])
        # save
        plt.savefig(pig_name)  # 保存图片

    def Plot_all(self):
        out_df = self.Get_out_df()
        for i in range(out_df.shape[0]):
            self.Plot(i,out_df)

