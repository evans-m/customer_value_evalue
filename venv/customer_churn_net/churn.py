# 作者 evan
# 日期 2018/10/11
# 时间 7:27
# PyCharm


import pandas as pd
import time
import math
import os
import gc
from sqlalchemy import create_engine

def main():
    while True:
        print("please input the dataset name,if you want quit you can type 'quit'!")
        Table= input("input the dataset name here：")
        if Table == 'quit':
            exit()
        try:
            df_path = get_data_from_database(Table)
            break
        except:
            print("dataset {} does not exist!".format(Table))

    df = read_OverSize_data(df_path)
    df = preprocessing(df)

    model_path = os.getcwd() + '\\' +'xgb_20180828.model'
    try:
        result = predict_xgb(df,model_path)
    except:
        while True:
            print("please input the model file path,if you want quit you can type 'quit'!")
            model_path = input("input model file path here：")
            if model_path == 'quit':
                exit()
            try:
                result = predict_xgb(df,model_path)
                break
            except:
                print("can't' find the model file,please input again:")

    Time = time.strftime('%Y%m%d')
    result_name = 'churn_net_result_' + Time + '.csv'
    output_path = os.getcwd() + '\\' + result_name
    result[['chassisno', '流失概率', '流失概率区间', '流失等级']].to_csv(output_path, index=False)
    print('The outputFile', result_name, 'already been saved as', output_path)



def get_data_from_database(tablename, save_path=os.getcwd()):

    # 连接数据库的配置
    os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL') or '****'
    engine = create_engine(SQLALCHEMY_DATABASE_URI, encoding='utf8')  # create engine with sqlalchemy

    sql_text = "select * from " + str(tablename)
    df = pd.read_sql(sql_text, engine)
    df.columns = list(map(str.upper, df.columns))
    Save_path = save_path + '\\' + tablename
    df.to_csv(Save_path)
    return Save_path


def read_OverSize_data(path, Encode='UTF-8', Chunksize=1000000):
    reader = pd.read_csv(path, index_col=0, encoding=Encode, iterator=True, low_memory=False)
    loop = True
    chunkSize = Chunksize
    chunks = []
    i = 1
    print('please waite.')
    print('reading data...............')
    while loop:
        try:
            chunk = reader.get_chunk(chunkSize)
            chunks.append(chunk)
            print(Chunksize * i)
            i = i + 1
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    print('successfully reading data')
    gc.collect()
    return df


def Divide(x, y):
    if y != None:
        if x != 0:
            return float(y / x)
        else:
            return y
    else:
        return y


col_dict = {
    'base_col': [
        'SRP001__CLASS_CNT',
        'SRP002__CLASS_CNT',
        'SRP003__CLASS_CNT',
        'SRP004__CLASS_CNT',
        'SRP005__CLASS_CNT',
        'SRP006__CLASS_CNT',
        'SRP008__CLASS_CNT',
        'SRP010__CLASS_CNT',
        'SRP011__CLASS_CNT',
        'SRP012__CLASS_CNT'
    ],
    'ID': ['CHASSISNO'],
    'Preditors': [
        'D1__LOCAL_BSI_Y2_INV_CNT',
        'MAX_MILEAGE_PERM',
        'D1__LOCAL_BSI_Y1_AMT',
        'PURCHASE_PRICE',
        'D1__LOCAL_BSI_Y2_AMT',
        'D1__WARR_Y2_INV_CNT',
        'RETAIL_PRICE',
        'D2__LOCAL_BSI_Y2_INV_CNT',
        'D1__INSU_Y1_AMT',
        'D1__LOCAL_BSI_Y1_INV_CNT',
        'D1__WARR_Y2_AMT',
        'D1__SUB_DEALER_Y1_AMT',
        'MILE_MY1',
        'D1__SUB_DEALER_Y2_AMT',
        'AGE_MTH',
        'D1__SUB_DEALER_Y2_INV_CNT',
        'D1__SUB_DEALER_Y1_INV_CNT',
        'D1__INSU_Y1_INV_CNT',
        'MILE_MY2',
        'D1__FOURTH_Y2_INV_CNT',
        'D1__INTERNAL_Y1_INV_CNT',
        'D1__INSU_Y2_AMT',
        'D1__COUTER_Y2_AMT',
        'PARTS_IN_MIN_HALFYEAR',
        'D1__BSI_Y2_AMT',
        'D1__FOURTH_Y2_AMT',
        'D1__COUTER_Y1_INV_CNT',
        'D1__BSI_Y1_AMT',
        'D1__COUTER_Y1_AMT',
        'D1__FOURTH_Y1_AMT',
        'D1__INSU_Y2_INV_CNT',
        'D1__SALE_CNT_Y2',
        'LABOROTHER_MIN_HALFYEAR',
        'PARTS_IN_MTH_AVG',
        'PARTS_IN_MIN_YEAR',
        'MILEAGE_AVG_Y2',
        'PARTS_ACCIDT_THREEYEAR_AVG',
        'INV_CUST_AVG_M3Y',
        'D1__SALE_AMT_Y2',
        'MILEAGE_AVG_HALFY',
        'PTS_IN_MTH_AMT_P',
        'D1__INTERNAL_Y1_AMT',
        'LABOROTHER_AMT_YEAR_P',
        'D1__INTERNAL_Y2_INV_CNT',
        'LABOROTHER_AMT_YEAR',
        'D1__COUTER_Y2_INV_CNT',
        'INV_CUST_AMT_YEAR13',
        'MILEAGE_AVG_MTH',
        'PARTS_IN_HALFYEAR_AVG',
        'LABOROTHER_AVG_YEAR',
        'PTS_REPAIR_THREEYEAR_AMT_P',
        'PARTS_CHEMI_HALFYEAR_MIN',
        'PTS_ACCIDT_THREEYEAR_AMT_P',
        'D2__WARR_Y2_INV_CNT',
        'MINAMOUNT_OUT_TWOYEAR',
        'TOTALAMOUNT_OUT_YEAR',
        'ALL_AVG_M3Y',
        'PTS_IN_HALFYEAR_AMT_P',
        'INV_CUST_HALFYEAR_MIN',
        'INV_CUST_YEAR_MIN',
        'MILEAGE_AVG_Y3',
        'D1__FOURTH_Y1_INV_CNT',
        'PTS_OUT_AMT_UPY13',
        'PARTS_MAINT_MTH_AVG',
        'SRP001__CLASS_CNT_per1w',
        'D1__SALE_AMT_Y1',
        'PARTS_CHEMI_YEAR_MIN',
        'INV_CUST_AVG_YEAR13_PER',
        'SRP01_AMT_MTH_P',
        'SRP01_AMT_HALFYEAR_P',
        'INV_WARR_AMT_YEAR13',
        'PTS_IN_TWOYEAR_AMT_P',
        'D1__CUST_R_Y1_AMT',
        'INV_CUST_HALFYEAR_AMT',
        'MILE_MY3',
        'D1__INTERNAL_Y2_AMT',
        'PTS_MAINT_AVG_UPM3Y',
        'PARTS_ACCIDT_THREEYEAR_MIN',
        'PTS_OUT_YEAR_AMT_P',
        'ALL_AMT_HALFYEAR_P',
        'PARTS_IN_MIN_TWOYEAR',
        'D1__CUST_R_Y2_INV_CNT',
        'PTS_MAINT_HALFYEAR_AMT_P',
        'ALL_AMT_YEAR13',
        'PTS_CHEMI_YEAR_AMT_P',
        'PARTS_REPAIR_THREEYEAR_MIN',
        'INV_CUST_TWOYEAR_MIN',
        'PARTS_ELECTR_TWOYEAR_MIN',
        'PTS_MAINT_MTH_AMT_P',
        'MAXAMOUNT_OUT_HALFYEAR',
        'PTS_CHEMI_CNT_UPY13',
        'D2__LOCAL_BSI_Y1_AMT',
        'SRP01_AMT_TWOYEAR_P',
        'PTS_CHEMI_AVG_UPM3Y',
        'PTS_IN_THREEYEAR_AMT_P',
        'PTS_OUT_TWOYEAR_AMT_P',
        'D1__SALE_CNT_Y1',
        'TOTALAMOUNT_OUT_TWOYEAR',
        'INV_WARR_TWOYEAR_MIN',
        'MAXAMOUNT_OUT_YEAR',
        'INV_CUST_MTH_AMT',
        'PTS_OUT_THREEYEAR_AMT_P',
        'LABOR_AVG_M3Y',
        'PARTS_IN_MIN_THREEYEAR',
        'PARTS_MAINT_THREEYEAR_AVG',
        'INV_INSUR_YEAR_AMT_P',
        'PTS_CHEMI_MTH_AMT_P',
        'PARTS_CHEMI_YEAR_AVG',
        'D1__WARR_Y1_AMT',
        'D2__INSU_Y1_AMT',
        'PARTS_MAINT_YEAR_MIN',
        'INV_INTER_AMT_YEAR13',
        'D1__CUST_R_Y2_AMT',
        'INV_INSUR_THREEYEAR_MIN',
        'PTS_CHEMI_AMT_UPY13',
        'ALL_AMT_HALFYEAR',
        'PARTS_CHEMI_THREEYEAR_AVG',
        'MINAMOUNT_OUT_YEAR',
        'PTS_MAINT_THREEYEAR_AMT_P',
        'LABOROTHER_AVG_YEAR13',
        'INV_WARR_YEAR_AMT_P',
        'PARTS_MAINT_HALFYEAR_MIN',
        'D2__SUB_DEALER_Y2_AMT',
        'INV_INTER_AVG_M3Y',
        'LABOROTHER_MIN_YEAR',
        'SRP10_AMT_TWOYEAR_P',
        'PARTS_CHEMI_TWOYEAR_AVG',
        'ALL_AVG_YEAR13',
        'MILE_MY2_PER',
        'LABOR_AMT_THREEYEAR_P',
        'MILEAGE_AVG_Y1',
        'PTS_MAINT_YEAR_AMT_P',
        'PARTS_CHEMI_HALFYEAR_AVG',
        'PTS_CHEMI_HALFYEAR_AMT_P',
        'TOTALAMOUNT_OUT_THREEYEAR',
        'LABOROTHER_AMT_THREEYEAR_P',
        'D2__LOCAL_BSI_Y2_AMT',
        'LABOR_AMT_MTH_P',
        'ALL_AMT_YEAR_P',
        'INV_CUST_HALFYEAR_AVG',
        'INV_CUST_THREEYEAR_MIN',
        'INV_CUST_YEAR_AMT',
        'PARTS_MAINT_HALFYEAR_AMT',
        'PTS_IN_YEAR_AMT_P',
        'ALL_AMT_MTH_P',
        'SRP01_AMT_YEAR_P',
        'MINAMOUNT_OUT_THREEYEAR',
        'INV_INSUR_TWOYEAR_MIN',
        'MAXAMOUNT_OUT_THREEYEAR',
        'PARTS_CHEMI_TWOYEAR_MIN',
        'INV_CUST_THREEYEAR_AMT_P',
        'D1__BSI_Y2_INV_CNT',
        'PTS_MAINT_TWOYEAR_AMT_P',
        'D2__WARR_Y2_AMT',
        'PARTS_MAINT_YEAR_AVG',
        'PTS_IN_AVG_UPY13',
        'MILE_Y12',
        'INV_CUST_MTH_MIN',
        'LABOR_AMT_HALFYEAR_P',
        'INV_WARR_THREEYEAR_AMT_P',
        'D1__WARR_Y1_INV_CNT',
        'INV_WARR_YEAR_MIN',
        'PTS_IN_AVG_UPM3Y',
        'D1__SEV_CNT',
        'D1__SEV_AMT',
        'INV_WARR_THREEYEAR_MIN',
        'PARTS_MAINT_TWOYEAR_MIN',
        'PARTS_MAINT_TWOYEAR_AVG',
        'PARTS_CHEMI_MTH_AVG',
        'PTS_CHEMI_THREEYEAR_AMT_P',
        'D1__CUST_R_Y1_INV_CNT',
        'ALL_AMT_THREEYEAR_P',
        'INV_WARR_AVG_M3Y',
        'D2__INTERNAL_Y1_INV_CNT',
        'LABOR_CNT_YEAR13_PER',
        'ALL_AMT_TWOYEAR_P',
        'LABOROTHER_AMT_TWOYEAR_P',
        'PARTS_CHEMI_YEAR_AMT',
        'MAXAMOUNT_OUT_TWOYEAR',
        'INV_WARR_TWOYEAR_AMT_P',
        'D2__COUTER_Y1_AMT',
        'PARTS_CHEMI_THREEYEAR_MIN',
        'D2__SUB_DEALER_Y1_AMT',
        'D2__LOCAL_BSI_Y1_INV_CNT',
        'PARTS_MAINT_HALFYEAR_AVG',
        'MILE_MY3_PER',
        'PARTS_IN_CNT_MTH',
        'PARTS_CHEMI_THREEYEAR_AMT',
        'MILE_MY1_PER',
        'PARTS_CHEMI_YEAR_CNT',
        'PTS_OUT_MTH_AMT_P',
        'INV_INSUR_HALFYEAR_AMT_P',
        'MILE_Y13',
        'INV_CUST_THREEYEAR_AMT',
        'SRP10_AMT_THREEYEAR_P',
        'D2__CUST_R_Y2_INV_CNT',
        'PARTS_MAINT_THREEYEAR_MIN',
        'PTS_IN_AVG_UPM3Y_P',
        'PTS_CHEMI_TWOYEAR_AMT_P',
        'INV_INSUR_THREEYEAR_AMT_P',
        'INV_INTER_HALFYEAR_AMT',
        'PARTS_CHEMI_TWOYEAR_AMT',
        'D2__COUTER_Y2_INV_CNT',
        'INV_CUST_YEAR_AMT_P',
        'INV_INSUR_THREEYEAR_MAX',
        'PTS_REPAIR_AVG_UPY13',
        'D2__COUTER_Y2_AMT',
        'MILE_Y12_PER',
        'INV_INTER_YEAR_AMT',
        'LABOR_AMT_TWOYEAR_P',
        'MILE_Y13_PER',
        'D2__SALE_CNT_Y2',
        'INV_INSUR_THREEYEAR_AVG',
        'D1__BSI_Y1_INV_CNT',
        'INV_INTER_THREEYEAR_AMT',
        'MILEAGE_MAX',
        'SRP10_AMT_YEAR_P',
        'PARTS_CHEMI_TWOYEAR_CNT',
        'INV_INTER_THREEYEAR_AVG',
        'INV_INSUR_TWOYEAR_AMT_P',
        'INV_CUST_TWOYEAR_AMT_P',
        'INV_WARR_THREEYEAR_AVG',
        'PARTS_CHEMI_THREEYEAR_CNT',
        'INV_INTER_TWOYEAR_MAX',
        'INV_WARR_THREEYEAR_MAX',
        'D2__SALE_CNT_Y1',
        'INV_INTER_TWOYEAR_AMT',
        'INV_INSUR_TWOYEAR_AVG',
        'INV_INSUR_TWOYEAR_AMT',
        'INV_WARR_THREEYEAR_AMT',
        'INV_INTER_YEAR_MAX',
        'PARTS_CHEMI_HALFYEAR_CNT',
        'PTS_MAINT_AVG_UPM3Y_P',
        'INV_WARR_TWOYEAR_AMT',
        'SRP01_AVG_THREEYEAR',
        'SRP010__CLASS_CNT_per1w',
        'D2__SALE_AMT_Y2',
        'SRP003__CLASS_CNT_per1w',
        'INV_WARR_TWOYEAR_AVG',
        'SRP10_AMT_MTH_P',
        'INV_WARR_TWOYEAR_MAX',
        'SRP002__CLASS_CNT_per1w',
        'LABOROTHER_CNT_THREEYEAR',
        'LABOR_CNT_TWOYEAR',
        'SRP011__CLASS_CNT_per1w',
        'LABOR_CNT_THREEYEAR',
        'SRP01_YEAR_CNT13_PER',
        'SRP01_THREEYEAR_MIN',
        'SRP008__CLASS_CNT_per1w',
        'SRP001__CLASS_CNT',
        'SRP006__CLASS_CNT_per1w',
        'SRP004__CLASS_CNT_per1w',
        'SRP01_TWOYEAR_CNT',
        'SRP005__CLASS_CNT_PERM',
        'SRP003__CLASS_CNT',
        'SRP012__CLASS_CNT_per1w',
        'DEALER_CNT'
    ]
}


def preprocessing(df):
    ID = col_dict['ID']
    df.index = df[ID]
    # 衍生变量
    df['AGE_MTH'] = df['AGE_MTH'].map(lambda x: 0 if x < 0 else x)
    df['AGE_MTH'] = df['AGE_MTH'].map(lambda x: 120 if x > 120 else x)
    df['MAX_MILEAGE_PERM'] = list(map(lambda x, y: Divide(x, y), df['AGE_MTH'], df['MILEAGE_MAX']))
    df['SRP005__CLASS_CNT_PERM'] = list(map(lambda x, y: Divide(x, y), df['AGE_MTH'] / 24, df['SRP005__CLASS_CNT']))
    for i in col_dict['base_col']:
        df[i + str('_per1w')] = list(map(lambda x, y: Divide(x, y), df['MILEAGE_INT'], df[i]))
    return df


def predict_xgb(df, Model_path):
    import xgboost as xgb
    model_path = Model_path
    model = xgb.Booster(model_file=model_path)
    Preditors = col_dict['Preditors']
    X = df[Preditors]
    Ddata = xgb.DMatrix(X)
    pred = model.predict(Ddata)
    print('calculating....................')
    result = pd.DataFrame({'流失概率': pred}, index=X.index).sort_values('流失概率', ascending=False)
    result['chassisno'] = result.index.map(lambda x: x[0])
    result['流失概率区间'] = pd.cut(result['流失概率'], [0, 0.17, 0.34, 0.51, 0.68, 0.85, 1.0])
    level_dic = {'(0.85, 1.0]': '******',
                 '(0.68, 0.85]': '*****',
                 '(0.51, 0.68]': '****',
                 '(0.34, 0.51]': '***',
                 '(0.17, 0.34]': '**',
                 '(0.0, 0.17]': '*'
                 }
    result['流失等级'] = result['流失概率区间'].map(lambda x: str(x)).map(level_dic)
    return result


if __name__ == '__main__':
    main()