import pyodbc
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
import statistics
warnings.filterwarnings("ignore")


def avg_mnths(*mnths):
    """Function gets the average of a list"""
    x = statistics.mean(mnths)
    return x


def stdev_of_mnths(*mnths):
    """Function gets the standard deviation of a list"""
    x = statistics.stdev(mnths)
    return x


def transform(data_extract):
    """Function does preprocessing on churn predict dataframe"""
    print('Transforming...')
    df = data_extract
    df_copy = df.copy()
    #dropping duplicates
    df.drop_duplicates(inplace=True)
    df.drop_duplicates(subset=['customer_id'], keep='last', inplace=True)
    #dropping the missing values
    df.dropna(inplace=True)
    #dropping abnornal age
    abnormal_age = df[(df['age'] > 127) | (df['age'] < 0)].index
    df.drop(abnormal_age, inplace=True)  
    #change data type
    df['age'] = df['age'].astype(int)
    #fix issue in sex feature
    df.replace('m', 'M', inplace=True)

    #Get new columns containing frequency
    df['customer_segment_freq'] = df['customer_segment'].map(df['customer_segment'].value_counts())
    df['generation_freq'] = df['generation'].map(df['generation'].value_counts())
    df['occupation_freq'] = df['occupation'].map(df['occupation'].value_counts())
    df['region_name_freq'] = df['region_name'].map(df['region_name'].value_counts())
    #One hot encoding for categorical variables
    df = pd.concat([df,pd.get_dummies(df['customer_status'])],axis=1)
    df = pd.concat([df,pd.get_dummies(df['sex'])],axis=1) 
    #drop irrelevant columns
    df.drop(columns=['customer_segment', 'generation', 'occupation', 'customer_status', 'sex', 'O', 'region_name', 'rib'],
        inplace=True)
    #Get feature for average 5 months transaction count
    df['avg_5mnths_cnt'] = df.apply(lambda x: avg_mnths(x['mnth1'], x['mnth2'], x['mnth3'], x['mnth4'], x['mnth5']), axis=1)
    #Get feature for standard deviation for 5 months transaction count
    df['5mnths_cnt_std'] = df.apply(lambda x: stdev_of_mnths(x['mnth1'], x['mnth2'], x['mnth3'], x['mnth4'], x['mnth5']), axis=1)
    #drop customer_id column
    df.drop(columns=['mnth1', 'customer_id'], axis=1, inplace=True)
    #rename columns
    df.rename(columns={'mnth2':'mnth1', 'mnth3':'mnth2', 'mnth4':'mnth3', 'mnth5':'mnth4', 'mnth6':'mnth5'}, inplace=True)

    print("Transformation Done")

    return df, df_copy


    