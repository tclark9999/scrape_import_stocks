import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st

def peg_color_format(value):
    if not pd.isna(value):
        value = float(value)
        if (value > 0.0) and (value < 1.0):
            return 'background-color: lightblue'
        elif (value <= 1.5):
            return 'background-color: lavender'
        else:
            return 'background-color: lightsalmon'
    else:
        return ''
    
def profit_margin_color_format(value):
    if not pd.isna(value):
        value = value.replace("%","")
        value = float(value)
        if (value > 15.0):
            return 'background-color: lightblue'
        elif (value>10.0):
            return 'background-color: lavender'
        else:
            return 'background-color: lightsalmon'
    else:
        return ''
    
def curr_ratio_color_format(value):
    if not pd.isna(value):
        value = float(value)
        if (value > 1.2) and (value <= 2.0):
            return 'background-color: lightblue'
        elif (value>2.0):
            return 'background-color: lavender'
        elif (value>=1.0):
            return 'background-color: lavender'
        else:
            return 'background-color: lightsalmon'
    else:
        return ''


def create_trend_cols(df: pd.DataFrame,col_prefix_list):
    for prefix in col_prefix_list:
        cols_list = [prefix + " 1", prefix + " 2", prefix + " 3", prefix + " 4"]
        df[prefix] = df.apply(lambda row:[row[cols_list[0]],row[cols_list[1]],
                                                    row[cols_list[2]],row[cols_list[3]]], axis=1)
        
        df.drop([col for col in cols_list],axis=1,inplace=True)

    return df
        

def create_relative_metric_field(metric_name,metric_file,date_rank)->str:
    if metric_file.startswith("Quarterly"):
        metric_type = ' qtr '
    elif metric_file.startswith("Yearly"):
        metric_type = ' yr ' 
    else:
        metric_type = ' qtr '
    
    relative_metric_field = metric_name + metric_type + str(int(date_rank))
    return relative_metric_field

def get_summary_data_frame(current_stats_df: pd.DataFrame, date_metrics_df: pd.DataFrame)->pd.DataFrame:
    current_stats_list = ['Current Ratio (mrq)','Profit Margin','Book Value Per Share (mrq)','5 Year Average Dividend Yield',
                          'Forward Annual Dividend Yield 4','Payout Ratio 4','Operating Margin','Return on Equity (ttm)']
    date_stats_list = ['Cost Of Revenue', 'Total Revenue','Forward P/E','PEG Ratio (5yr expected)','Price/Book','Trailing P/E', 'Net Income']

    curr_filter = current_stats_df['metric_name'].isin(current_stats_list)
    date_filter = (date_metrics_df['metric'].isin(date_stats_list)) & (date_metrics_df['dates_dense_rank'] < 5)

    current_stats_fltr_df = current_stats_df[curr_filter].reset_index(drop=True).drop_duplicates()
    date_metrics_fltr_df = date_metrics_df[date_filter].reset_index(drop=True)

    date_metrics_fltr_df['relative_metric'] = date_metrics_fltr_df.apply(lambda x: create_relative_metric_field(x['metric'], x['file'], x['dates_dense_rank']), axis=1)

    date_metrics_fltr_df['most_recent_date_qtr'] = date_metrics_fltr_df[date_metrics_fltr_df["file"].str.contains("Quarterly")].groupby(['metric','stock'])['date'].transform('max')
    date_metrics_fltr_df['most_recent_date_yr'] = date_metrics_fltr_df[date_metrics_fltr_df["file"].str.contains("Yearly")].groupby(['metric','stock'])['date'].transform('max')

    date_metrics_fltr_df['most_recent_date_qtr'] = date_metrics_fltr_df.fillna('1900-01-01').groupby(['stock'])['most_recent_date_qtr'].transform('max')
    date_metrics_fltr_df['most_recent_date_yr'] = date_metrics_fltr_df.fillna('1900-01-01').groupby(['stock'])['most_recent_date_yr'].transform('max')

    date_metrics_pivot_df = date_metrics_fltr_df.pivot(index=['stock','most_recent_date_qtr','most_recent_date_yr'],columns=['relative_metric'],values=['metric_value'])
    curr_metrics_pivot_df = current_stats_fltr_df.pivot(index=['stock'],columns=['metric_name'],values=['metric_value'])

    date_metrics_pivot_df.columns = date_metrics_pivot_df.columns.droplevel()
    curr_metrics_pivot_df.columns = curr_metrics_pivot_df.columns.droplevel()
    curr_metrics_pivot_df.reset_index(inplace=True)
    date_metrics_pivot_df.reset_index(inplace=True)
    
    combined_df = curr_metrics_pivot_df.merge(date_metrics_pivot_df,on='stock')

    column_prefix_list = ["Cost Of Revenue yr", "Cost Of Revenue qtr", "Total Revenue qtr", "Total Revenue yr",
                          "Net Income yr", "Net Income qtr"]

    combined_df = create_trend_cols(combined_df, col_prefix_list=column_prefix_list)

    return combined_df


if __name__ == "__main__":

    curr_df = pd.read_csv("TRANSFORMED/Current_Stats_Combined.csv")
    date_df = pd.read_csv("TRANSFORMED/Date_Metrics_Combined.csv")

    metrics_df = get_summary_data_frame(curr_df,date_df)

    cols_to_keep = ["stock","PEG Ratio (5yr expected) qtr 1", "Price/Book qtr 1", "Trailing P/E qtr 1",
                    "Forward P/E qtr 1","Book Value Per Share (mrq)","Current Ratio (mrq)",
                    "Forward Annual Dividend Yield 4", "Payout Ratio 4", "Profit Margin",	
                    "Return on Equity (ttm)", "most_recent_date_qtr", "most_recent_date_yr",
                    "Cost Of Revenue yr", "Cost Of Revenue qtr", "Total Revenue qtr", "Total Revenue yr",
                          "Net Income yr", "Net Income qtr"]
    
    metrics_df = metrics_df[cols_to_keep]

    metrics_df.replace("--", pd.NA, inplace=True)

    st.dataframe(metrics_df.style.applymap(peg_color_format,subset=["PEG Ratio (5yr expected) qtr 1"])\
        .applymap(curr_ratio_color_format,subset=["Current Ratio (mrq)"])\
        .applymap(profit_margin_color_format,subset=["Profit Margin"])\
        .background_gradient(cmap="coolwarm",high=.75,low=.75,subset=["Trailing P/E qtr 1",
                    "Forward P/E qtr 1"]),use_container_width=True)
