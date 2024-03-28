import pandas as pd
from scrape_import_stocks import save_stock_data

def merge_high_level_stats(csv_list):
    high_level_stats_df = pd.DataFrame()
    for csv_file in csv_list:
        stats_df = pd.read_csv(f"./RAW/{csv_file}")
        stats_df.columns = ["metric_name","metric_value","stock"]
        high_level_stats_df = pd.concat([high_level_stats_df,stats_df])

    return high_level_stats_df

def melt_merge_date_pivots(csv_list):
    metrics_with_dates_df = pd.DataFrame()
    for csv_file in csv_list:
        dates_df = pd.read_csv(f"./RAW/{csv_file}")
        dates_df = pd.melt(dates_df,id_vars=["metric","stock"],var_name="date",value_name="metric_value")
        metrics_with_dates_df = pd.concat([metrics_with_dates_df,dates_df])

    return metrics_with_dates_df
        




stats_csv_file_list = ["Balance_Sheet_Stats.csv","Cash_Flow_Stats.csv","Div_Split_Stats.csv","Income_Statement_Stats.csv"
                 ,"Management_Effect_Stats.csv","Profitability_Stats.csv","Share_Stats.csv","Stock_History_Stats.csv"]

dates_csv_file_list = ["Quarterly_Balance_Sheet.csv","Quarterly_Cash_Flow.csv","Quarterly_Income_Statement.csv",
                       "Yearly_Balance_Sheet.csv","Yearly_Cash_Flow.csv","Yearly_Income_Statement.csv"]

final_date_df = melt_merge_date_pivots(dates_csv_file_list)

save_stock_data({"Date_Metrics_Combined":final_date_df},"TRANSFORMED")

final_stats_df = merge_high_level_stats(stats_csv_file_list)

save_stock_data({"Current_Stats_Combined":final_stats_df},"TRANSFORMED")