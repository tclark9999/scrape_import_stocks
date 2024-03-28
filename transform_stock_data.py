import pandas as pd
from scrape_import_stocks import save_stock_data

def merge_high_level_stats(csv_list):
    high_level_stats_df = pd.DataFrame()
    for csv_file in csv_list:
        stats_df = pd.read_csv(f"./RAW/{csv_file}")
        stats_df.columns = ["metric_name","metric_value","stock"]
        high_level_stats_df = pd.concat([high_level_stats_df,stats_df])

    print(high_level_stats_df.head())
    return high_level_stats_df

# def melt_merge_date_pivots(csv_list):






csv_file_list = ["Balance_Sheet_Stats.csv","Cash_Flow_Stats.csv","Div_Split_Stats.csv","Income_Statement_Stats.csv"
                 ,"Management_Effect_Stats.csv","Profitability_Stats.csv","Share_Stats.csv","Stock_History_Stats.csv"]

final_stats_df = merge_high_level_stats(csv_file_list)

save_stock_data({"Current_Stats_Combined":final_stats_df},"TRANSFORMED")