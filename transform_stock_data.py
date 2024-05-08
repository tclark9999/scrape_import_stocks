import pandas as pd
from scrape_import_stocks import save_stock_data
from datetime import datetime

def merge_high_level_stats(csv_list):
    high_level_stats_df = pd.DataFrame()
    for csv_file in csv_list:
        stats_df = pd.read_csv(f"./RAW/{csv_file}")
        stats_df.columns = ["metric_name","metric_value","stock"]
        stats_df['file']= csv_file
        high_level_stats_df = pd.concat([high_level_stats_df,stats_df])
    high_level_stats_df = high_level_stats_df.sort_values(by=['stock','file','metric_name'])
    return high_level_stats_df

def melt_merge_date_pivots(csv_list):
    metrics_with_dates_df = pd.DataFrame()
    for csv_file in csv_list:
        dates_df = pd.read_csv(f"./RAW/{csv_file}")
        if csv_file == 'Valuation_Stats.csv':
            # Loop over column names
            for col_name in dates_df.columns:
                try:
                    # Attempt to convert the column name to datetime
                    date_obj = datetime.strptime(col_name, "%m/%d/%Y")
                    # If successful, convert the column name format
                    new_col_name = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                    # Rename the column
                    dates_df.rename(columns={col_name: new_col_name}, inplace=True)
                except ValueError:
                    # If conversion fails, continue to the next column
                    continue
            dates_df = dates_df.rename(columns={"Current":datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
        dates_df = pd.melt(dates_df,id_vars=["metric","stock"],var_name="date",value_name="metric_value")
        dates_df['file'] = csv_file
        metrics_with_dates_df = pd.concat([metrics_with_dates_df,dates_df])

    metrics_with_dates_df = metrics_with_dates_df.dropna()
    metrics_with_dates_df['date'] = pd.to_datetime(metrics_with_dates_df['date'])
    metrics_with_dates_df = metrics_with_dates_df.sort_values(by=['stock','file','metric','date'],ascending=[True,True,True,False])
    metrics_with_dates_df['dates_dense_rank'] = metrics_with_dates_df.groupby(['stock','file'])\
                                                            ['date'].rank('dense',ascending=False)

    return metrics_with_dates_df.drop_duplicates()
        




stats_csv_file_list = ["Balance_Sheet_Stats.csv","Cash_Flow_Stats.csv","Div_Split_Stats.csv","Income_Statement_Stats.csv"
                 ,"Management_Effect_Stats.csv","Profitability_Stats.csv","Share_Stats.csv"]

dates_csv_file_list = ["Quarterly_Balance_Sheet.csv","Quarterly_Cash_Flow.csv","Quarterly_Income_Statement.csv",
                       "Yearly_Balance_Sheet.csv","Yearly_Cash_Flow.csv","Yearly_Income_Statement.csv","Valuation_Stats.csv"]


final_date_df = melt_merge_date_pivots(dates_csv_file_list)

save_stock_data({"Date_Metrics_Combined":final_date_df},"TRANSFORMED")

final_stats_df = merge_high_level_stats(stats_csv_file_list)

save_stock_data({"Current_Stats_Combined":final_stats_df},"TRANSFORMED")