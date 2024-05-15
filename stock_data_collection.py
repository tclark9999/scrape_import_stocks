import pandas as pd
from datetime import datetime
import yfinance as yf
import requests
from requests import Session
import os
# from requests_cache import CacheMixin, SQLiteCache
# from requests_ratelimiter import LimiterMixin, MemoryQueueBucket
# from pyrate_limiter import Duration, RequestRate, Limiter
# class CachedLimiterSession(CacheMixin, LimiterMixin, Session):
#     pass

# _SESSION = CachedLimiterSession(
#     limiter=Limiter(RequestRate(2, Duration.SECOND*5)),  # max 2 requests per 5 seconds
#     bucket_class=MemoryQueueBucket,
#     backend=SQLiteCache("yfinance.cache"),
# )

def get_stock_stats_data_raw(stock: str) -> list[pd.DataFrame]:
    """
    Fetches raw stock statistics data from Yahoo Finance for a given stock symbol.
    
    Args:
        stock (str): Stock symbol (e.g., 'AAPL', 'MSFT').
    
    Returns:
        pd.DataFrame: Raw data containing stock statistics.
    """
    stats_url_link = f"https://finance.yahoo.com/quote/{stock}/key-statistics?p={stock}"
    r = requests.get(stats_url_link,headers ={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
    read_html_pandas_data = pd.read_html(r.text)
    return read_html_pandas_data

def get_stock_financials_data_raw(stock_list: str):
    """
    Retrieves raw financial data for a given stock symbol using Yahoo Finance API.
    
    Args:
        stock (str): Stock symbol (e.g., 'AAPL', 'MSFT').
    
    Returns:
        list: List of financial data DataFrames (income statement, balance sheet, cash flow, etc.).
    """
    stock_string = " ".join(stock_list)
    tickers = yf.Tickers(stock_string)
    
    return tickers.tickers



class StockDataCollection:
    collection_name: str
    stocks: list
    valuation: pd.DataFrame = pd.DataFrame()
    stock_price_history: pd.DataFrame = pd.DataFrame()
    share_stats: pd.DataFrame = pd.DataFrame()
    div_split: pd.DataFrame = pd.DataFrame()
    profitability: pd.DataFrame = pd.DataFrame()
    mngmt_effect: pd.DataFrame = pd.DataFrame()
    income_stmnt: pd.DataFrame = pd.DataFrame()
    balance_sht: pd.DataFrame = pd.DataFrame()
    cash_flow: pd.DataFrame = pd.DataFrame()
    yr_income_stmnt: pd.DataFrame = pd.DataFrame()
    qtr_income_stmnt: pd.DataFrame = pd.DataFrame()
    yr_balance_sheet: pd.DataFrame = pd.DataFrame()
    qtr_balance_sheet: pd.DataFrame = pd.DataFrame()
    yr_cash_flow: pd.DataFrame = pd.DataFrame()
    qtr_cash_flow: pd.DataFrame = pd.DataFrame()
    all_stats_df: pd.DataFrame = pd.DataFrame()
    date_metrics_df: pd.DataFrame = pd.DataFrame()
    default_file_mapping: dict

    def __init__(self,stock_list, collectionName):
        self.stocks = stock_list
        self.collection_name = collectionName

        self.default_file_mapping = {
            "valuation": f"{collectionName}/RAW/Valuation_Stats.csv",
            "stock_price_history": f"{collectionName}/RAW/Stock_History_Stats.csv",
            "share_stats": f"{collectionName}/RAW/Share_Stats.csv",
            "div_split": f"{collectionName}/RAW/Div_Split_Stats.csv",
            "profitability": f"{collectionName}/RAW/Profitability_Stats.csv",
            "mngmt_effect": f"{collectionName}/RAW/Management_Effect_Stats.csv",
            "income_stmnt": f"{collectionName}/RAW/Income_Statement_Stats.csv",
            "balance_sht": f"{collectionName}/RAW/Balance_Sheet_Stats.csv",
            "cash_flow": f"{collectionName}/RAW/Cash_Flow_Stats.csv",
            "yr_income_stmnt": f"{collectionName}/RAW/Yearly_Income_Statement.csv",
            "qtr_income_stmnt": f"{collectionName}/RAW/Quarterly_Income_Statement.csv",
            "yr_balance_sheet": f"{collectionName}/RAW/Yearly_Balance_Sheet.csv",
            "qtr_balance_sheet": f"{collectionName}/RAW/Quarterly_Balance_Sheet.csv",
            "yr_cash_flow": f"{collectionName}/RAW/Yearly_Cash_Flow.csv",
            "qtr_cash_flow": f"{collectionName}/RAW/Quarterly_Cash_Flow.csv"
        }

    @classmethod
    def from_yahoo_screener(cls,collectionName,screener_url,n: int = None):
        if n:
            screener_url = screener_url + f"/?count={n}&offset=0"
        r = requests.get(screener_url,headers ={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
        screener_df = pd.read_html(r.text)
        screener_list = screener_df[0]["Symbol"].tolist()
        return cls(screener_list,collectionName)


    def load_data_from_files(self,file_mapping: dict = None):
        if not file_mapping:
            file_mapping = self.default_file_mapping
        if "valuation" in file_mapping:
            self.valuation = pd.read_csv(file_mapping["valuation"])
        if "stock_price_history" in file_mapping:
            self.stock_price_history = pd.read_csv(file_mapping["stock_price_history"])
        if "share_stats" in file_mapping:
            self.share_stats = pd.read_csv(file_mapping["share_stats"])
        if "div_split" in file_mapping:
            self.div_split = pd.read_csv(file_mapping["div_split"])
        if "profitability" in file_mapping:
            self.profitability = pd.read_csv(file_mapping["profitability"])
        if "mngmt_effect" in file_mapping:
            self.mngmt_effect = pd.read_csv(file_mapping["mngmt_effect"])
        if "income_stmnt" in file_mapping:
            self.income_stmnt = pd.read_csv(file_mapping["income_stmnt"])
        if "balance_sht" in file_mapping:
            self.balance_sht = pd.read_csv(file_mapping["balance_sht"])
        if "cash_flow" in file_mapping:
            self.cash_flow = pd.read_csv(file_mapping["cash_flow"])
        if "yr_income_stmnt" in file_mapping:
            self.yr_income_stmnt = pd.read_csv(file_mapping["yr_income_stmnt"])
        if "qtr_income_stmnt" in file_mapping:
            self.qtr_income_stmnt = pd.read_csv(file_mapping["qtr_income_stmnt"])
        if "yr_balance_sheet" in file_mapping:
            self.yr_balance_sheet = pd.read_csv(file_mapping["yr_balance_sheet"])
        if "qtr_balance_sheet" in file_mapping:
            self.qtr_balance_sheet = pd.read_csv(file_mapping["qtr_balance_sheet"])
        if "yr_cash_flow" in file_mapping:
            self.yr_cash_flow = pd.read_csv(file_mapping["yr_cash_flow"])
        if "qtr_cash_flow" in file_mapping:
            self.qtr_cash_flow = pd.read_csv(file_mapping["qtr_cash_flow"])


    def save_data_to_files(self, file_mapping: dict = None):
        if not file_mapping:
            file_mapping = self.default_file_mapping

        for key, value in file_mapping.items():
            directory = os.path.dirname(value)
            if not os.path.exists(directory):
                os.makedirs(directory)

            if key == "valuation":
                self.valuation.to_csv(value, index=False)
            elif key == "stock_price_history":
                self.stock_price_history.to_csv(value, index=False)
            elif key == "share_stats":
                self.share_stats.to_csv(value, index=False)
            elif key == "div_split":
                self.div_split.to_csv(value, index=False)
            elif key == "profitability":
                self.profitability.to_csv(value, index=False)
            elif key == "mngmt_effect":
                self.mngmt_effect.to_csv(value, index=False)
            elif key == "income_stmnt":
                self.income_stmnt.to_csv(value, index=False)
            elif key == "balance_sht":
                self.balance_sht.to_csv(value, index=False)
            elif key == "cash_flow":
                self.cash_flow.to_csv(value, index=False)
            elif key == "yr_income_stmnt":
                self.yr_income_stmnt.to_csv(value, index=False)
            elif key == "qtr_income_stmnt":
                self.qtr_income_stmnt.to_csv(value, index=False)
            elif key == "yr_balance_sheet":
                self.yr_balance_sheet.to_csv(value, index=False)
            elif key == "qtr_balance_sheet":
                self.qtr_balance_sheet.to_csv(value, index=False)
            elif key == "yr_cash_flow":
                self.yr_cash_flow.to_csv(value, index=False)
            elif key == "qtr_cash_flow":
                self.qtr_cash_flow.to_csv(value, index=False)

        return


    def scrape_stock_stats_data(self):
        for stock in self.stocks:
            # time.sleep(2)
            stats_data = get_stock_stats_data_raw(stock)
            
            
            valuation_df = stats_data[0]
            valuation_df["stock"] = stock
            valuation_df = valuation_df.rename(columns={"Unnamed: 0":"metric"})
            valuation_df = valuation_df.rename(columns={valuation_df.columns[2]:valuation_df.columns[2]\
                                                    .replace("As of Date:","")\
                                                        .replace("Current","")})
            self.valuation = pd.concat([valuation_df,self.valuation])

            stock_price_history_df = stats_data[8]
            stock_price_history_df["stock"] = stock
            self.stock_price_history = pd.concat([self.stock_price_history,stock_price_history_df])
            
            share_stats_df = stats_data[9]
            share_stats_df["stock"] = stock
            self.share_stats = pd.concat([self.share_stats,share_stats_df])
            
            div_split_df = stats_data[10]
            div_split_df["stock"] = stock
            self.div_split = pd.concat([self.div_split,div_split_df])
            
            profitability_df = stats_data[3]
            profitability_df["stock"] = stock
            self.profitability = pd.concat([self.profitability,profitability_df])
            
            mngmt_effect_df = stats_data[4]
            mngmt_effect_df["stock"] = stock
            self.mngmt_effect = pd.concat([self.mngmt_effect,mngmt_effect_df])
            
            income_stmnt_df = stats_data[5]
            income_stmnt_df["stock"] = stock
            self.income_stmnt = pd.concat([self.income_stmnt,income_stmnt_df])
            
            balance_sht_df = stats_data[6]
            balance_sht_df["stock"] = stock
            self.balance_sht = pd.concat([self.balance_sht,balance_sht_df])
            
            cash_flow_df = stats_data[7]
            cash_flow_df["stock"] = stock
            self.cash_flow = pd.concat([self.cash_flow,cash_flow_df])
        return
    
    def scrape_financials_data(self):
        tickers = get_stock_financials_data_raw(self.stocks)
        for stock, ticker in tickers.items():
            financial_data = get_stock_financials_data_raw(stock)

            yr_income_df = ticker.income_stmt
            yr_income_df['stock'] = stock	
            yr_income_df = yr_income_df.reset_index().rename(columns={"index":"metric"})	
            self.yr_income_stmnt = pd.concat([self.yr_income_stmnt,yr_income_df])

            qtr_income_df = ticker.quarterly_income_stmt
            qtr_income_df['stock'] = stock
            qtr_income_df = qtr_income_df.reset_index().rename(columns={"index":"metric"})	
            self.qtr_income_stmnt = pd.concat([self.qtr_income_stmnt,qtr_income_df])

            yr_balance_df = ticker.balance_sheet
            yr_balance_df['stock'] = stock
            yr_balance_df = yr_balance_df.reset_index().rename(columns={"index":"metric"})	
            self.yr_balance_sheet = pd.concat([self.yr_balance_sheet,yr_balance_df])

            qtr_balance_df = ticker.quarterly_balance_sheet
            qtr_balance_df['stock'] = stock
            qtr_balance_df = qtr_balance_df.reset_index().rename(columns={"index":"metric"})	
            self.qtr_balance_sheet = pd.concat([self.qtr_balance_sheet,qtr_balance_df])

            yr_cash_flow_df = ticker.cash_flow
            yr_cash_flow_df['stock'] = stock
            yr_cash_flow_df = yr_cash_flow_df.reset_index().rename(columns={"index":"metric"})	
            self.yr_cash_flow = pd.concat([self.yr_cash_flow,yr_cash_flow_df])

            qtr_cash_flow_df = ticker.quarterly_cash_flow
            qtr_cash_flow_df['stock'] = stock
            qtr_cash_flow_df = qtr_cash_flow_df.reset_index().rename(columns={"index":"metric"})	
            self.qtr_cash_flow = pd.concat([self.qtr_cash_flow,qtr_cash_flow_df])
        return

    def merge_high_level_stats(self,include_only_list: list = None):
        high_level_stats_df = pd.DataFrame()
        stats_dict = {"stock_price_history":self.stock_price_history,
                       "share_stats":self.share_stats,
                        "div_split":self.div_split,
                        "profitability":self.profitability,
                        "mngmt_effect":self.mngmt_effect,
                        "income_stmnt":self.income_stmnt,
                        "balance_sht":self.balance_sht,
                        "cash_flow":self.cash_flow}
        if include_only_list:
            final_stats_dict = {k:v for k,v in stats_dict.items() if k in include_only_list}
        else:
            final_stats_dict = stats_dict
        for key,stats_df in final_stats_dict.items():
            stats_df.columns = ["metric_name","metric_value","stock"]
            stats_df['file']= key
            high_level_stats_df = pd.concat([high_level_stats_df,stats_df])
        high_level_stats_df = high_level_stats_df.sort_values(by=['stock','file','metric_name'])
        self.all_stats_df = high_level_stats_df.reset_index(drop=True).drop_duplicates()
        return 
    
    def melt_merge_date_pivots(self, include_only_list: list = None):
        metrics_with_dates_df = pd.DataFrame()
        dates_dict = {"valuation":self.valuation,
                       "yr_income_stmnt": self.yr_income_stmnt,
                        "qtr_income_stmnt": self.qtr_income_stmnt,
                        "yr_balance_sheet": self.yr_balance_sheet,
                        "qtr_balance_sheet": self.qtr_balance_sheet,
                        "yr_cash_flow": self.yr_cash_flow,
                        "qtr_cash_flow": self.qtr_cash_flow}
        
        if include_only_list:
            final_dates_dict = {k:v for k,v in dates_dict.items() if k in include_only_list}
        else:
            final_dates_dict = dates_dict

        for key, dates_df in final_dates_dict.items():
            if key == 'valuation':
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
            dates_df['file'] = key
            metrics_with_dates_df = pd.concat([metrics_with_dates_df,dates_df])

        metrics_with_dates_df = metrics_with_dates_df.dropna()
        metrics_with_dates_df['date'] = pd.to_datetime(metrics_with_dates_df['date'])
        metrics_with_dates_df = metrics_with_dates_df.sort_values(by=['stock','file','metric','date'],ascending=[True,True,True,False])
        metrics_with_dates_df['dates_dense_rank'] = metrics_with_dates_df.groupby(['stock','file'])\
                                                                ['date'].rank('dense',ascending=False)

        self.date_metrics_df =  metrics_with_dates_df.reset_index(drop=True).drop_duplicates()     
        return 