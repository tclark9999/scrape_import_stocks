import pandas as pd
import requests
import html5lib
import yfinance as yf
from bs4 import BeautifulSoup
import time
import os


def get_stock_stats_data_raw(stock: str) -> pd.DataFrame:
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

def get_stock_financials_data_raw(stock: str) -> list:
    """
    Retrieves raw financial data for a given stock symbol using Yahoo Finance API.
    
    Args:
        stock (str): Stock symbol (e.g., 'AAPL', 'MSFT').
    
    Returns:
        list: List of financial data DataFrames (income statement, balance sheet, cash flow, etc.).
    """
    curr_stock = yf.Ticker(stock)
    
    financials_data = [curr_stock.income_stmt,curr_stock.quarterly_income_stmt, curr_stock.balance_sheet, curr_stock.quarterly_balance_sheet, curr_stock.cash_flow, curr_stock.quarterly_cash_flow]

    return financials_data

def produce_stock_data_set(stock_list: list) -> dict:
    """
    Generates a comprehensive stock dataset by combining various financial data.
    
    Args:
        stock_list (list): List of stock symbols (e.g., ['AAPL', 'MSFT']).
    Returns:
        dict: Dictionary with dataset name as the key and pandas dataframe as the item
    """
    all_financials_data = []
    valuation = pd.DataFrame()
    stock_price_history = pd.DataFrame()
    share_stats = pd.DataFrame()
    div_split = pd.DataFrame()
    profitability = pd.DataFrame()
    mngmt_effect = pd.DataFrame()
    income_stmnt = pd.DataFrame()
    balance_sht = pd.DataFrame()
    cash_flow = pd.DataFrame()
    yr_income_stmnt = pd.DataFrame()
    qtr_income_stmnt = pd.DataFrame()
    yr_balance_sheet = pd.DataFrame()
    qtr_balance_sheet = pd.DataFrame()
    yr_cash_flow = pd.DataFrame()
    qtr_cash_flow = pd.DataFrame()
    for stock in stock_list:
        time.sleep(2)
        stats_data = get_stock_stats_data_raw(stock)
        
        
        valuation_df = stats_data[0]
        valuation_df["stock"] = stock
        valuation = pd.concat([valuation_df,valuation])
        
        stock_price_history_df = stats_data[1]
        stock_price_history_df["stock"] = stock
        stock_price_history = pd.concat([stock_price_history,stock_price_history_df])
        
        share_stats_df = stats_data[2]
        share_stats_df["stock"] = stock
        share_stats = pd.concat([share_stats,share_stats_df])
        
        div_split_df = stats_data[3]
        div_split_df["stock"] = stock
        div_split = pd.concat([div_split,div_split_df])
        
        profitability_df = stats_data[5]
        profitability_df["stock"] = stock
        profitability = pd.concat([profitability,profitability_df])
        
        mngmt_effect_df = stats_data[6]
        mngmt_effect_df["stock"] = stock
        mngmt_effect = pd.concat([mngmt_effect,mngmt_effect_df])
        
        income_stmnt_df = stats_data[7]
        income_stmnt_df["stock"] = stock
        income_stmnt = pd.concat([income_stmnt,income_stmnt_df])
        
        balance_sht_df = stats_data[8]
        balance_sht_df["stock"] = stock
        balance_sht = pd.concat([balance_sht,balance_sht_df])
        
        cash_flow_df = stats_data[9]
        cash_flow_df["stock"] = stock
        cash_flow = pd.concat([cash_flow,cash_flow_df])
        

        financial_data = get_stock_financials_data_raw(stock)

        yr_income_df = financial_data[0]
        yr_income_df['stock'] = stock
        yr_income_stmnt = pd.concat([yr_income_stmnt,yr_income_df])

        qtr_income_df = financial_data[1]
        qtr_income_df['stock'] = stock
        qtr_income_stmnt = pd.concat([qtr_income_stmnt,qtr_income_df])

        yr_balance_df = financial_data[2]
        yr_balance_df['stock'] = stock
        yr_balance_sheet = pd.concat([yr_balance_sheet,yr_balance_df])

        qtr_balance_df = financial_data[3]
        qtr_balance_df['stock'] = stock
        qtr_balance_sheet = pd.concat([qtr_balance_sheet,qtr_balance_df])

        yr_cash_flow_df = financial_data[4]
        yr_cash_flow_df['stock'] = stock
        yr_cash_flow = pd.concat([yr_cash_flow,yr_cash_flow_df])

        qtr_cash_flow_df = financial_data[5]
        qtr_cash_flow['stock'] = stock
        qtr_cash_flow = pd.concat([qtr_cash_flow,yr_cash_flow_df])

    return {"Quarterly_Cash_Flow":qtr_cash_flow, "Yearly_Cash_Flow":yr_cash_flow, 
            "Quarterly_Balance_Sheet":qtr_balance_sheet, "Yearly_Balance_Sheet":yr_balance_sheet,
            "Quarterly_Income_Statement":qtr_income_stmnt, "Yearly_Income_Statement":yr_income_stmnt, 
            "Cash_FLow_Stats":cash_flow, "Balance_Sheet_Stats":balance_sht, 
            "Income_Statement_Stats":income_stmnt,"Management_Effect_Stats":mngmt_effect, 
            "Profitability_Stats":profitability, "Div_Split_Stats":div_split, 
            "Share_Stats":share_stats, "Stock_History_Stats":stock_price_history,
            "Valuation_Stats":valuation}

def save_stock_data(stock_dict_list,directory_name):
    """
    Saves stock data DataFrames to CSV files.

    Args:
        stock_dict_list (dict): A dictionary where keys are stock names and values are corresponding data DataFrames.

    Returns:
        None
    """
    for name, data_df in stock_dict_list.items():
        dirname = os.path.dirname(__file__)
        fullpath = os.path.join(dirname,directory_name)
        data_df.to_csv(f"{fullpath}/{name}.csv",index=False)

if __name__ == "__main__":

    stock_dict_list = produce_stock_data_set(["PYPL","AAPL"])

    save_stock_data(stock_dict_list=stock_dict_list,directory_name="RAW")