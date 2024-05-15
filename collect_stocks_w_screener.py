
from collect_stocks import StockDataCollection
from collect_stocks import get_summary_data_frame, peg_color_format, curr_ratio_color_format, profit_margin_color_format
import streamlit as st
import pandas as pd

USE_CACHE = 0

screenerCollection = StockDataCollection.from_yahoo_screener("YAHOO_SCREENER","https://finance.yahoo.com/screener/predefined/undervalued_growth_stocks",n=50)

if USE_CACHE == 0:
    screenerCollection.scrape_stock_stats_data()
    screenerCollection.scrape_financials_data()
    screenerCollection.save_data_to_files()
else:
    screenerCollection.load_data_from_files()

screenerCollection.merge_high_level_stats()
screenerCollection.melt_merge_date_pivots()

metrics_df = get_summary_data_frame(screenerCollection.all_stats_df
                                    ,screenerCollection.date_metrics_df)

cols_to_keep = ["stock","PEG Ratio (5yr expected) qtr 1", "Price/Book qtr 1", "Trailing P/E qtr 1",
                "Forward P/E qtr 1","Book Value Per Share (mrq)","Current Ratio (mrq)",
                "Forward Annual Dividend Yield 4", "Payout Ratio 4", "Profit Margin",	
                "Return on Equity (ttm)", "most_recent_date_qtr", "most_recent_date_yr",
                "Total Revenue yr", "Total Revenue qtr", "Cost Of Revenue yr", "Cost Of Revenue qtr", 
                        "Net Income yr", "Net Income qtr"]

metrics_df = metrics_df[cols_to_keep]

metrics_df.replace("--", pd.NA, inplace=True)

st.dataframe(metrics_df.style.applymap(peg_color_format,subset=["PEG Ratio (5yr expected) qtr 1"])\
    .applymap(curr_ratio_color_format,subset=["Current Ratio (mrq)"])\
    .applymap(profit_margin_color_format,subset=["Profit Margin"])\
    .background_gradient(cmap="coolwarm",high=.75,low=.75,subset=["Trailing P/E qtr 1",
                "Forward P/E qtr 1"]),use_container_width=True,
                column_config={
                    "Cost Of Revenue yr":
                        st.column_config.LineChartColumn(
                            "Cost of Revenue (Last 4 Years)",
                            width="medium"
                        ),
                    "Cost Of Revenue qtr":
                        st.column_config.LineChartColumn(
                            "Cost of Revenue (Last 4 Qtrs)",
                            width="medium"
                        ),
                    "Total Revenue yr":
                        st.column_config.LineChartColumn(
                            "Total Revenue (Last 4 Years)",
                            width="medium"
                        ),
                    "Total Revenue qtr":
                        st.column_config.LineChartColumn(
                            "Total Revenue (Last 4 Qtrs)",
                            width="medium"
                        ),
                    "Net Income yr":
                        st.column_config.LineChartColumn(
                            "Net Income (Last 4 Years)",
                            width="medium"
                        ),
                    "Net Income qtr":
                        st.column_config.LineChartColumn(
                            "Net Income (Last 4 Qtrs)",
                            width="medium"
                        )
                })