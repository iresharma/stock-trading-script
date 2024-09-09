import os
from datetime import datetime
from json import loads, dumps
from typing import List, Dict

import bsedata.exceptions
import requests
from apscheduler.triggers.cron import CronTrigger

from bsedata.bse import BSE
import time
from apscheduler.schedulers.background import BackgroundScheduler

import json

import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted
import csv

news_url = "https://saurav.tech/NewsAPI/top-headlines/category/business/in.json"
b = BSE()


def flatten_and_lowercase(data):
    flattened_data = []

    for element in data:
        name = element["name"].lower()
        symbol = element["symbol"].lower()
        flattened_data.extend([name, symbol, symbol + ".bs", symbol + ".ns"])

    return flattened_data


def custom_gemini_sentiment(news):
    genai.configure(api_key=os.environ.get("GOOGLE"))
    generation_config = {
        "temperature": 1,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 8192,
        "response_mime_type": "application/json",
    }

    model = genai.GenerativeModel(
        model_name="gemini-1.5-pro",
        generation_config=generation_config,
        system_instruction='You are an analyst at goldman sacs and you have about 20years of experience in analysing '
                           'news articles to estimate how it is going to effect the indian stock market. given a news '
                           'source, title and description you can tell the sentiment between -1 to 1 where -1 means '
                           'strongly buy and -1 means strongly sell. You also mention which top 100 stocks will the '
                           'news article effect make only the symbol of the stock is mentioned. Respond in the '
                           'following json schema {"stock": [""],"sentiment": 0.4}. DO NOT PROVIDE ANY EXPLAINATION '
                           'OR REASIONING.'
    )
    chat_session = model.start_chat()
    prompt = {
        "source": news["source"]["name"],
        "title": news["title"],
        "description": news["description"],
    }
    while True:
        try:
            response = chat_session.send_message(f"analyse {prompt}")
            resp = json.loads(response.text)
            print(json.dumps(resp, indent=4))
            return resp
        except ResourceExhausted:
            print("waiting for response, 429 was received")
            time.sleep(20)
            continue


def get_india_news(flattened_stocks):
    req = requests.get(news_url)
    resp = req.json()
    print(f"Total news articles: {resp['totalResults']}")
    analyse_collection = []
    for article in resp["articles"][:10]:
        print(article)
        if article["description"] is None:
            continue
        analyse_collection.append(custom_gemini_sentiment(article))
    stock_sentiment = {}
    stock_count = {}
    print(len(analyse_collection))

    for record in analyse_collection:
        stock_names = record['stock']
        sentiment = record['sentiment']
        for stock in stock_names:
            if not any(stock.lower() == stock_element for stock_element in flattened_stocks):
                continue
            if stock in stock_sentiment:
                stock_sentiment[stock] += sentiment
                stock_count[stock] += 1
            else:
                stock_sentiment[stock] = sentiment
                stock_count[stock] = 1

    # Calculate the average sentiment for each stock
    for stock in stock_sentiment:
        stock_sentiment[stock] /= stock_count[stock]

    print(json.dumps(stock_sentiment, indent=4))

    return stock_sentiment


def find_stock_in_scrips(stock_data: dict, scrip_data_redefined: dict):
    for stock, scrip in scrip_data_redefined.items():
        print(stock_data, stock, stock_data["name"].lower() in stock.lower())
        if stock_data["name"].lower() in stock.lower():
            try:
                resp = b.getQuote(scrip)
                if "securityID" not in resp:
                    time.sleep(10)
                    resp = b.getQuote(scrip)
                if resp["securityID"] == stock_data["symbol"]:
                    return scrip
            except bsedata.exceptions.InvalidStockException as e:
                print("Inactive stock")
                continue


def update_stock_scrip_codes():
    b.updateScripCodes()
    with open("stk.json", "r") as file:
        scrip_data = loads(file.read())
        scrip_data_redefined = {v.lower(): k for k, v in scrip_data.items()}
        for index in range(len(stocks)):
            stock_data = stocks[index]
            stock_data['scrip'] = find_stock_in_scrips(stock_data, scrip_data_redefined)
            stocks[index] = stock_data


def create_market_aux_url(stock: str, secondary=False) -> str:
    marketaux_news_url = (f"https://api.marketaux.com/v1/news/all?symbols=SYMBOL.BO&filter_entities=true"
                          f"&min_match_score=100&countries=in&api_token=")
    marketaux_news_url = marketaux_news_url.replace("SYMBOL", stock)
    if secondary:
        marketaux_news_url += os.environ.get('MARKET_AUX2')
    else:
        marketaux_news_url += os.environ.get('MARKET_AUX')
    return marketaux_news_url


def get_analysed_news(symbol: str, curr_sentiment: float, secondary: bool = False):
    url = create_market_aux_url(symbol, secondary)
    req = requests.get(url)
    resp = req.json()
    if 'error' in resp:
        print("API limit reached switching to secondary API_KEY")
        return None
    data = resp["data"]
    highlights = []
    sentiment_avg_marketaux = 0
    sentiment_count = 0
    for news_item in data:
        highlight_str = ""
        for entity in news_item["entities"]:
            for highlight in entity["highlights"]:
                highlight_str += highlight["highlight"]
                if highlight["sentiment"] is not None and highlight["sentiment"] != 0:
                    sentiment_avg_marketaux += highlight["sentiment"]
                    sentiment_count += 1
        highlights.append(highlight_str)
    if sentiment_count != 0:
        sentiment_avg_marketaux = sentiment_avg_marketaux / sentiment_count
    weighted_sentiment_avg = (sentiment_avg_marketaux * 0.6) + (curr_sentiment * 0.4)
    return weighted_sentiment_avg


def in_portfolio(stock: str, portfolio: List[Dict[str, any]]) -> List[List[any]]:
    mentions = []
    for index, value in enumerate(portfolio):
        if value["symbol"] == stock:
            mentions.append([index, value])
    return mentions


def calculate_buy_ratio(buy_stocks, balance):
    sentiment = 0
    for ele in buy_stocks:
        sentiment += ele['sentiment']
    print("unitary", 1 / sentiment)
    return balance / sentiment if sentiment > 0 else 0


def handle_orders(final_sentiments: dict, incoming_stocks):
    sell_stocks = []
    buy_stocks = []
    transactions = []
    for stock, sentiment in final_sentiments.items():
        if sentiment < 0:
            sell_stocks.append({'stock': stock.split(".")[0], 'sentiment': sentiment})
        else:
            buy_stocks.append({'stock': stock.split(".")[0], 'sentiment': sentiment})
    with open("final_stocks.json", "r") as final_stocks_file:
        portfolio = json.loads(final_stocks_file.read())

    print("===================Sell Sentiments===================")
    for log in sell_stocks:
        print(log["stock"], log["sentiment"])
    print("=====================================================")

    print("===================Buy Sentiments===================")
    for log in buy_stocks:
        print(log["stock"], log["sentiment"])
    print("====================================================")

    balance = portfolio['balance']
    portfolio_stocks = portfolio["stocks"]
    pl = portfolio['p/l']
    for sell_stock in sell_stocks:
        print(f"Selling {sell_stock}")
        in_portfolio_responses = in_portfolio(sell_stock['stock'], portfolio_stocks)
        if len(in_portfolio_responses) == 0:
            print(f"Not in portfolio, not selling")
            continue
        for in_portfolio_response in in_portfolio_responses:
            index, portfolio_info = in_portfolio_response
            qty = portfolio_info['qty']
            buy_price = portfolio_info['price']
            scrip = portfolio_info['scrip']
            latest_stock_info = b.getQuote(scrip)
            current_price = latest_stock_info['currentValue']
            balance += current_price * qty
            pl += (buy_price - current_price) * qty
            transactions.append({'action': 'sell', 'stock': portfolio_info, 'current_price': current_price})
            portfolio_stocks.pop(index)
    portfolio["p/l"] = pl
    buy_stocks = sorted(buy_stocks, key=lambda x: x['sentiment'])
    ratio = calculate_buy_ratio(buy_stocks, balance)
    for index, buy_stock in enumerate(buy_stocks):
        print(f"Buying {buy_stock}")
        in_portfolio_response = in_portfolio(buy_stock['stock'], portfolio_stocks)
        if len(in_portfolio_response) > 0:
            portfolio_info = in_portfolio_response[0][1]
        else:
            filtered_response = list(filter(lambda x: x['symbol'] == buy_stock['stock'], incoming_stocks))
            print(f"Filtered response for {buy_stock}: {filtered_response}")
            if len(filtered_response) == 0:
                print(f"Issue in buying {buy_stock}")
                continue
            portfolio_info = filtered_response[0]
        buy_ratio = buy_stock['sentiment'] * ratio
        scrip = portfolio_info['scrip']
        latest_stock_info = b.getQuote(scrip)
        current_price = float(latest_stock_info['currentValue'])
        qty = buy_ratio // current_price
        if qty == 0:
            continue
        balance -= current_price * qty
        transactions.append({'action': 'buy', 'current_price': current_price, **portfolio_info})
        portfolio_stocks.append({**portfolio_info, 'price': current_price, 'qty': qty})
    portfolio["balance"] = balance
    with open("final_stocks.json", "w") as final_stocks_file:
        final_stocks_file.write(json.dumps(portfolio, indent=4))
    with open("transactions.csv", "r+") as transactions_log:
        keys = transactions[0].keys()
        writer = csv.DictWriter(transactions_log, fieldnames=keys)
        writer.writerows(transactions)


def get_news_for_all(incoming_stocks):
    flattened_stocks = flatten_and_lowercase(incoming_stocks)
    # if datetime.today().weekday() in [5, 6]:
    #     return
    india_news_analyse_response = get_india_news(flattened_stocks)
    final_sentiment_analysis = {}
    for stock in india_news_analyse_response.keys():
        buy_sentiment = get_analysed_news(stock, india_news_analyse_response[stock])
        if buy_sentiment is None:
            buy_sentiment = get_analysed_news(stock, india_news_analyse_response[stock], secondary=True)
        print(stock, buy_sentiment)
        final_sentiment_analysis[stock] = buy_sentiment
    handle_orders(final_sentiment_analysis, incoming_stocks)


if __name__ == '__main__':
    with open("stocks.json", "r") as f:
        stocks = loads(f.read())
        print(len(stocks))

    get_news_for_all(stocks)
    # update_stock_scrip_codes()

    # scrips_update_scheduler = BackgroundScheduler()
    # scrips_update_scheduler.add_job(update_stock_scrip_codes, 'interval', seconds=172800)
    # scrips_update_scheduler.start()
    #
    # news_scheduler = BackgroundScheduler()
    # trigger = CronTrigger(
    #     year="*", month="*", day="*", hour="12", minute="0", second="5"
    # )
    # news_scheduler.add_job(get_news_for_all, args=(stocks,), trigger=trigger)
    # news_scheduler.start()
    #
    # print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    #
    # try:
    #     # This is here to simulate application activity (which keeps the main thread alive).
    #     while True:
    #         time.sleep(2)
    # except (KeyboardInterrupt, SystemExit):
    #     # Not strictly necessary if daemonic mode is enabled but should be done if possible
    #     scrips_update_scheduler.shutdown()
    #     news_scheduler.shutdown()
