import math, time, re, urllib.request, dateutil.relativedelta, locale

import options
import datetime as dt
import matplotlib.pyplot as plt
from matplotlib import style
import pandas as pd
import pandas_datareader as web
from gsheet import SheetService
import json as js

#region Startup Conditions
UPDATE_COMP_LIST = False # True will update the companies of your holdings
DOWNLOAD_TRADE_DATA = False # True will download historical trading data for your holdings
GET_STOCKS = False 
GET_ETFS = False
GET_OPTIONS = False
UPDATE_YIELDS = 0
#endregion
#region Program Constants
SPREADSHEET_ID = '1WwWiShznSibNzz8czwR9FYXb_zGJQmoKltM8gIUHFTc'
COMPANIES_FILE = 'Trading Data/companies.txt'
STOCKS_FILE = 'Trading Data/stocks.json'
ETFS_FILE = 'Trading Data/etfs.json'
OPTIONS_FILE = 'Trading Data/options.json'

PAGE_COUNT = 200
STOCKS_URL = 'https://www.nasdaq.com/api/v1/screener?page={}&pageSize={}'
ETFS_URL = 'https://api.nasdaq.com/api/screener/etf?offset={}'

TIME_RANGES = ['1d', '1w', '1m', '3m', '1y', '5y', '10y', 'max']
OPTIONS_CONTRACT_RANGE = "3m"
#endregion
#region Global Variables
sheet_serv = SheetService()
sheet_serv.Login()
#endregion
#region CLI Functions
def printProgramStatus(message):
  curr_time = dt.datetime.now().strftime("%X")
  print(f"{curr_time}\t{message}")
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
def __call_prog_bar(start, stop, current, final, msg = ""):
  time_remaining =  time.strftime("%H hours, %M minutes, %S seconds remaining", time.gmtime((stop - start).total_seconds() * (final / current - 1))) 
  printProgressBar(current, final, prefix = 'Progress:', suffix = f'Complete, {time_remaining}{msg:20}', length = 50)
#endregion
#region Google Sheets Interface
def GetCompanies(from_google_sheets):
  companies = []
  txt = ""

  if from_google_sheets:
    ranges = ["Holdings!A2:A", "Holdings!E2:E"]

    symbols = sheet_serv.GetSheetData(SPREADSHEET_ID, ranges[0])
    descriptions = sheet_serv.GetSheetData(SPREADSHEET_ID, ranges[1])
    
    for x in range(0, len(symbols) - 1):
      companies.append([symbols[x][0], descriptions[x][0]])
      txt += f"{symbols[x][0]},{descriptions[x][0]}\r\n"
  else:
    with open(COMPANIES_FILE, mode='r') as cf:
      cf.readline()

      while True:
        comp = cf.readline().split(sep=',')

        if comp[0] == '':
          break
        
        companies.append([comp[0], comp[1][:-1]])
        txt += str.join(',', comp)

  return companies, txt
#endregion
#region Updating Yield Functions
def LoadTempGrowthData(all_tickers, temp_file):
  """
  @params
    all_tickers - Required: to be filled with data (list)
    temp_file   - Required: where the temp data is located (str)
    type        - Required: either 'stock' or 'etf' (str)
  """
  try:
    with open(temp_file, mode='r') as tf:
      line = tf.readline()
      if len(all_tickers) == 0:
        while line:
          all_tickers.append(js.loads(line))
          line = tf.readline()
        return len(all_tickers) - 1
      else:
        count = 0
        while line:
          all_tickers[count] = js.loads(line)
          count += 1

          line = tf.readline()
        return count
  except FileNotFoundError:
    return
def DownloadTempGrowthData(all_tickers, start_pos, temp_file):
  ticker_size = len(all_tickers)
  
  start = dt.datetime.now()
  for x in range(start_pos, ticker_size):
    stop = dt.datetime.now()
    symbol = all_tickers[x]['Ticker']
    __call_prog_bar(start, stop, x + 1 - start_pos, ticker_size - 1 - start_pos, f"| processing {symbol}")

    trd = GetTimeRangeData(all_tickers[x]['Ticker'])

    for y in range(0, len(TIME_RANGES)):
      all_tickers[x][TIME_RANGES[y]] = trd[y]
    
    with open(temp_file, mode='a+') as tf:
      tf.write(js.dumps(all_tickers[x]) + '\r\n')
def UpdateYields(security, tickers):
  if security == 'stock':
    start_ticker = LoadTempGrowthData(tickers, f"{STOCKS_FILE}.tmp")
    DownloadTempGrowthData(tickers, start_ticker, f"{STOCKS_FILE}.tmp")

    with open(STOCKS_FILE, 'w+') as sf:
      sf.write(js.dumps(tickers, default=lambda o: o.__dict__))

  elif security == 'etf':
    start_ticker = LoadTempGrowthData(tickers, f"{ETFS_FILE}.tmp")
    DownloadTempGrowthData(tickers, start_ticker, f"{ETFS_FILE}.tmp")

    with open(ETFS_FILE, 'w+') as ef:
      ef.write(js.dumps(tickers, default=lambda o: o.__dict__))
#endregion
#region Load-in Securities 
def DownloadSecurityInformation(security_type):
  def html_to_stock(html):
    stocks = []

    stocks_js = js.loads(html)
    for stock in stocks_js['data']:
      stocks.append({
        'Ticker' : stock['ticker'],
        'Company Name' : stock['company'],
        'Sector' : stock['sectorName']
      })
    
    return stocks
  def html_to_etf(html):
    etfs = []

    etfs_js = js.loads(html)
    for etf in etfs_js['data']['records']['data']['rows']:
      etfs.append({
        'Ticker' : etf['symbol'],
        'Company Name' : etf['companyName']
      })
    
    return etfs

  start = dt.datetime.now()

  if security_type == 'stock':
    first_page = urllib.request.urlopen(STOCKS_URL.format(1, PAGE_COUNT)).read()
    page_count = int((js.loads(first_page)['count'] / PAGE_COUNT + 0.9))
    all_stocks = html_to_stock(first_page)

    stop = dt.datetime.now()
    __call_prog_bar(start, stop, 1, page_count)

    for x in range(2, page_count):
      obj = html_to_object(urllib.request.urlopen(STOCKS_URL.format(x, PAGE_COUNT)).read())
      all_stocks += obj
      
      stop = dt.datetime.now()
      __call_prog_bar(start, stop, x, page_count)

    return all_stocks  
  elif security_type == 'etf':
    first_page = urllib.request.urlopen(ETFS_URL.format(0)).read()
    page_count = int((js.loads(first_page)['data']['records']['totalrecords'] / 50 + 0.9))
    all_etfs = html_to_etf(first_page)

    stop = dt.datetime.now()
    __call_prog_bar(start, stop, 1, page_count)

    for x in range(0, page_count - 1):
      obj = html_to_object(urllib.request.urlopen(ETFS_URL.format(x * 50, PAGE_COUNT)).read())
      all_etfs += obj
      
      stop = dt.datetime.now()
      __call_prog_bar(start, stop, x+1, page_count)

    return all_etfs
def GetStocks(save, open_file = False):
  printProgramStatus("Getting Stocks...")
  all_stocks = []
  
  if open_file:
    with open(STOCKS_FILE, "r") as ef:
      all_stocks = js.loads(ef.read())
  else:
    all_stocks = DownloadSecurityInformation('stock')

  if save: 
    with open(STOCKS_FILE, "w+") as sf:
      sf.write(js.dumps(all_stocks, default=lambda o: o.__dict__))

  return all_stocks
def GetETFs(save, open_file = False):
  def html_to_object(page):
    etfs = []

    etfs_js = js.loads(page)
    for etf in etfs_js['data']['records']['data']['rows']:
      etfs.append({
        'Ticker' : etf['symbol'],
        'Company Name' : etf['companyName']
      })
    
    return etfs

  printProgramStatus("Getting ETFs...")
  all_etfs = []
  if open_file:
    with open(ETFS_FILE, "r") as ef:
      all_etfs = js.loads(ef.read())
  else:
    all_etfs = DownloadSecurityInformation('etf')
    
  if save: 
    with open(ETFS_FILE, "w+") as sf:
      sf.write(js.dumps(all_etfs, default=lambda o: o.__dict__))
  return all_etfs
def GetOptions(tickers, max_exp, save, open_file = False):
  def exp_to_datetime(tm):
    multiplier, period = re.search("(\d+)([dwmy])", tm).groups()
    multiplier = int(multiplier)

    if period == 'd': return dt.datetime.now() + dateutil.relativedelta.relativedelta(days=multiplier)
    elif period == 'w': return dt.datetime.now() + dateutil.relativedelta.relativedelta(weeks=multiplier)
    elif period == 'm': return dt.datetime.now() + dateutil.relativedelta.relativedelta(months=multiplier)
    elif period == 'y': return dt.datetime.now() + dateutil.relativedelta.relativedelta(years=multiplier)
  
  all_options = []
  to_date = exp_to_datetime(max_exp)
  start_pos = 0
  
  try:
    with open(f"{OPTIONS_FILE}.tmp", mode="r") as of:
      line = of.readline()
      count = 0

      while line:
        all_options.append(options.OptionsContract(**js.loads(line)))
        count += 1
        line = of.readline()

      if count > 0:
        start_pos = tickers.index(next(x for x in tickers if x['Ticker'] == all_options[count-1].__dict__['Company Ticker'])) + 1
      
  except FileNotFoundError:
    pass
  
  start = dt.datetime.now()
  start_request = dt.datetime.now()
  request_number = 1

  for x in range(start_pos, len(tickers)):
    stop_request = dt.datetime.now()
    if request_number == 119:
      time_dif = (stop_request - start_request).total_seconds()
      
      if time_dif < 60:
        time.sleep(time_dif)
        start_request = dt.datetime.now()
        request_number = 1

    opts = options.Options.GetOptions(tickers[x]['Ticker'], to_date)
    request_number += 1

    with open(f"{OPTIONS_FILE}.tmp", mode="a+") as of:
      for opt in opts:
        of.write(js.dumps(opt.Simplify()) + '\r\n')
  
    stop = dt.datetime.now()
    __call_prog_bar(start, stop, x+1, len(tickers), f", Processing {tickers[x]['Ticker']:5}")
def ConvertTempOptions(ocs):
  try:
    with open(f"{OPTIONS_FILE}.tmp", mode='r') as of:
      line = of.readline()

      while line:
        ocs.append(options.OptionsContract(**js.loads(line)))
        line = of.readline()
    
    with open(OPTIONS_FILE, mode='w+') as of:
      of.write(js.dumps(ocs, default = lambda o: o.__dict__))

    return ocs
  except FileNotFoundError:
    return []
#endregion
def DownloadTradingData(tickers, start, end, get_max = False) -> pd.DataFrame:
  def verify_time_frame(df : pd.DataFrame) -> bool:
    start_time_difference = (df.index[0] - start).days > 5
    stop_time_difference  = (df.index[-1] - end).days > 5

    return not (start_time_difference or stop_time_difference)
    
  if isinstance(tickers, list):
    dfs = []
    for ticker in tickers:
      df = web.DataReader(ticker, 'yahoo', start, end)

      if verify_time_frame(df) or get_max:
        dfs.append(df)
      return dfs
  elif isinstance(tickers, str):
    df = web.DataReader(tickers, 'yahoo', start, end)

    if verify_time_frame(df) or get_max:
      return df
    else:
      pass
      #raise ValueError("Unable to get correct timeframe")
def GetTimeRangeData(ticker):
  def input_to_date(tm):
    multiplier, period = re.search("(\d+)([dwmy])", tm).groups()
    multiplier = int(multiplier)

    if period == 'd': return dt.datetime.now() + dateutil.relativedelta.relativedelta(days=-multiplier)
    elif period == 'w': return dt.datetime.now() + dateutil.relativedelta.relativedelta(weeks=-multiplier)
    elif period == 'm': return dt.datetime.now() + dateutil.relativedelta.relativedelta(months=-multiplier)
    elif period == 'y': return dt.datetime.now() + dateutil.relativedelta.relativedelta(years=-multiplier)
  
  def get_yield(df):
    open_val = df.iloc[0]['Open'] + 0.001
    close_val = df.iloc[-1]['Adj Close']

    return round((close_val - open_val) / open_val * 100, 2)

  not_fixable = False
  time_data = []
  for tr in TIME_RANGES:
    while True:
      try:
        if tr == 'max':
          attempted_date = dt.datetime.now() + dateutil.relativedelta.relativedelta(years=-100)
          df = DownloadTradingData(ticker, attempted_date, dt.datetime.now())
          
          time_data.append(get_yield(df))
        else:
          attempted_date = input_to_date(tr)
          df = DownloadTradingData(ticker, attempted_date, dt.datetime.now())

          time_difference = (df.index[0] - attempted_date).days
        
          if time_difference > 10:
            time_data.append('N/A')
          else:
            time_data.append(get_yield(df))
        
        not_fixable = False
        break
      except KeyError:
        if not_fixable:
          return ['N/A'] * len(TIME_RANGES)
        else:
          ticker = ticker.replace('.', '-')
          not_fixable = True
      except web._utils.RemoteDataError:
        return ['N/A'] * len(TIME_RANGES)
  return time_data
def RunCommandLine(stocks, etfs, options):
  def input_to_date(tm):
    multiplier, period = re.search("(\d+)([dwmy])", tm).groups()
    multiplier = int(multiplier)

    if period == 'd': return dt.datetime.now() + dateutil.relativedelta.relativedelta(days=-multiplier)
    elif period == 'w': return dt.datetime.now() + dateutil.relativedelta.relativedelta(weeks=-multiplier)
    elif period == 'm': return dt.datetime.now() + dateutil.relativedelta.relativedelta(months=-multiplier)
    elif period == 'y': return dt.datetime.now() + dateutil.relativedelta.relativedelta(years=-multiplier)

  search = input("> ").split(sep=' ')
  current_df = ''
  df = None

  while search[0] != "quit":
    range = ":20" if (len(search) == 1 or (len(search) == 2 or search[2] == '')) else search[2]
    start, stop = re.search('(\d*)\:?(\d*)', range).groups()
    start = int(start) if start != "" else 0
    stop = int(stop) if stop != "" else -1

    if search[0] in ['stocks', 'etfs']:
      if search[0] == 'stocks':
        if current_df != 'stocks':
          df = pd.DataFrame.from_dict(stocks)
          current_df = 'stocks'
      elif search[0] == 'etfs':
        if current_df != 'etfs':
          df = pd.DataFrame.from_dict(etfs)
          current_df = 'etfs'
      
      df[search[1]] = pd.to_numeric(df[search[1]], errors='coerce')
      df = df.sort_values(by=[search[1]], ascending=False)
      print(df[start:stop])
    elif search[0] == 'options':
      if current_df != 'options':
        # jj = js.loads(js.dumps(options, default=lambda o: o.__dict__))
        df = pd.DataFrame([x.Simplify() for x in options])
        current_df = 'options'
        
      df = df.sort_values(by=['Contract Rating'], ascending=False)
      df.to_csv('options.csv')
      print(df[start:stop])
    else:
      if current_df != search[0]:
        df = DownloadTradingData(search[0], input_to_date(search[1]), dt.datetime.now())
        print(df[start:stop])
    
    search = input('> ').split(sep=' ')
def Setup():
  if UPDATE_COMP_LIST:
    comp, txt = GetCompanies(True)
    with open(COMPANIES_FILE, mode='w') as cf:
      cf.write(f"Symbol,Description\r\n{txt}")
    
  companies, txt = GetCompanies(False)

  if DOWNLOAD_TRADE_DATA:
    start = dt.datetime(2000, 1, 1)
    end = dt.datetime.now()
    DownloadTradingData([comp[0] for comp in companies], start, end)

  Tickers = [comp[0] for comp in companies]
  
  stocks = GetStocks(1) if GET_STOCKS else GetStocks(1, True)
  etfs = GetETFs(1) if GET_ETFS else GetETFs(1, True) 

  if UPDATE_YIELDS:
    UpdateYields('stock', stocks)
    UpdateYields('etf', etfs)

  if GET_OPTIONS:
    GetOptions(stocks, '3m', True, open_file = False)
    GetOptions(etfs, '3m', True, open_file = False)

  ocs = []
  ConvertTempOptions(ocs)

  RunCommandLine(stocks, etfs, ocs)

def GetTheoreticalInvestmentData(ticker, principal, periodic_investment, period, start_date, end_date, get_max = False) -> dict:
  def remap(value, low1, high1, low2, high2) -> float:
    return low2 + (value - low1) * (high2 - low2) / (high1 - low1)

  def get_rating(x : float) -> float:
    ratings = ['AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC', 'CC', 'C', 'DDD', 'DD', 'D']
    val = sigmoid(x)

    return ratings[int(len(ratings) - remap(val, 0, 1, 0, len(ratings)))]
  
  def sigmoid(x : float) -> float:
    e_X = pow(math.e, x)
    return e_X / (e_X + 1.0)
  
  def add_period(start, period) -> dt.datetime:
    multiplier, period = re.search("(\d+)([dwmy])", period).groups()
    multiplier = int(multiplier)

    if period == 'd': return start + dateutil.relativedelta.relativedelta(days=multiplier)
    elif period == 'w': return start + dateutil.relativedelta.relativedelta(weeks=multiplier)
    elif period == 'm': return start + dateutil.relativedelta.relativedelta(months=multiplier)
    elif period == 'y': return start + dateutil.relativedelta.relativedelta(years=multiplier)
  
  df = DownloadTradingData(ticker, start_date, end_date, get_max)

  if type(df) != pd.DataFrame:
    return None

  total_shares_purchased = principal / df.iloc[0, :]['Adj Close']
  total_money_spent = principal

  while start_date < end_date:
    start_date = add_period(start_date, period)
    
    try:
      total_shares_purchased += (periodic_investment / df.loc[start_date.strftime("%Y-%m-%d"), : ]['Adj Close'])
      total_money_spent += periodic_investment
      
    except KeyError:
      start_Date = add_period(start_date, "1d")

  value = total_shares_purchased * df.iloc[-1, :]['Adj Close']
  profit = value - total_money_spent

  investing_years = round((df.index[-1] - df.index[0]).days / 365, 2)

  return_obj = {
    "Ticker" : ticker.upper(),
    "Start Time" : df.index[0],
    "Investment Time" : f"{investing_years} years",
    "Money Spent" : locale.currency(total_money_spent, grouping=True),
    "Shares Purchased" : round(total_shares_purchased, 2),
    "Holdings Value" : locale.currency(value, grouping=True),
    "Net Profit" : locale.currency(profit, grouping=True),
    "Investment Rating" : get_rating(profit / total_money_spent / investing_years)
  }
  
  return return_obj

if __name__ == "__main__":
  # locale.setlocale(locale.LC_ALL, '')
  # stop = dt.datetime.now() + dateutil.relativedelta.relativedelta(months=-3)
  # start = stop + dateutil.relativedelta.relativedelta(years=-12)

  # start = dt.datetime(2011,1,1)
  # stop  = dt.datetime(2015,1,1)

  # tickers = ['SAVE']#, 'tqqq', 'spy', 'FRCOF', 'EJPRF', 'TOKUF', 'STAEF']

  # returns = []
  # for tick in tickers:
  #   returns.append(GetTheoreticalInvestmentData(tick, 1000, 1000, '1m', start, stop, True))
  

#region gg
  # stocks = GetStocks(0,1)
  # etfs = GetETFs(0, 1)

  # stock_total = len(stocks)
  # total = stock_total + len(etfs) - 1
  # printProgressBar(1, total, length=50)

  # returns = []
  # count = 1
  # for x in stocks:
  #   try:
  #     returns.append(GetTheoreticalInvestmentData(x['Ticker'], 1000, 500, "6m", start, stop, True))
  #     count += 1
  #     printProgressBar(count, total, length=50)
  #   except:
  #     continue

  # for x in etfs:
  #   try:
  #     returns.append(GetTheoreticalInvestmentData(x['Ticker'], 1000, 500, "6m", start, stop, True))
  #     count += 1
  #     printProgressBar(count, total, length=50)
  #   except:
  #     continue
#endregion


  # df = pd.DataFrame.from_dict(returns)
  # df.to_csv('returns.csv')
  # print(df)
  Setup()
  exit(0)
  