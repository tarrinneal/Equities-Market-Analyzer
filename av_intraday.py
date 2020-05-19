import json as js, datetime as dt, pandas as pd, math, time, os, re
from dateutil.relativedelta import relativedelta
from alpha_vantage.timeseries import *

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
API_KEYS_FILE = CURRENT_DIRECTORY + '/assets/api_keys.txt'
SYMBOLS_FILE = CURRENT_DIRECTORY + '/assets/symbols.txt'
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
CURRENCY = '${:,.2f}'

def GetCAGR(starting : float, ending : float, start_date : dt.datetime, stop_date : dt.datetime) -> float:
  return math.pow((ending / starting), 365 / (stop_date - start_date).days) - 1

def GetPerformance(ticker : str, capital : float, interval : str):
  def get_date(s : str) -> dt.date:
    return dt.datetime.strptime(s, TIME_FORMAT).date()
  def get_weight(n : int, i : int) -> float:
    return (2 * (n - i + 1)) / (n * (n + 1))

  #region Setup
  # Get API Key
  with open(API_KEYS_FILE, mode='r+') as akf:
    key_match = re.search('^alpha_vantage\=(.+)$', akf.read(), flags=re.MULTILINE)
  
  if type(key_match) == None:
    raise KeyError("Could not locate Alpha Vantage API key")
  
  # Use AlphaVantage to get Intraday Trading History
  ts = TimeSeries(key= key_match.groups()[0], output_format='json')
  data, meta_data = ts.get_intraday(ticker, interval=interval, outputsize='full')
  
  # Get Dates available to create model
  available_times = list(reversed([x for x in data]))

  # Setup for model computation
  trading_days = []
  date_history = []
  start_date = get_date(available_times[0])
  current_date = start_date

  for datetime in available_times:
    date = get_date(datetime)
    
    if date != current_date:
      current_date = date
      trading_days.append(date_history)
      date_history = []
    
    date_history.append(data[datetime])
  
  shares_outstanding = 0
  uninvested_cash = 0
  #endregion

  for x in range(0, len(trading_days)):
    uninvested_cash += capital * get_weight(trading_days.__len__(), x + 1)
    split = uninvested_cash / len(trading_days[x])

    for y in range(0, len(trading_days[x])):
      shares_outstanding += split / float(trading_days[x][y]['1. open'])
    
    portfolio_value = shares_outstanding * float(trading_days[x][y]['4. close'])

    if portfolio_value > uninvested_cash:
      shares_outstanding = 0
      uninvested_cash = portfolio_value
    else:
      shares_outstanding = 0
      uninvested_cash = portfolio_value
    
  cagr = round(GetCAGR(capital, uninvested_cash, start_date, current_date), 3)
  return [ticker, start_date, current_date, uninvested_cash, shares_outstanding, cagr]

title = f"Portfolio data for '{SYMBOLS_FILE}''"
print(f"{title}\r\n{''.join(['-'] * len(title))}")

with open(SYMBOLS_FILE, mode='r+') as sf:
  symbols = [line.strip('\n') for line in sf.readlines()]
  
starting_capital = 2000
split = starting_capital / len(symbols)
performances = []

start_min = dt.datetime.now()
counter = 0

for x in symbols:
  print(f'Getting Data for {x}{"".join([" "] * 5)}', end='\r')
  if counter == 5 and (dt.datetime.now() - start_min).total_seconds() < 60:
    time.sleep(60 - (dt.datetime.now() - start_min).total_seconds())
    counter = 0
    start_min = dt.datetime.now()

  performances.append(GetPerformance(x, split, '15min'))
  counter += 1
  
df = pd.DataFrame.from_records(performances, columns=['Ticker', 'Start Date', 'Stop Date', 'Portfolio Value', 'Outstanding Shares', 'CAGR'])
total_portfolio_value = df['Portfolio Value'].sum()

df['Portfolio Value'] = df.apply(lambda row: CURRENCY.format(row['Portfolio Value']), axis=1)
df['CAGR'] = df.apply(lambda row: f"{round(row['CAGR'] * 100,3)}%", axis=1)

portfolio_cagr = round(100 * GetCAGR(starting_capital, total_portfolio_value, performances[0][1], performances[0][2]), 3)
print(df)
print("Cash In Hand: ", CURRENCY.format(total_portfolio_value))
print("Portfolio CAGR: ", f"{portfolio_cagr}%")