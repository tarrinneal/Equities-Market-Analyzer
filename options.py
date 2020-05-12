import requests, datetime, dateutil, math, scipy.stats

class OptionsContract:
  def __init__(self, *args, **kwargs):
    self.__dict__ = kwargs

  def Simplify(self):
    return {
      'Company Ticker' : self.__dict__['Company Ticker'],
      'Type' : self.__dict__['Type'],
      'Description' : self.__dict__['Description'],
      'Symbol' : self.__dict__['Symbol'],
      'Black-Scholes Value' : self.__dict__['Black-Scholes Value'],
      'TD-Ameritrade Value' : self.__dict__['TD-Ameritrade Value'],
      'Premium' : self.__dict__['Premium'],
      'Contract Rating' : self.__dict__['Contract Rating']
    }

class Options:
  __API_KEY = "9TCE4V1ADTXKGSVZF8Q9RMRQK7OJQBW4"
  __OPTIONS_URL = 'https://api.tdameritrade.com/v1/marketdata/chains'
  
#region Black-Scholes Model
  @staticmethod
  def __d1(s,x,r,t,o):
    return (math.log(s/x, math.e)+t*(r+pow(o,2)*0.5))/(o*math.sqrt(t))
  @staticmethod
  def __d2(d1, o, t):
    return d1 - o * math.sqrt(t)

  @staticmethod
  def __call_value(current_price, exercise_price, interest_rate, time, log_std_dev):
    d1 = Options.__d1(current_price, exercise_price, interest_rate, time, log_std_dev)
    d2 = Options.__d2(d1, log_std_dev, time)
    
    n1 = scipy.stats.norm.cdf(d1)
    n2 = scipy.stats.norm.cdf(d2)

    v_0 = round(current_price * n1 - exercise_price / pow(math.e, interest_rate*time) * n2, 3)
    return v_0
  
  @staticmethod
  def __put_value(current_price, exercise_price, interest_rate, time, log_std_dev):
    v_c = Options.__call_value(current_price, exercise_price, interest_rate, time, log_std_dev)

    v_0 = round(v_c + exercise_price / pow(math.e, interest_rate*time) - current_price, 3)
    return v_0
  
  @staticmethod
  def CallValue(option):
    return Options.__call_value(option.underlyingPrice, option.strikePrice, option.interestRate / 100, option.daysToExpiration/365, option.volatility/100) 
  @staticmethod
  def PutValue(option):
    return Options.__put_value(option.underlyingPrice, option.strikePrice, option.interestRate / 100, option.daysToExpiration/365, option.volatility/100) 
#endregion
  
  @staticmethod
  def GetOptions(ticker_symbol, to_date, valuable = True):
    def get_options(contract_type, options_json):
      contract_location = "callExpDateMap" if contract_type == 'call' else "putExpDateMap"
     
      contracts = []
      for exp_date in options_json[contract_location]:
        for contract in options_json[contract_location][exp_date]:
          new_opt = OptionsContract(**options_json[contract_location][exp_date][contract][0])
          
          if new_opt.daysToExpiration <= 0.0 or options_json['underlyingPrice'] == 0:
            continue
          
          new_opt.companyTicker = options_json['symbol']
          new_opt.interestRate = options_json['interestRate']
          new_opt.volatility = options_json['volatility']
          new_opt.underlyingPrice = options_json['underlyingPrice']
          new_opt.blackScholes = Options.CallValue(new_opt) if contract_type == 'call' else Options.PutValue(new_opt)
          new_opt.theoreticalOptionValue = float(new_opt.theoreticalOptionValue)
          new_opt.ask = float(new_opt.ask)

          try:
            if new_opt.theoreticalOptionValue == 'nan' or new_opt.theoreticalOptionValue == -999.0:
              new_opt.ContractRating = round((new_opt.blackScholes - new_opt.ask) / new_opt.blackScholes * 100, 2)
            else:
              new_opt.ContractRating = round(100 * ((new_opt.blackScholes + new_opt.theoreticalOptionValue) / (2 * new_opt.ask) - 1), 2)
          except RuntimeWarning:
            new_opt.ContractRating = new_opt.blackScholes - new_opt.ask

          if valuable:
            if (float(new_opt.blackScholes) > float(new_opt.ask) or (new_opt.theoreticalOptionValue != 'NaN' and float(new_opt.theoreticalOptionValue) > float(new_opt.ask))):
              contracts.append(new_opt)
          else: contracts.append(new_opt)
      return contracts

    r = requests.get(url=Options.__OPTIONS_URL, params = {
      'apikey' : Options.__API_KEY,
      'symbol' : ticker_symbol,
      'contractType' : "ALL",
      'strikeCount' : 50,
      'includeQuotes' : "True",
      "strategy" : 'SINGLE',
      'fromDate' : datetime.datetime.now(),
      'toDate' : to_date
      })

    if r.status_code != 200:
      return []

    options = r.json()
    
    return get_options('call', options) + get_options('put', options)
