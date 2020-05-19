import enum, locale, re, requests, math, scipy.stats, sqlite3, json as js, datetime as dt

from typing import *
from dateutil import relativedelta

from pandas import read_sql, DataFrame
from pandas_datareader import DataReader, _utils


class SecurityType(enum.Enum):
  Equity = 0 
  Option = 1
  EquityListing = 2

class EquityListing:
  __properties = ['Symbol', 'CompanyName']

  def __init__(self, *args, **kwargs):
    if len(args) > 0:
      self.__dict__ = {self.__properties[index] : arg for (index, arg) in enumerate(args, start=0)}
    else:
      self.__dict__ = kwargs

  @staticmethod
  def GetListedEquities(status_func : Optional[Callable[[str], None]] = None,
                        progress_func : Optional[Callable[[int, int, dt.datetime], None]] = None) -> List['EquityListing']:
    """
    This function will use the NASDAQ API to retrieve data on stocks and ETFs
    """
    all_equities = []
    
    page_size = 200
    stocks_url = 'https://www.nasdaq.com/api/v1/screener?page={}&pageSize={}'
    etfs_url = 'https://api.nasdaq.com/api/screener/etf?offset={}'
    
    # We use this variable for HTTP requests to NASDAQ because they do not
    # allow automated HTTP requests. This header will simulate a real user
    # accessing their API.
    fake_header = { 
      'user-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
    }

    # --- SECTION: Get basic info for stocks ---i
    if status_func:
      status_func("Scraping web for listed stocks")
    start_time = dt.datetime.now()

    # Retrieve the total count of pages to access
    first_page_text = requests.get(stocks_url.format(1, 1), headers=fake_header).text
    total_page_count = int(js.loads(first_page_text)['count'] / page_size + 0.9)

    for page_index in range(1, total_page_count):
      # Download JSON file from NASDAQ
      current_page_text = requests.get(stocks_url.format(page_index, page_size), headers=fake_header).text
      current_page_json = js.loads(current_page_text)

      # Add all stocks into all_securities
      all_equities.extend([EquityListing(stock['ticker'], stock['company']) for stock in current_page_json['data']])

      if progress_func:
        progress_func(page_index, total_page_count - 1, start_time)
    # --- END SECTION ---

    # --- SECTION: Get basic info for etfs ---
    if status_func:
      status_func("Scraping web for listed ETFs")
    start_time = dt.datetime.now()

    # Retrieve the total count of pages to access
    first_page_text = requests.get(etfs_url.format(0), headers=fake_header).text
    total_page_count = int(js.loads(first_page_text)['data']['records']['totalrecords'] / 50 + 0.9)

    for page_index in range(1, total_page_count):
      # Download JSON file from NASDAQ
      current_page_text = requests.get(etfs_url.format((page_index - 1) * 50), headers=fake_header).text
      current_page_json = js.loads(current_page_text)

      # Add all ETFs into all_securities
      all_equities.extend([EquityListing(etf['symbol'], etf['companyName'] if etf['companyName'] != None else "N/A") for etf in current_page_json['data']['records']['data']['rows']])

      if progress_func:
        progress_func(page_index, total_page_count - 1, start_time)
    # --- END SECTION --- 
    
    return all_equities


class Equity:
  __properties = ['Symbol', 'CompanyName', '1D', '1W', '1M', '3M', '1Y', '5Y', '10Y', 'Max', 'LastUpdated']
  
  def __init__(self, *args, **kwargs):
    if len(args) > 0:
      self.__dict__ = {self.__properties[index] : arg for (index, arg) in enumerate(args, start=0)}
    else:
      self.__dict__ = kwargs

  @staticmethod
  def __time_range_to_date(time_range : str) -> dt.datetime:
    """
    Converts a time range (ex. '1m', '5', 'max') to a datetime ojbect
    """

    if time_range.lower() == 'max':
      return dt.datetime(1900,1,1)

    multiplier, period = re.search("(\d+)([dwmy])", time_range.lower()).groups()
    multiplier = int(multiplier)

    if period == 'd': return dt.datetime.now() + relativedelta.relativedelta(days=-multiplier)
    elif period == 'w': return dt.datetime.now() + relativedelta.relativedelta(weeks=-multiplier)
    elif period == 'm': return dt.datetime.now() + relativedelta.relativedelta(months=-multiplier)
    elif period == 'y': return dt.datetime.now() + relativedelta.relativedelta(years=-multiplier)

  @staticmethod
  def GetHistoricalData(symbol : str, time_range : str) -> Optional[DataFrame]: 
    """
    This function will retrieve historical trading data for the symbol and over the time 
    range specified in a pandas DataFrame object. Returns None if the data is not retrievable
    """ 

    time_format = "%Y-%m-%d"
    start_date = Equity.__time_range_to_date(time_range)
    end_date = dt.datetime.now()

    symbol_could_not_be_fixed = False
    while True:
      try:
        df = DataReader(symbol, data_source='yahoo', start=start_date, end=end_date)
        break
      except KeyError: # Yahoo does not recognize the inputted equity symbol
        if symbol_could_not_be_fixed:
          return None
        else:
          symbol = symbol.replace('.', '-')
          symbol_could_not_be_fixed = True
      except _utils.RemoteDataError: # Yahoo does not have trading data available for this equity
        return None
      except ConnectionError: # Most likely just a timeout. Retry the request
        continue

    start_dates_match = abs((dt.datetime.strptime(df.index[0]._date_repr, time_format)  - start_date).days) < 5
    end_dates_match   = abs((dt.datetime.strptime(df.index[-1]._date_repr, time_format) - end_date).days) < 5

    if not(start_dates_match or end_dates_match):
      return None
    
    return df

  @staticmethod
  def GetPercentChangeOverTimeRanges(symbol : str, time_ranges : List[str]) -> List[dict]:
    """
    This function will get the percent change of equity share price
    of a set of different time ranges.
    """
    
    def get_percent_change(pd_dataframe):
      """
      This will calculate the percent change of a share over some time frame by reading DataFrame values
      """
      time_format = "%Y-%m-%d"

      open_val = pd_dataframe.iloc[0]['Open']
      close_val = pd_dataframe.iloc[-1]['Adj Close']

      if open_val == 0:
        return "N/A"
      else:
        return round((close_val - open_val) / open_val * 100, 2)

    symbol_requires_formatting = False
    percent_changes = []

    for time_range in time_ranges:
      historical_data = Equity.GetHistoricalData(symbol, time_range)
    
      if not isinstance(historical_data, DataFrame):
        percent_changes.append('N/A')
      else:
        percent_changes.append(get_percent_change(historical_data))

    return percent_changes

  @staticmethod
  def BacktestDollarCostAveraging(symbol : str, start_date : str, principal : float, periodic_investment : float, period : int) -> Dict:
    ratings = [letter * x for letter in ['A', 'B', 'C', 'D'] for x in range(1,4)]
    
    def remap(value : float, old_low : float, old_high : float, new_low : float, new_high : float) -> float:
      return new_low + (value - old_low) * (new_high - new_low) / (old_high - old_low)

    def get_rating(x : float) -> float:
      e_X = pow(math.e, x)
      new_val = e_X / (e_X + 1)

      return ratings[int(len(ratings) - remap(new_val, 0, 1, 0, len(ratings)))]

    df = Equity.GetHistoricalData(symbol, start_date)

    if type(df) != DataFrame:
      return None
    
    total_shares_purchased = principal / df.iloc[0, :]['Adj Close']
    total_money_spent = principal

    current_date = Equity.__time_range_to_date(start_date)

    while current_date < dt.datetime.now():
      current_date += relativedelta.relativedelta(days=period)

      try: 
        total_shares_purchased += (periodic_investment / float(df.loc[current_date.strftime('%Y-%m-%d'), :]['Adj Close']))
        total_money_spent += periodic_investment

      except KeyError:
        current_date += relativedelta.relativedelta(days=1)
    
    value = total_shares_purchased * df.iloc[-1, :]['Adj Close']
    profit = value - total_money_spent

    investing_years = round((df.index[-1] - df.index[0]).days / 365, 2)

    return_obj = {
      'Symbol' : symbol.upper(),
      'Start Date' : df.index[0],
      'Investment Time' : f'{investing_years} years',
      'Money Spent' : locale.currency(total_money_spent, grouping=True),
      'Shares Purchased' : round(total_shares_purchased, 2),
      'Market Value' : locale.currency(value, grouping=True),
      'Net Profit' : locale.currency(profit, grouping=True),
      'Investment Rating' : get_rating(profit / total_money_spent / investing_years)
    }

    return return_obj

class Option:
  class OptionType(enum.Enum):
    Call = "CALL"
    Put = "PUT"

  class Contract:
    """
    The Contract class is a wrapper class for the JSON objects retrieved from the TD-Ameritrade API
    and additional information for the user.
    """

    def __init__(self, *args, **kwargs):
      self.__dict__ = kwargs

  __properties = ['CompanySymbol', 'Type', 'Description', 'Symbol', 'BlackScholesValue', 'TDAmeritrade', 'Premium', 'ContractRating', 'LastUpdated']

  def __init__(self, *args, **kwargs):
    if len(args) > 0:
      self.__dict__ = {self.__properties[index] : arg for (index, arg) in enumerate(args, start=0)}
    else:
      self.__dict__ = kwargs

  @staticmethod
  def __d1(s, x, r, t, o):
    return (math.log(s / x, math.e) + t * (r + pow(o, 2) * 0.5)) / (o * math.sqrt(t))

  @staticmethod
  def __d2(d1, o, t):
    return d1 - o * math.sqrt(t)

  @staticmethod
  def __call_value(current_price, exercise_price, interest_rate, time, log_std_dev):
    d1 = Option.__d1(current_price, exercise_price, interest_rate, time, log_std_dev)
    d2 = Option.__d2(d1, log_std_dev, time)
    
    n1 = scipy.stats.norm.cdf(d1)
    n2 = scipy.stats.norm.cdf(d2)

    v_0 = round(current_price * n1 - exercise_price / pow(math.e, interest_rate*time) * n2, 3)
    return v_0

  @staticmethod
  def __put_value(current_price, exercise_price, interest_rate, time, log_std_dev):
    v_c = Option.__call_value(current_price, exercise_price, interest_rate, time, log_std_dev)

    v_0 = round(v_c + exercise_price / pow(math.e, interest_rate * time) - current_price, 3)
    return v_0

  @staticmethod
  def __time_range_to_date(time_range : str) -> dt.datetime:
    """
    Converts a time range (ex. '1m', '5', 'max') to a datetime ojbect
    """

    if time_range.lower() == 'max':
      return dt.datetime(1900,1,1)

    multiplier, period = re.search("(\d+)([dwmy])", time_range.lower()).groups()
    multiplier = int(multiplier)

    if period == 'd': return dt.datetime.now() + relativedelta.relativedelta(days=multiplier)
    elif period == 'w': return dt.datetime.now() + relativedelta.relativedelta(weeks=multiplier)
    elif period == 'm': return dt.datetime.now() + relativedelta.relativedelta(months=multiplier)
    elif period == 'y': return dt.datetime.now() + relativedelta.relativedelta(years=multiplier)

  @staticmethod
  def __json_to_options(contract_type : 'OptionType', json : str, get_valuable = True) -> List['Option']:
    contract_location = "callExpDateMap" if contract_type == Option.OptionType.Call else 'putExpDateMap'

    contracts = []
    for exp_date in json[contract_location]:
      for contract in json[contract_location][exp_date]:
        new_opt = Option.Contract(**json[contract_location][exp_date][contract][0])
        
        if new_opt.daysToExpiration <= 0.0 or json['underlyingPrice'] == 0:
          continue
        
        new_opt.CompanySymbol = json['symbol']
        new_opt.interestRate = json['interestRate']
        new_opt.volatility = json['volatility']
        new_opt.underlyingPrice = json['underlyingPrice']
        new_opt.BlackScholes = Option.CallValue(new_opt) if contract_type == Option.OptionType.Call else Option.PutValue(new_opt)
        new_opt.theoreticalOptionValue = float(new_opt.theoreticalOptionValue)
        new_opt.ask = float(new_opt.ask)

        try:
          if new_opt.theoreticalOptionValue == 'nan' or new_opt.theoreticalOptionValue == -999.0:
            new_opt.ContractRating = round((new_opt.BlackScholes - new_opt.ask) / new_opt.BlackScholes * 100, 2)
          else:
            new_opt.ContractRating = round(100 * ((new_opt.BlackScholes + new_opt.theoreticalOptionValue) / (2 * new_opt.ask) - 1), 2)
        except RuntimeWarning:
          new_opt.ContractRating = new_opt.BlackScholes - new_opt.ask

        if get_valuable:
          if (float(new_opt.BlackScholes) > float(new_opt.ask) or (new_opt.theoreticalOptionValue != 'NaN' and float(new_opt.theoreticalOptionValue) > float(new_opt.ask))):
            contracts.append(new_opt)
        else: contracts.append(new_opt)
    return contracts

  @staticmethod
  def CallValue(contract : 'Contract') -> float:
    """
    This function returns the Black-Scholes call value for an options contract
    """
    return Option.__call_value(contract.underlyingPrice, contract.strikePrice, contract.interestRate / 100, contract.daysToExpiration / 365, contract.volatility / 100)

  @staticmethod
  def PutValue(contract : 'Contract') -> float:
    """
    This function returns the Black-Scholes put value for an options contract
    """
    return Option.__put_value(contract.underlyingPrice, contract.strikePrice, contract.interestRate / 100, contract.daysToExpiration / 365, contract.volatility / 100) 

  @staticmethod
  def GetOptions(td_ameritrade_api_key : str, symbol : str, to_date : str) -> List['Option']:
    """
    This function will use the TD Ameritrade API to retrieve Option(s) for the symbol available
    up to the specified to_date
    """

    options_url = 'https://api.tdameritrade.com/v1/marketdata/chains'
    request = requests.get(url = options_url, params = {
      'apikey' : td_ameritrade_api_key,
      'symbol' : symbol,
      'contractType' : "ALL",
      'strikeCount' : 50,
      'includeQuotes' : "True",
      "strategy" : "SINGLE",
      "fromDate" : dt.datetime.now(),
      "toDate" : Option.__time_range_to_date(to_date)
    })

    if request.status_code != 200:
      return []
    
    json = request.json()

    contracts =  Option.__json_to_options(Option.OptionType.Call, json) + Option.__json_to_options(Option.OptionType.Put, json)
    
    options = []
    for contract in contracts:
      options.append(Option(
        contract.CompanySymbol,
        contract.putCall,
        contract.description,
        contract.symbol,
        contract.BlackScholes,
        contract.theoreticalOptionValue,
        contract.ask,
        contract.ContractRating
        ))
        
    return options

class RelationalOperator(enum.Enum):
  EqualTo = '='
  NotEqualTo = '<>'
  LessThan = '<'
  LessThanOrEqualTo = '<='
  GreaterThan = '>'
  GreaterThanOrEqualTo = '>='
  Between = 'BETWEEN'
  Like = 'LIKE'
  In = 'IN'

class Ordering(enum.Enum):
  Ascending = 'ASC'
  Descending = 'DESC'

class SecurityDatabaseWrapper:
  def __init__(self, database_path):
    self.__conn = sqlite3.connect(database_path)
    self.__conn.row_factory = sqlite3.Row     

    self.__cursor = self.__conn.cursor()
    self.__cursor.execute("""CREATE TABLE IF NOT EXISTS ListedEquities (
                              Symbol CHAR(10),
                              CompanyName CHAR(255))""")

    self.__cursor.execute("""CREATE TABLE IF NOT EXISTS Equities (
                              Symbol CHAR(10),
                              CompanyName CHAR(255),
                              '1D' FLOAT(5),
                              '1W' FLOAT(5),
                              '1M' FLOAT(5),
                              '3M' FLOAT(5),
                              '1Y' FLOAT(5),
                              '5Y' FLOAT(5),
                              '10Y' FLOAT(5),
                              Max FLOAT(5),
                              LastUpdated DATETIME DEFAULT CURRENT_TIMESTAMP)""")

    self.__cursor.execute("""CREATE TABLE IF NOT EXISTS Options (
                              CompanySymbol CHAR(10),
                              Type CHAR(4),
                              Description CHAR(255),
                              Symbol CHAR(255),
                              BlackScholesValue FLOAT(10),
                              TDAmeritrade FLOAT(10),
                              Premium FLOAT(10),
                              ContractRating FLOAT(20),
                              LastUpdated DATETIME DEFAULT CURRENT_TIMESTAMP)""")

    self.__cursor.execute("""SELECT * FROM sqlite_master
                             WHERE type = 'trigger'""")
    trigger_list = self.__cursor.fetchall()
                             
    if len(trigger_list) != 2:
      self.__cursor.execute("""CREATE TRIGGER Update_Equities_LastUpdated
                              AFTER UPDATE ON Equities
                              FOR EACH ROW
                              BEGIN
                                  UPDATE Equities SET LastUpdated = CURRENT_TIMESTAMP WHERE Symbol=old.Symbol;
                              END""")

      self.__cursor.execute("""CREATE TRIGGER Update_Options_LastUpdated
                              AFTER UPDATE ON Options
                              FOR EACH ROW
                              BEGIN
                                  UPDATE Options SET LastUpdated = CURRENT_TIMESTAMP WHERE Symbol=old.Symbol;
                              END""")      
  
  @staticmethod
  def __get_table_name(security : Union[Equity, Option, SecurityType]) -> str:
    """
    Finds the corresponding table name for the security specified
    """

    if isinstance(security, Equity):
      return 'Equities'

    elif isinstance(security, Option):
      return 'Options'

    elif isinstance(security, EquityListing):
      return "ListedEquities"

    elif isinstance(security, SecurityType):
      if security == SecurityType.Equity:
        return 'Equities'

      elif security == SecurityType.Option:
        return 'Options'

      elif security == SecurityType.EquityListing:
        return 'ListedEquities'
      else:
        ValueError("Literally impossible for this to happen")
    else: 
      raise ValueError("Literally impossible for this to happen")

  @staticmethod
  def _validate_column_name(col_name : str) -> str:
    """
    Assure that the column name is SQL valid
    """

    if col_name[0].isdigit():
      return f'"{col_name}"'
    return col_name

  @staticmethod
  def _validate_value(value : Any) -> str:
    """
    Assure that the value is SQL valid
    """
    if isinstance(value, str):
      value = value.replace("'", "''")
      return f'"{value}"'
    return f"{value}"

  @staticmethod
  def __convert_to_sql_where(conditions : List[Tuple[Any, RelationalOperator, Any]]) -> str:
    """
    Takes the conditions in tuple format and converts it to a proper SQL WHERE clause
    """

    formatted_identifiers = []

    for identifier in conditions:
      col_name, relation, value = identifier

      if relation == RelationalOperator.Between and len(value) != 2:
        raise ValueError("Between relational operator requires the value parameter to be a list of length 2")
      
      if relation != RelationalOperator.Between:
        value = SecurityDatabaseWrapper._validate_value(value)
      
      col_name = SecurityDatabaseWrapper._validate_column_name(col_name)

      formatted_identifiers.append((col_name, relation, value))

    where_clause_section = ' AND '.join([f'{col_name} {relation.value} {value}' for col_name, relation, value in formatted_identifiers])
    return f"({where_clause_section})"

  def CloseConnection(self):
    self.__conn.close()

  def AddNewSecurity(self, security : Union[Equity, Option, EquityListing]) -> None:
    """
    Adds security to corresponding table in database
    """
    
    table_name = self.__get_table_name(security)
    
    self.Insert(table_name, security.__dict__.keys(), security.__dict__.values())
  
  def ModifySecurities(self, new_security : Union[Equity, Option],
                              condition : Tuple[Any, RelationalOperator, Any]) -> None:
    """
    Changes security entry that fits the condition parameter to the new security parameter. The condition
    works as such: Tuple(<column to look at>, <relation of the two values>, <value(s) to look for>)
    """
    table_name = self.__get_table_name(new_security)

    set_clause  = ", ".join([f"{self._validate_column_name(key)} = '{value}'" for key, value in new_security.__dict__.items()])
    where_clause = self.__convert_to_sql_where([condition])

    self.__cursor.execute(f"""UPDATE {table_name}
                              SET {set_clause}
                              WHERE {where_clause}""")

  def DeleteSecurity(self, security : Union[Equity, Option]) -> None:
    """
    Delete security from the database.
    """

    table_name = self.__get_table_name(security)

    # Query for the security with all matching key, value pairs
    where_clause = self.__convert_to_sql_where([(key, RelationalOperator.EqualTo, value) for key, value in security.__dict__.items()])
  
    self.__cursor.execute(f"""DELETE FROM {table_name}
                              WHERE {where_clause}""")

  def DeleteSecuritiesConditional(self, security_type : SecurityType, conditions : List[Tuple[Any, RelationalOperator, Any]] = None) -> None:
    """
    Deletes securities from database according to the conditions provided
    """
    
    table_name = self.__get_table_name(security_type)

    where_clause = self.__convert_to_sql_where(conditions)

    self.__cursor.execute(f"""DELETE FROM {table_name}
                              WHERE {where_clause}""")

  def GetSecurities(self, security_type : SecurityType,
                          conditions : Optional[List[Tuple[Any, RelationalOperator, Any]]] = None,
                          order_by_cols : Optional[List[Tuple[str, Ordering]]] = None) -> List[Union[Equity, Option]]:
    """
    Finds all securities of type security_type with the specified conditions and ordering by columns
    """
    table_name = self.__get_table_name(security_type)

    select_clause = f"SELECT * FROM {table_name} "

    if conditions != None:
      where_clause = self.__convert_to_sql_where(conditions)
      select_clause += f"WHERE {where_clause} "
    if order_by_cols != None:
      order_by_clause = ", ".join([f"{self._validate_column_name(col_name)} {ordering_type.value}" for (col_name, ordering_type) in order_by_cols])
      select_clause += f"ORDER BY {order_by_clause} "

    self.__cursor.execute(select_clause + ';')
    results = self.__cursor.fetchall()
    
    if security_type == SecurityType.Equity:
      return [Equity(*equity) for equity in results]

    elif security_type == security_type.Option:
      return [Option(*option) for option in results]
      
    elif security_type == SecurityType.EquityListing:
      return [EquityListing(*equityListing) for equityListing in results]

  def ExecuteSQLStatement(self, sql : str) -> Optional[List[Any]]:
    """
    Execute some SQL statement to the database and return any results if applicable.
    Famous last words: hopefully the user knows what they're doing when using this method
    """
    self.__cursor.execute(sql)

    results = [dict(zip(row.keys(), row)) for row in self.__cursor.fetchall()]

    return results

  def Insert(self, table, columns : List, values : List) -> None:
    columns_clause = ", ".join([self._validate_column_name(col_name) for col_name in columns])
    values_clause = ", ".join([self._validate_value(value) for value in values])

    sql_statement = f"""INSERT INTO {table} ({columns_clause})
                        VALUES ({values_clause});"""
    self.__cursor.execute(sql_statement)

  def Save(self) -> None:
    """
    Saves the changes made to the database
    """
    self.__conn.commit()
