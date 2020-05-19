import os, time, tabulate, locale, datetime as dt

from typing import *
from security_db_wrapper import *

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
DATABASE_FILE_PATH = '/assets/securities_data.db'
API_FILE_PATH = '/assets/api_keys.txt'
LOG_FILE_PATH = '/assets/logs - {}.txt'
HELP_FILE_PATH = '/assets/program_help.txt'

security_db = None
td_ameritrade_api_key = ""

def ProgramStatusUpdate(message : str, log = False) -> None:
  """
  Format message to print to screen for user
  """

  current_time = dt.datetime.now().strftime('%X')
  print_message = f"{current_time} - {message}"

  print(f"{current_time} - {message}")

  if log:
    current_log_file = CURRENT_DIRECTORY + LOG_FILE_PATH.format(dt.datetime.now().date().strftime('%Y-%m-%d'))
    
    with open(current_log_file, mode='a+') as log_file:
      log_file.write(print_message + '\n')

def ProgressBar(iteration : int, total : int, start_time : dt.datetime, length = 75, message = "", log = False) -> None:
  """
  This function creates a progress bar in the command line.
  Args:
    iteration     - current increment
    total         - total increments to iterate over
    start_time    - time when the progress bar was first generated
    length        - length of progress bar in command line
    message       - message to append to the right of the progress bar
    log           - True to write progress bar to log file
  """
  
  percent = ("{0:.2f}").format(100 * (iteration / total))
  filledLength = int(length * iteration // total)
  bar = '█' * filledLength + '-' * (length - filledLength)

  time_remaining = time.strftime("%H hours, %M minutes, %S seconds remaining", time.gmtime((dt.datetime.now() - start_time).total_seconds() * (total / iteration - 1)))

  print_message = f'\r|{bar}| {percent}% | {time_remaining} | {message:20}'
  print(print_message, end='\r')

  if iteration == total:
    print("\r\n")

  if log:
    current_log_file = CURRENT_DIRECTORY + LOG_FILE_PATH.format(dt.datetime.now().date().strftime('%Y-%m-%d'))
    
    with open(current_log_file, mode='a+') as log_file:
      log_file.write(print_message + '\n')

def UpdateEquitiesData(expire_date : Optional[dt.datetime] = dt.datetime.now()) -> None:
  """
  This function will add any equity that is in the ListedEquities table and not in the Equities table
  to the Equities table and update any Equity who was last updated past the expire_date
  """
  time_ranges_to_update = ['1D', '1W', '1M', '3M', '1Y', '5Y', '10Y', 'Max']

  # --- SECTION: Add any new equities to Equities table ---
  ProgramStatusUpdate("Getting data for new companies...")

  all_equity_listings = security_db.GetSecurities(SecurityType.EquityListing)

  equities_with_data = [equity.Symbol for equity in security_db.GetSecurities(SecurityType.Equity)]
  equities_without_data = list(filter(lambda equity: equity.Symbol not in equities_with_data, all_equity_listings))

  start_time = dt.datetime.now()

  for (index, equity_listing) in enumerate(equities_without_data, start=0):
    ProgressBar(index + 1, len(equities_without_data), start_time, message=f'Processing {equity_listing.Symbol}')

    new_data = Equity.GetPercentChangeOverTimeRanges(equity_listing.Symbol, time_ranges_to_update)

    new_equity = Equity(equity_listing.Symbol, equity_listing.CompanyName, *new_data)
    security_db.AddNewSecurity(new_equity)
    security_db.Save()
  # --- END SECTION ----

  # --- SECTION: Update expired equities ---
  ProgramStatusUpdate("Updating old equities...")

  formatted_date = expire_date.strftime('%Y-%m-%d')
  equities_to_update = security_db.GetSecurities(SecurityType.Equity, [('LastUpdated', RelationalOperator.LessThan, formatted_date)])

  start_time = dt.datetime.now()

  for (index, equity) in enumerate(equities_to_update, start=0):
    ProgressBar(index + 1, len(equities_to_update), start_time, message=f'Processing {equity.Symbol}')

    new_data = Equity.GetPercentChangeOverTimeRanges(equity.Symbol, time_ranges_to_update)

    new_equity = Equity(equity.Symbol, equity.CompanyName, *new_data)
    security_db.ModifySecurities(new_equity, ('Symbol', RelationalOperator.EqualTo, new_equity.Symbol))
    security_db.Save()
  # --- END SECTION ---

def UpdateSingleEquity(symbol : str) -> None:
  """
  This function updates a single equity's data
  """
  
  time_ranges_to_update = ['1D', '1W', '1M', '3M', '1Y', '5Y', '10Y', 'Max']
  
  new_data = Equity.GetPercentChangeOverTimeRanges(symbol, time_ranges_to_update)
  old_equity_entry = security_db.GetSecurities(SecurityType.Equity, ('Symbol', RelationalOperator.EqualTo, symbol))
  new_equity = Equity(old_equity_entry.Symbol, old_equity_entry.CompanyName, *new_data)

  security_db.ModifySecurities(new_equity, ('Symbol', RelationalOperator.EqualTo, new_equity.Symbol))
  security_db.Save()

def UpdateOptionsData(expire_time : Optional[str] = '3m') -> None:
  #region Clear expired options
  ProgramStatusUpdate("Clearing expired options...")
  
  all_options = security_db.GetSecurities(SecurityType.Option)
  start_time = dt.datetime.now()
  for (index, option) in enumerate(all_options, start=0):
    ProgressBar(index + 1, len(all_options), start_time, message=f'Processing {option.CompanySymbol}')
    # expire date
    month, day, year = re.search(r'.+?\s(\w{3})\s(\d+)\s(\d+)', option.Description).groups()

    if dt.datetime.strptime(f'{month}-{day}-{year}', '%b-%d-%Y') < dt.datetime.now():
      security_db.DeleteSecurity(option)
      all_options.remove(option)
  #endregion

  #region Add any new option(s) to Options table
  ProgramStatusUpdate("Getting new options...")
  
  all_symbols = [equity_listing.Symbol for equity_listing in security_db.GetSecurities(SecurityType.EquityListing)]
  companies_with_data = set([option.CompanySymbol for option in all_options])
  companies_without_data = list(filter(lambda symbol: symbol not in companies_with_data, all_symbols))

  # Clear out lists no longer needed to save on memory
  all_options = []
  all_symbols = []
  companies_with_data = []

  start_time = dt.datetime.now()
  
  for (index, symbol) in enumerate(companies_without_data, start=0):
    ProgressBar(index + 1, len(companies_without_data), start_time, message=f"Getting options for {symbol}")
    new_options = Option.GetOptions(td_ameritrade_api_key, symbol, expire_time)

    for option in new_options:
      security_db.AddNewSecurity(option)
      security_db.Save()
  #endregion

def DisplayItems(items : List[Union[Equity, Option, EquityListing, Dict]]) -> None:
  """
  Takes list of a security and prints it 
  """

  if len(items) == 0: 
    return

  ProgramStatusUpdate('Generating table...')

  if isinstance(items[0], dict):
    table = tabulate.tabulate(items, headers='keys')
  else:
    table = tabulate.tabulate([security.__dict__ for security in items], headers='keys')
  
  print(f'\n{table}\n')

def __handle_init_command() -> None:
  """
  WARNING: Should only be called by CommandReader()\n
  This function will scrape for listed equities then add them to the database
  """
  

  equities = EquityListing.GetListedEquities(ProgramStatusUpdate, ProgressBar)

  for equity in equities:
    security_db.AddNewSecurity(equity)

  security_db.Save()

def __handle_update_command(arguments : iter) -> None:
  """
  WARNING: Should only be called by CommandReader()\n
  This function will update securities in the database according to the user's input
  """

  next_arg = next(arguments, None)
      
  if next_arg == None:
    # If no additional options or arguments, assume user wants everything updated
    UpdateEquitiesData()
    UpdateOptionsData()
  else:
    while next_arg != None:
      next_arg = next_arg.lower()

      if next_arg in ['-a', '-all']:
        UpdateEquitiesData()
        UpdateOptionsData()

      elif next_arg in ['-s', '-single']:
        equity_symbol = next(arguments)
        UpdateSingleEquity(equity_symbol)

      elif next_arg in ['-e', '-equities', '-equity']:
        UpdateEquitiesData()
      
      elif next_arg in ['-o', '-options', '-option']:
        UpdateOptionsData()
      
      next_arg = next(arguments, None)

def __handle_view_command(arguments : iter) -> None:
  """
  WARNING: Should only be called by CommandReader()\n
  This function will display data from the database according to the user's input
  """
  
  # retrieve_securities_orders acts as list of task lists. A object in this list
  # contains multiple entries of GetSecurities() call information. Another way to
  # about the structure of this object is like if you had a list of things to do 
  # one day, a different list of things to do the next day, and so on. At the moment
  # this actually over-complexifies this method, but it may be necessary with future updates.
  retrieve_securities_orders = []

  next_arg = next(arguments, None)
  if next_arg == None:
    # If no additional arguments given, assume user wants everything displayed with no ordering
    retrieve_securities_orders.append([
      (SecurityType.EquityListing, None, None),
      (SecurityType.Equity, None, None),
      (SecurityType.Option, None, None)
    ])

  else:
    while next_arg != None:
      next_arg = next_arg.lower()
      slice_match = re.match(r'^(-?\d+)?:(-?\d+)?(?::(-?\d+))?$', next_arg, flags=re.MULTILINE)
      
      # Handles case where user provides a slice notation for the securities to be displayed
      if slice_match != None:
        start, stop, step = slice_match.groups()

        start = int(start) if start else None
        stop  = int(stop)  if stop  else None
        step  = int(step)  if step  else 1
        
        slice_obj = slice(start, stop, step)

        if len(retrieve_securities_orders) == 0:
          retrieve_securities_orders.append([
            (SecurityType.EquityListing, slice_obj, None),
            (SecurityType.Equity, slice_obj, None),
            (SecurityType.Option, slice_obj, None)
          ])

        else:
          call_list = retrieve_securities_orders.pop()
          new_call_list = [(security_type, slice_obj, ordering) for (security_type,_,ordering) in call_list]
          
          retrieve_securities_orders.append(new_call_list)
      
      # Handles case when user wants to execute a SQL statement
      elif next_arg in ['-sql']:
        sql_statement_builder = ""
        current_section_of_statement = next(arguments)

        while current_section_of_statement != None:
          sql_statement_builder += current_section_of_statement + ' '
          current_section_of_statement = next(arguments, None)
        
        results = security_db.ExecuteSQLStatement(sql_statement_builder)

        DisplayItems(results)
          

      # Handles case where user provides a list of column names and ordering method for the securities to be displayed
      elif next_arg in ['s', 'sort_by', 'sort', 'order_by']:
        sorting_definitions = next(arguments)
        
        while not ")" in sorting_definitions:
          sorting_definitions += ' ' + next(arguments)

        ordering_groups = []
        ascending_aliases  = ['ascending', 'asc', 'a']
        descending_aliases = ['descending', 'desc', 'd']
        regex_pattern = r'(\w+)\s(' + "|".join(ascending_aliases + descending_aliases) + ')'

        for match in re.findall(regex_pattern, sorting_definitions):
          col_name, ordering_str = match

          if ordering_str.lower() in ascending_aliases:
            ordering_groups.append((col_name, Ordering.Ascending))
          elif ordering_str.lower() in descending_aliases:
            ordering_groups.append((col_name, Ordering.Descending))

        if len(retrieve_securities_orders) == 0:
          retrieve_securities_orders.append([
            (SecurityType.EquityListing, None, ordering_groups),
            (SecurityType.Equity, None, ordering_groups),
            (SecurityType.Option, None, ordering_groups)
          ])

        else:
          call_list = retrieve_securities_orders.pop()
          new_call_list = [(security_type, slicing, ordering_groups) for (security_type, slicing, _) in call_list]

          retrieve_securities_orders.append(new_call_list)

      # Handles case where user wants all tables in the database printed
      elif next_arg in ['-a', '-all']:
        retrieve_securities_orders.append([
          (SecurityType.EquityListing, None, None),
          (SecurityType.Equity, None, None),
          (SecurityType.Option, None, None)
        ])
      
      # Handles case where user wants all listed equities printed
      elif next_arg in ['-el', '-equitylistings', '-listedequities']:
        retrieve_securities_orders.append( [ (SecurityType.EquityListing, None, None) ] )

      # Handles case where user wants all equities printed
      elif next_arg in ['-e', '-equities', '-equity']:
        retrieve_securities_orders.append( [ (SecurityType.Equity, None, None) ] )

      # Handles case where user wants all options printed
      elif next_arg in ['-o', '-options', '-option']:
        retrieve_securities_orders.append( [ (SecurityType.Option, None, None) ] )

      next_arg = next(arguments, None)


  # Now that the retrieve_securities_orders object has been fully constructed
  # we go through each GetSecurities() order in each chunk.
  for call_chunk in retrieve_securities_orders:
    for (security_type, sel_slice, ordering) in call_chunk:
      if sel_slice != None:
        if ordering != None:
          primary_col_name,_ = ordering[0]
          condition = [(primary_col_name, RelationalOperator.NotEqualTo, 'N/A')]
          securities = security_db.GetSecurities(security_type, conditions=condition, order_by_cols=ordering)[sel_slice]
        else:
          securities = security_db.GetSecurities(security_type, order_by_cols=ordering)[sel_slice]
      else:
        securities = security_db.GetSecurities(security_type, order_by_cols=ordering)

      DisplayItems(securities)

def __handle_backtest_command(arguments : iter) -> None:
  """
  WARNING: Should only be called by CommandReader()\n
  This function will backtest a symbol using the DCA strategy with the parameters provided
  by the user
  """

  symbol = next(arguments, None)

  if symbol == None:
    ProgramStatusUpdate("Please enter a trading symbol to backtest. For help, use command 'help' or 'h'")
    return
  else:
    time_range = next(arguments, None)

    if time_range == None:
      # User only entered a symbol
      backtest_data = Equity.BacktestDollarCostAveraging(symbol, 'Max', 1000, 1000, 30)
    else:
      principal = next(arguments, None)

      if principal == None:
        # User only entered a symbol and a start time
        backtest_data = Equity.BacktestDollarCostAveraging(symbol, time_range, 1000, 1000, 30)
      else:
        periodic_investment = next(arguments, None)

        if periodic_investment == None:
          # User only entered a symbol, start time, and principal investment
          backtest_data = Equity.BacktestDollarCostAveraging(symbol, time_range, float(principal), 1000, 30)
        else:
          period_time = next(arguments, None)

          if period_time == None:
            # User entered everything except the period between each periodic investment
            backtest_data = Equity.BacktestDollarCostAveraging(symbol, time_range, float(principal), float(periodic_investment), 30)
          else:
            # User entered every parameter
            backtest_data = Equity.BacktestDollarCostAveraging(symbol, time_range, float(principal), float(periodic_investment), float(period_time))

  table = tabulate.tabulate(backtest_data.items(), headers='keys')
  print(f'\n{table}\n')

def __handle_help_command():
  """
  WARNING: Should only be called by CommandReader()\n
  This function will display the help file to the user
  """
  with open(CURRENT_DIRECTORY + HELP_FILE_PATH, mode='r') as help_file:
    print(help_file.read())

def CommandReader():
  user_input = input('> ')

  while user_input not in ['quit', 'q']:
    arguments = iter(user_input.split(' '))
    first_arg = next(arguments).lower()

    if first_arg in ['init', 'initialize', 'i']:
      __handle_init_command()

    elif first_arg in ['update', 'u']:
      __handle_update_command(arguments)

    elif first_arg in ['view', 'v']:
      __handle_view_command(arguments)

    elif first_arg in ['backtest', 'bt']:
      __handle_backtest_command(arguments)

    elif first_arg in ['help', 'h']:
      __handle_help_command()

    user_input = input('> ')

def main():
  global security_db
  global td_ameritrade_api_key

  locale.setlocale(locale.LC_ALL, '')
  security_db = SecurityDatabaseWrapper(CURRENT_DIRECTORY + DATABASE_FILE_PATH)

  with open(CURRENT_DIRECTORY + API_FILE_PATH, mode='r') as api_file:
    key_match = re.search('^td_ameritrade\=(.+)$', api_file.read(), flags=re.MULTILINE)

    if type(key_match) == None:
      raise KeyError("Could not locate TD Ameritrade API key")
      
    td_ameritrade_api_key = key_match.group(1)

  CommandReader()
  security_db.CloseConnection()

# TODO: Implement AlphaVantage intraday trading history

if __name__ == "__main__":
  try:
    main()
  except KeyboardInterrupt:
    security_db.CloseConnection()