# Equities Market Analzyer

This project was designed to provide insight into US equity markets and to test out trading strategies.
It will first scrape data from the internet to get the names and symbols of US listed equities.
Then, it will retrieve data on the equities performance over different time periods so that the user may see which ones are top performers or poor performers.
Next, it will get all available options for every equity and calculate the Black-Scholes value for them so the user can judge which options are most valuable.
Additionally, the user may choose to backtest equities using a modified DCA strategy that will show how it would perform over
a user defined period of time and assign a rating to that investment.

## Dependencies

- Must have Python 3.6+ installed
- This program requires tabulate, pandas, scipy, and requests to be installed which can be done with the following command:

  `pip install tabulate pandas scipy requests`

- Requires API keys to be in the file `assets/api_keys.txt` in the following format: `<api name>=<api key>`. The required APIs are:
  - td_ameritrade

## How To Use

1. Clone this repository
2. Run analyze.py
3. Use command `help` in the program's console to see available commands

## Notes

- Please let me know of any bugs, feature requests, etc.

## TODO

- Implement AlphaVantage intraday trading history for new DCA trading strategies
- Work on ML-assisted trading
