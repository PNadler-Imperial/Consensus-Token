import pandas as pd
import csv
import matplotlib.pyplot as plt

def parse_coinmarketcap():
	df = pd.read_excel("coinmarketcap_historical_dates.xlsx", header = None)
	for i in df.loc[:, 3]:
		i = str(i)
		print "parsing historical data on %s..." % i  
	
		tables = pd.read_html("https://coinmarketcap.com/historical/" + i)
		tables[0].to_csv("./coinmarketcap_historical_snapshots/" + i + ".csv")
	return

def aggregate_data():
	aggregate_df = pd.DataFrame()
	df = pd.read_excel("coinmarketcap_historical_dates.xlsx", header = None)
	
	for i in df.loc[:, 3]:
		i = str(i)
		data_df = pd.read_csv("./coinmarketcap_historical_snapshots/" + i + ".csv")
		data_df = data_df.drop(data_df.columns[[0]], axis = 1)
		data_df['Date'] = i
		data_df = data_df[data_df['Market Cap'] != "?"]
		aggregate_df = aggregate_df.append(data_df)
		
	aggregate_df.columns = [c.replace(' ', '_') for c in aggregate_df.columns]
	aggregate_df['Circulating_Supply'] = map(lambda x: str(x).replace(" *",""), aggregate_df['Circulating_Supply'])
	aggregate_df['Name'] = map(lambda x: str(x).replace("  "," "), aggregate_df['Name'])

	aggregate_df.to_csv("./aggregated_marketcap_till_" + i + ".csv", index = False)

def get_coin_list():
	i = "20180211"
	aggregate_df = pd.read_csv("./aggregated_marketcap_till_" + i + ".csv")
	#aggregate_df.Name.value_counts().to_csv('./coin_counts.csv')
	
	df = aggregate_df.loc[aggregate_df["Date"] == 20180211]
	
	pd.options.mode.chained_assignment = None
	df['Market_Cap'] = map(lambda x: x.replace("$","").replace(",", ""), df['Market_Cap'])
	df['Market_Cap'] = df['Market_Cap'].apply(pd.to_numeric, errors = 'coerce')

	marketcap_total = df['Market_Cap'].sum()

	df['Market_Cap_CumSum'] = df['Market_Cap'].cumsum()

	fig = df.plot(x = '#', y = 'Market_Cap_CumSum', color='b')
	ax.set_yscale('log')
	#fig.axhline(y = marketcap_total * 0.9, color = 'r', linestyle = '--', lw = 2)
	#newfig = fig.get_figure()
	#newfig.savefig('output.png')
	
	
def main():
	#parse_coinmarketcap()
	#aggregate_data()
	get_coin_list()

if __name__ == "__main__":
    main()