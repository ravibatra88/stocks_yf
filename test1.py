import requests

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = 'https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=“NSE:SBIN”&apikey=DNPJBF53MKUNI7O3'
r = requests.get(url)
data = r.json()

print(data)