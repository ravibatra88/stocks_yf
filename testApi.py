import requests

url = "https://indian-stock-info.p.rapidapi.com/api/v1/getCashFlowData/ASHOKLEY.NS"

headers = {
	"x-rapidapi-key": "67fc7bf220mshde86cfc84d42f64p1a19d8jsn1c67a085c8b4",
	"x-rapidapi-host": "indian-stock-info.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

print(response.json())