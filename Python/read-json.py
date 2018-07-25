import json

def load_stations(path):
	with open(path) as f:
		bike_data = json.loads(f.read())
		return bike_data['network']['stations']

def write_stations(path, stations):
	with open(path, 'w') as f:
		for s in stations:
			f.write(json.dumps(s) + "\n")

def main():
	stations = load_stations('../data/api.citybik.es.v2.networks.citycycle.json')
	write_stations('stations.json', stations)

if __name__ = "__main__":
	main()