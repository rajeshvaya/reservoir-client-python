# from Reservoir import ReservoirClient
import sys
import os
here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(here, '../src/')))

from Reservoir import ReservoirClient
if __name__ == '__main__':
	r = ReservoirClient()
	print r.get("pk_movie")

