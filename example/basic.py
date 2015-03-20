# from Reservoir import ReservoirClient
import sys
import os
here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(here, '../src/')))

from Reservoir import ReservoirClient
if __name__ == '__main__':
	r = ReservoirClient()
	print r.get("pk_movie")

	print r.set("pk_movie", "awesome", 0)
	print r.get("pk_movie")

	print r.set_batch([
		{ "key":"pk_actor", "value":"aamir khan", "expiry":"0" },
		{ "key":"pk_director", "value":"raj kumar hirani", "expiry":"0" },
		{ "key":"pk_to_delete", "value":"some crap", "expiry":"0" }
	])

	print r.icr("pk_movie_votes");
	print r.icr("pk_movie_votes");
	print r.get("pk_movie_votes");

	print r.dcr("pk_movie_votes");
	print r.get("pk_movie_votes");

	print r.get("pk_to_delete")
	
	print r.delete("pk_to_delete")
	print r.get("pk_to_delete")



