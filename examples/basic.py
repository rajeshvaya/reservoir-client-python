# from Reservoir import ReservoirClient
import sys
import os
here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(here, '../src/')))

from Reservoir import ReservoirClient
if __name__ == '__main__':
	r = ReservoirClient()
	# simple get
	print r.get("pk_movie")

	# simple set 
	print r.set("pk_movie", "awesome", 0)
	print r.get("pk_movie")

	# batch set
	print r.set_batch([
		{ "key":"pk_actor", "value":"aamir khan", "expiry":"0" },
		{ "key":"pk_director", "value":"raj kumar hirani", "expiry":"0" },
		{ "key":"pk_to_delete", "value":"some crap", "expiry":"0" }
	])

	# incrementer and decrementer
	print r.icr("pk_movie_votes");
	print r.icr("pk_movie_votes");
	print r.get("pk_movie_votes");

	print r.dcr("pk_movie_votes");
	print r.get("pk_movie_votes");

	# simple delete
	print r.get("pk_to_delete")
	print r.delete("pk_to_delete")
	print r.get("pk_to_delete")

	# simple dependants
	print r.set("coldplay", "Best band ever", 0)
	print r.set_dependant_batch([
		{ "key":"coldplay_singer", "value":"chris martin", "expiry":"0" , "parent_key": "coldplay"},
		{ "key":"coldplay_guitar", "value":"johny buckland", "expiry":"0" , "parent_key": "coldplay"},
		{ "key":"coldplay_drums", "value":"will champion", "expiry":"0" , "parent_key": "coldplay"},
		{ "key":"coldplay_base", "value":"guy berrymen", "expiry":"0" , "parent_key": "coldplay"}

	])
	print r.get_batch(['coldplay_singer', 'coldplay_guitar'])
	print r.delete("coldplay") # this will delete the dependants too!
	print r.get_batch(['coldplay_singer', 'coldplay_guitar'])





