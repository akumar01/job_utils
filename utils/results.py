import os
import numpy as np
import h5py_wrapper
from copy import deepcopy
from idxpckl import IndexedPickle

# Class to handle atomic level results containers 
class ResultsManager():

	def __init__(self, total_tasks, directory): 

		self.total_tasks = total_tasks
		self.directory = directory 

		# Initialize directory
		if not os.path.exists(directory):
			os.makedirs(directory)

		# Initialize container for children results
		self.inserted_idxs = []
		self.children = []

	def init_structure(self, dummy_dict):
		'''
			function init_structure : Replicate the structure of dummy_dict, 
			but add an additional axis with size total_tasks
		'''

		self.results = dummy_dict.deepcopy()
		self.results = format_and_expand_dict(self.results, self.total_tasks)

	@classmethod 
	def restore_from_file(self, file_path):
		'''
			function restore_from_file : fill in the results from file, as opposed
			to using __init__ -> init_structure
		'''
		ip = IndexedPickle(file_path)
		ip.init_read()
		self.inserted_idxs = ip.read(0)
		self.results = ip.read(1)

		self.total_tasks = ip.header['total_tasks']
		self.directory = ip.header['directory']

		ip.close_read()

	def save(self):
		'''
			function save : Use the IndexedPickle object to 
			save away the results (total_tasks, inserted_idxs)
		'''

		ip = IndexedPickle('%s/manager.dat' % self.directory)
		metadata = {'total_tasks' : self.total_tasks, 'directory' : self.directory}
		ip.init_save(2, metadata)
		ip.save(self.inserted_idxs)
		ip.save(self.results)
		ip.close_save()

	def add_child(self, data, idx, path = None): 
		'''
			function add_child : Insert the data present in the provided file and 
			add the child to the list of currently processed children files
			
			data : dict 
				data dictionary that should be of the same format as that initialized by
				init_structure

			path : str
				path to file from which data was loaded. If None, nothing will be done 
				when it comes time to kill children

			idx : location in the master list to insert the results
		'''

		self.inserted_idxs.append(idx)
		self.children.append(path)
		self.results = self.insert_data(self.results, data, idx)


	def kill_children(self): 
		'''
			function kill_children: Delete all the files contained in self.children
		'''

		for i in range(len(self.children)):
			orphan = self.children.pop(i)
			if orphan is not None:
				os.remove(orphan)


# Find all array-type fields of the dictionary dict_ and format them as zero-valued arrays 
# with an additional dimension of size n
def format_and_expand_dict(dict_, n):

	for key, value in dict_.items():
		if type(value) == dict:
			dict_[key] = format_and_expand_dict(value, n)
		elif type(value) = np.ndarray:
			dict_[key] = np.zeros((n,) + value.shape)

	return dict_

# recursively traverse the dictionaries until the ndarray fields are found, and then 
# inser the child dictionary's data into the mater_dict in the appropriate location 
def insert_data(master_dict, child_dict, idx):

	for key, value in master_dict.items():
		if type(value) == dict:
			value = format_and_expand_dict(value, child_dict[key], idx)
		elif type(value) == np.ndarray:
			value[idx, ...] = child_dict[key]

	return master_dict
