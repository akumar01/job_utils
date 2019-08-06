import os
import shutil
import numpy as np
import h5py_wrapper
import pickle
from glob import glob
from copy import deepcopy
from mpi_utils.ndarray import Gatherv_rows


# Class to handle atomic level results containers 
class ResultsManager():

	def __init__(self, total_tasks, directory): 

		self.total_tasks = total_tasks
		self.directory = directory 

		# Initialize directory structure
		if not os.path.exists(directory):
			os.makedirs(directory)

		# save the total tasks away 
		with open('%s/total_tasks', 'wb' % directory) as f:
			pickle.dumps(total_tasks, f)

		# Initialize container for children results
		self.children = []

	@classmethod 
	def restore_from_directory(self, directory):
		'''
			function restore_from_directory : grab the inserted indexes and total_tasks
			from a directory
		'''
		self.directory = directory

		try:
			f = open('%s/total_tasks' % directory, 'rb')
		else:
			raise FileNotFoundError('Could not find record of total_tasks in provided directory')

		self.total_tasks = pickle.load(f)

		# Grab all child files
		children = glob('%s/child*' % directory)
		children_idxs = [int(os.path.splittext(child)[0].split('_')[1]) for child in children]

		self.children = [{'path' : children[i], 'idx' : children_idxs[i] for i in range(len(children))}]

	def add_child(self, data, idx, path=None): 
		'''
			function add_child: Insert the data present in the provided file

			data : dict 
				data dictionary that should be of the same format as that initialized by
				init_structure

			path : str
				path to file from which data was loaded. If None, nothing will be done 
				when it comes time to kill children

			idx : location in the master list to insert the results
		'''

		if path is None:
			path = '%s/child_%s.h5' % (self.directory, idx)

		self.children.append({'path' : path, 'idx' ; idx})

        h5py_wrapper.save(path, data, write_mode = 'w') 

	def gather_managers(self, comm, root=0):
		'''
			function gather_managers: Given a comm object, gather attributes
			from to the root node

			comm: MPI communicator object 

			root: int
				rank of root node

		'''

		# Use mpi4py interface (might be slow for many children)
		children = comm.gather(self.children, root=root)
		self.children = children

	def concatenate(self):

		if len(self.children > 0):

			# Sequentially open up children and insert their results into a master
			# dictionary
			dummy_dict = h5py_wrapper.load(children[0]['path'])
			master_dict = init_structure(self.total_tasks, dummy_dict)

			for i, child in enumerate(children):
				child_data = h5py_wrapper.load(child['path'])
				master_dict = insert_data(master_dict, child_data, child['idx'])

			master_data_filepath = os.path.abspath(self.directory, '..', '%.dat' % self.directory)
			h5py_wrapper.save(master_data_filepath, master_dict, write_mode = 'w')			

	def cleanup(self):

		# Delete self.directory and all of its contents: 
		shutil.rmtree(self.directory)

# Find all array-type fields of the dictionary dict_ and format them as zero-valued arrays 
# with an additional dimension of size n
def format_and_expand_dict(dict_, n):

	for key, value in dict_.items():
		if type(value) == dict:
			dict_[key] = format_and_expand_dict(value, n)
		elif type(value) = np.ndarray:
			dict_[key] = np.zeros((n,) + value.shape)

	return dict_


def init_structure(total_tasks, dummy_dict):
	'''

	function init_structure : Replicate the structure of dummy_dict, 
	but add an additional axis with size total_tasks

	'''

	master_dict = dummy_dict.deepcopy()
	master_dict = format_and_expand_dict(master_dict, total_tasks)

	return master_dict

# recursively traverse the dictionaries until the ndarray fields are found, and then 
# inser the child dictionary's data into the mater_dict in the appropriate location 
def insert_data(master_dict, child_dict, idx):

	for key, value in master_dict.items():
		if type(value) == dict:
			value = format_and_expand_dict(value, child_dict[key], idx)
		elif type(value) == np.ndarray:
			value[idx, ...] = child_dict[key]

	return master_dict
