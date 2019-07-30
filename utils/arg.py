from idxpckl import IndexedPickle
import numpy as np 

class Param():
''' 
	class Param: Wrapper class to contain arguments fed to a job object for subsequent execution by
	compatible script. Support compatibility with multiple file formats used to save the args to 
	disk. Currently only the IndexedPickle object is supported

	name: str
		Name of the argument object

	paramdir : str
		Directory in which the object can safely store any companion files that arise

'''

	def __init__(self, name, paramdir):

		self.name = name
		self.paramdir = paramdir

	def set_params(self, params, meta=None):
		'''
		function set_args: Assign arguments to this object

		params : list of dicts
			List of arguments to be fed into the job as dicts

		meta : dict
			Additional metadata separate from the list of dicts. This
			meta will be saved as the header of the indexed pickle
		'''


		self.params = params
		self.meta = meta

	def save(self, file_path = None):
		'''
		function save: Save the params away as an indexed pickle

		file_path : str
			If provided, location where the param file will be stored. Else, the paramdir
			attribute will be used, in conjunction with the name attribute. Must include
			the file extension 
		'''

		if file_path is None:
			file_path = '%s/%s.dat' % (self.paramdir, self.name)

		ip = IndexedPickle(file_path)
		if self.meta is None:
			meta = {}
		else:
			meta = self.meta

		ip.init_save(len(self.params), meta)
		for param in self.params:
			ip.save(param)
		ip.close_save()

		return file_path

	@classmethod
	def init_from_file(self, file_path, load_params = False):
		'''
		function load : Populate from an indexed pickle file

		file_path : str
			Path of the file. Must include file path extension
		load_params : bool
			Should we load the actual params from file? 
		'''

		paramdir, _, name = file_path.rpartition('/')

		self.paramdir = paramdir
		# Take off the file extension
		self.name = name.split('.')[0]

		ip = IndexedPickle(file_path)
		ip.init_read()
		self.meta = ip.header
		ip.close_read()

		if load_params:
			self.load_params_from_file(file_path)

	def load_params_from_file(self, file_path=None):
	'''
		function load_args : Actually load the arguments into memory (potentially memory intensive)
	'''
		if file_path is None:
			file_path = '%s/%s.dat' % (self.paramdir, self.name)

		ip = IndexedPickle(file_path)
		ip.init_read()
		params = []
		for idx in np.arange(ip.nobj):
			params.append(idx.read(idx))

		self.params = params

	def split(self, nsplits, file_path = None):
		'''
		function split : Take a given param file, split its arguments into nsplits, and save into separate
		files. If file_path is provided, we append _split%d % i to the provided path. Otherwise, we take 
		the existing name and dir_path and append _split%d % i

		n_splits : int

		file_path : str
			Omit file path extension

		Returns a list of the created arg objects
		'''

		split_params = np.array_split(self.params, nsplits)

		if file_path is None:
			file_path = '%s/%s' % (self.paramdir, self.name)
			param_dir = self.param_dir
			name = self.name
		else:
			param_dir, _, name = file_path.rpartition('/')

		split_param_names = ['%s_split%d' % (name, i) for i in range(nsplits)]
		split_file_paths = ['%s/%s_split%d.dat' % (paramdir, name, i) for i in range(nsplits)]

		split_params = []
		# Write to file
		for sp in split_params:
			p = Param(name = name, paramdir = paramdir)
			# Copy meta
			p.set_params(sp, meta = self.meta)
			p.save(file_path = file_path)
			split_params.append(p)

		return split_params
