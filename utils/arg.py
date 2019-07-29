from idxpckl import IndexedPickle
import numpy as np 

class Arg():
''' 
	class Arg: Wrapper class to contain arguments fed to a job object for subsequent execution by
	compatible script. Support compatibility with multiple file formats used to save the args to 
	disk. Currently only the IndexedPickle object is supported

	name: str
		Name of the argument object

	argdir : argdir
		Directory in which the object can safely store any companion files that arise

'''

	def __init__(self, name, argdir):

		self.name = name
		self.argdir = argdir

	def set_args(self, args, meta=None):
		'''
		function set_args: Assign arguments to this object

		args : list of dicts
			List of arguments to be fed into the job as dicts

		meta : dict
			Additional metadata separate from the list of dicts. This
			meta will be saved as the header of the indexed pickle
		'''


		self.args = args
		self.meta = meta

	def save(self, file_path = None):
		'''
		function save: Save the arguments away as an indexed pickle

		file_path : str
			If provided, location where the arg file will be stored. Else, the argdir
			attribute will be used, in conjunction with the name attribute. Must include
			the file extension 
		'''

		if file_path is None:
			file_path = '%s/%s.dat' % (self.argdir, self.name)

		ip = IndexedPickle(file_path)
		if self.meta is None:
			meta = {}
		else:
			meta = self.meta

		ip.init_save(len(self.args), meta)
		for arg in self.args:
			ip.save(arg)
		ip.close_save()

	@classmethod
	def init_from_file(self, file_path, load_args = False):
		'''
		function load : Populate from the args from an indexed pickle file

		file_path : str
			Path of the file. Must include file path extension
		load_args : bool
			Should we load the actual arguments from file? 
		'''

		argdir, _, name = file_path.rpartition('/')

		self.argdir = argdir
		# Take off the file extension
		self.name = name.split('.')[0]


		ip = IndexedPickle(file_path)
		ip.init_read()
		self.meta = ip.header
		ip.close_read()

		if load_args:
			self.load_args_from_file(file_path)

	def load_args_from_file(self, file_path=None):
	'''
		function load_args : Actually load the arguments into memory (potentially memory intensive)
	'''
		if file_path is None:
			file_path = '%s/%s.dat' % (self.argdir, self.name)

		ip = IndexedPickle(file_path)
		ip.init_read()
		args = []
		for idx in np.arange(ip.nobj):
			args.append(idx.read(idx))

		self.args = args

	def split(self, nsplits, file_path = None):
		'''
		function split : Take a given arg file, split its arguments into nsplits, and save into separate
		files. If file_path is provided, we append _split%d % i to the provided path. Otherwise, we take 
		the existing name and dir_path and append _split%d % i

		n_splits : int

		file_path : str
			Omit file path extension

		Returns a list of the created arg objects
		'''

		split_args = np.array_split(self.args, nsplits)

		if file_path is None:
			file_path = '%s/%s' % (self.argdir, self.name)
			arg_dir = self.arg_dir
			name = self.name
		else:
			arg_dir, _, name = file_path.rpartition('/')

		split_arg_names = ['%s_split%d' % (name, i) for i in range(nsplits)]
		split_file_paths = ['%s/%s_split%d.dat' % (argdir, name, i) for i in range(nsplits)]


		split_args = []
		# Write to file
		for sa in split_args:
			a = Arg(name = name, argdir = argdir)
			# Copy meta
			a.set_args(sa, meta = self.meta)
			a.save(file_path = file_path)
			split_args.append(a)

		return split_args
