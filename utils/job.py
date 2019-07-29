import os, shlex, subprocess
from utils import sbatch_line
from arg import Arg
from idxpckl import Indexed_Pickle 
# Scripts to create job structures
# Validator objects that test whether a job is setup correctly

# Consider a 'Result' type or some indication that is saved with the job of 
# tracking of partial progress, allowing one to resume without incident


class JobArray():
'''
	Class Job Array: Container class for managing an array of jobs
	
	--> Use this class to 
	(1) create the job directory structure
	(2) create all arg, sbatch, job files as needed
	(3) save log
	(4) query various job attributes (e.g. finished)
	(5) Edit job attributes easily 
	(6) Selectively submit/resubmit/resume/cancel jobs

	--> Take as inputs:
	(1) minimum information to feed into a function handle that *yields* the arg data to be saved
	(2) job kwargs such as metadata, startup_cmds, sbatch_params, etc...  
	(3) Validator objects : Any validation that the args/jobs should go through prior to submission
'''

	def __init__(self, rootdir, desc = None):

		self.rootdir = rootdir
		self.arg_generator = arg_generator
		if desc is None:
			desc = 'No description provided'
		self.desc = desc


	def create_job_structure(self, arg_generator, validate=True, validator_objects = [],  
							 **arg_generator_kwargs, **job_kwargs):
		''' 
			function create_job_structure : Create the directory structure in rootdir, generate args from arg_generator, 
			create and save job/sbatch files, assess validation, and save away a log

			defer until you can figure out how to generate the arguments sensibly

		'''


	# Needed methods!
	def submit_jobs(self):

	def query_jobs(self):

	def query_args(self):

	def cancel_jobs(self):

	def resubmit_jobs(self):	

	def edit_job_attribute(self):

	def validate(self):

	def refresh_log(self):

	def save_log(self):

	# Return jobs matching some filter
	def grab_jobs(self):

	# Is this possible?
	def edit_arg(self): 


class Job():
'''
	Class Job: Base class that keeps track of all properties need to create/submit/and re-submit jobs

	script : str
		Actual script to be executed. The script should accept arg_file as a command line argument, if 
		arg_file is provided

	name : str 
		Jobname. Passed onto other contexts in which the job needs to be indentified with a name, e.g.
		SLURM queues

	jobdir : str 
		Path where job can appropriately save any companion files generated

	arg_file : str or Arg object
		Either the Path to where an Args class object can be initialized from or the desired Arg 
		object itself

	data_file : str
		Path to where the data will be saved. The actual saving of the data should be handled 

	script args : list
		Additional arguments to be passed onto the script
	
	metadata : A dictionary of additional metadata that is stored away with the job

'''

	def __init__(self, name, script, jobdir, 
				 arg_=None, data_file=None, 
				 script_args = [], metadata=None):


		self.script = script
		self.name = name
		self.jobdir = jobdir

		# Assign an Arg object to the job object
		if arg_ is not None:
			if type(arg_) == Arg:
				self.arg = arg_
			else:
				a = Arg.init_from_file(arg_)
				self.arg = a

		self.data_file = data_file
		self.script_args = script_args
		self.metadata = metadata

		# finished: did this job finish running?
		# submitted: was this job submitted?

		self.finished = False
		self.submitted = False

	# Create the sbatch file used to submit
	def gen_submit_script(self):
		pass

	# Submit the job
	def submit(self):
		self.submitted = True

	# What to do when a job is finished?
	def finish(self):
		self.finished = True

	# How to resume a job?
	def resume(self)


class NERSCJob(Job):
'''
		Class NERSCJob: Job class adapted for operating on NERSC. Supports operation within any qos and both
		Haswell and KNL architectures
'''

	def __init__(self, name, script, jobdir, arg_=None, data_file=None, script_args = [], metadata=None): 

		if data_file is None:
			data_file = '%s/%s_results.dat' % (jobdir, self.name)			

		super(NERSC_job, self).__init__(name, script, jobdir, arg_, data_file, 
						   	 			script_args, metadata)

		# Store all sbatch params in a dict for easy reference later on
		self.sbatch_params = {}

	@classmethod
	def query_job_attribute(self, file_path):
		'''
		function query job_attribute : Load only a specific attribute saved to a file for a job. 
		'''
		pass

	@classmethod
	def init_from_file(self, file_path):
		'''
		function load : Reconstruct a job file from a file output by Job.save

		file_path : str
			Path of the file. Must include file path extension
		'''

		ip = Indexed_Pickle(file_path)

		ip.init_read()
		self.metadata = ip.header

		# Consult the save function for the ordering of these
		self.name = ip.read(0)
		self.script = ip.read(1)
		self.jobdir = ip.read(2)
		arg_file_path = ip.read(3)
		self.data_file = ip.read(4)
		self.script_args = ip.read(5)
		self.sbatch_params = ip.read(6)
		self.startump_cmds = ip.read(7)
		status_dict = ip.read(8)
		ip.close_read()

		self.submitted = status_dict['submitted']
		self.finished = status_dict['finished']
		self.slurm_ID = status_dict['slurm_ID']

		# Initialize an Arg object from arg_file_path, but do not load the arguments off of disk
		a = Arg.init_from_file(arg_file_path)
		self.arg = a


	def save(self, file_path = None):
	'''
		function save: Commit to disk all necessary information needed to recreate this job object from file
	'''

		if file_path is None:
			file_path = '%s/%s.job' % (self.jobdir, self.name)

		ip = IndexedPickle(file_path)
		if self.metadata is None:
			meta = {}
		else:
			meta = self.metadata

		# Job attributes are saved in the following order:
		# (0) name
		# (1) script
		# (2) jobdir
		# (3) arg_ (this is saved as the path only, and not a reference to the arg file)
		# (4) data_file 
		# (5) script_args
		# (6) sbatch_params
		# (7) startup_cmds
		# (8) status dict

		ip.init_save(9, meta)
		ip.save(self.name)
		ip.save(self.script)
		ip.save(self.jobdir)
		arg_filepath = '%s/%s.dat' % (self.arg_.argdir, self.arg_.name)
		ip.save(arg_file_path)
		ip.save(self.data_file)
		ip.save(self.script_args)
		ip.save(self.sbatch_params)
		if not hasattr(self, 'startup_cmds'):
			startup_cmds = None
		ip.save(self.startup_cmds)

		status_dict = self.get_status()
		ip.save(status_dict)
		ip.close_save()

	def get_status(self):

		status_dict = {'submitted' : self.submitted,
					   'finished' : self.finished,
					   'slurm_ID' : None}

		if hasattr(self, 'slurm_ID'):
			status_dict['slurm_ID'] = self.slurm_ID

		return status_dict

	def init_sbatch_params(self, architecture, qos, 
					 	   nodes=1, ntasks=1,
						   cpu_per_task=1,
						   time='12:00:00', name = None,
						   outfile=None, errfile=None,
						   email='ankit_kumar@berkeley.edu',
						   mail_type='FAIL',
						   startup_cmds = None):
	'''
		method init_sbatch_params: Assign all properties needed to run the job on NERSC 
		
		architecture : string
			'knl' or 'haswell'. The NERSC architecture to run the job on

		qos : string 
			Quality of service to run the job on the specified architecture (e.g. shared, regular, low)

		nodes : int
			Number of nodes to reserve for the job

		ntasks : int
			Number of MPI tasks to distribute across

		cpu_per_task : int
			Number of CPU to allocate per MPI task. Check the NERSC documentation. 2 CPUs correspond to 1 physical 
			core. Recommended keep at least 2 CPU per task

		time : str
			Time to reserve for the job in format 'HH:MM:SS'

		name : str 
			Name for the job on SLURM. Defaults to the job's name

		outfile : str
			Path of file to write stdout to. If left as None, will generate a companion file using the job's name
			and directory

		errfile : str
			Path of file to write stderr to. If left as None, will generate a companion file using the job's name
			and directory

		email : str
			E-mail address to send job updates to.

		mail_type : str
			Check NERSC documentation. Sets the type of event that prompts an e-mail

		startup_cmds : list of str
			A list of any further commands that should be executed prior to running the srun command. By default 
			we activate our conda environment

	'''

		# Store value in a 2-element list, where the first element corresponds to the flag string recognized 
		# by slurm
		self.sbatch_params['architecture'] = ['--constraint', architecture]
		self.sbatch_params['qos'] = ['--qos', qos]
		self.sbatch_params['nodes'] = ['-N', nodes]
		self.sbatch_params['ntasks'] = ['-n', ntasks]
		self.sbatch_params['cpu_per_task'] = ['-c', cpu_per_task]
		self.sbatch_params['time'] = ['-t', time]

		is name is None:
			name = self.name
		self.sbatch_params['name'] = ['--job-name', name]

		if outfile is None:
			outfile = '%s/%s.o' % (self.jobdir, self.name)
		self.sbatch_params['outfile'] = ['--out', outfile]
		if errfile is None:
			errfile = '%s/%s.e' % (self.jobdir, self.name)
		self.sbatch_params['errfile'] = ['--error', errfile]

		self.sbatch_params['email'] = ['--mail-user', mail_user]

		self.sbatch_params['mail_type'] = ['--mail_type', mail_type]

		self.startup_cmds = startup_cmds

	def edit_sbatch_params(self, key, value):
	'''
		function edit sbatch_params: set a specific sbatch param 

		key : str
			sbatch_params dictionary key to be edited

		value : str
			value to set. The actual SBATCH flags are externally immutable
	'''

		self.sbatch_params[key][1] = value

	def srun_statement(self):
	'''
		Output srun statement. The default form of this statement
		is srun python -u self.script self.arg_file self.data_file self.script_args
	'''
		srun = 'srun python -u %s %s %s' % (self.script, self.arg_file,
											self.data_file)

		for arg in self.script_args:
			srun += ' %s' arg

		return srun

	def gen_sbatch_file(self, path=None):
	'''
		function generate_sbatch_file : Given the job's current sbatch_params, generate an sbatch file

		path : str
			Save path of the sbatch file. By default will generate one in the jobdir

	'''
		if path is None:
			path = '%s/%s_submit.sh' % (jobdir, self.name)

		with open(path, 'w') as sb:
            sb.write('#!/bin/bash\n')
			
			for key, value in self.sbatch_params.items():
				sb.write(sbatch_line(key, value))

			sb.write('\n')

			# By default, activate our anaconda environment
            sb.write('source ~/anaconda3/bin/activate\n')
            sb.write('source activate nse\n')

            # These settings give good MPI performance (haven't tested
            # whether this affects job running on the shared queue)
            sb.write('export OMP_NUM_THREADS=1\n')
            sb.write('export KMP_AFFINITY=disabled\n')

            # srun statement
            sb.write(self.srun_statement())

       	self.sbatch_path = path

   	def submit(self):
   	'''
		function submit : Submit the job to the slurm queue and return the JobID
		assigned to the job by SLURM
   	'''

   		os.system('chmod u+x %s' % self.sbatch_path)
   		stdout = subprocess.check_output('sbatch %s' % self.sbatch_path, shell = True)

   		# Grab the jobid
   		jobid = [int(s) for s in stdout.split() if s.isdigit()][0] 	

   		self.slurm_ID = jobid

   		super(NERSCJob, self).submit()

   		return jobid

   	def split(self, nsplits, split_arg = True, file_path = None, 
   			  gen_sbatch = False, arg_file_path=None):
   	'''
		function split : Split the job into nsplits in case our original setup cannot fit within 
		runtime limits on NERSC. First splits the corresponding argile and then assigns each split
		job the new argfile. Similar behavior w.r.t file_path as Arg.split

		nsplits : int

		split_arg : bool
			Should we also invoke the split command of the job's corresponding arg file? If so, then 
			we assign to each split job each corresponding split arg file
	
		file_path : str

		gen_sbatch : bool
			Should we generate the sbatch files associated with each new job file object? In this case, 
			the split jobs will inherit all of the parent jobs sbatch params

		arg_file_path : str
			file_path argument to send into Arg.splits. Ignored if split_arg = False

		Returns the list of new job objects created
   	'''

   		if file_path is None:
   			file_path = '%s/%s' % (self.jobdir, self.name)
   			job_dir = self.jobdir
   			name = self.name
   		else:
   			jobdir, _, name = file_path.rpartition('/')

   		split_job_names = ['%s_split%d' % (name, i) for i in range(nsplits)]

   		# Split the job's arg file
   		if split_arg:
   			arg_objs = self.arg.split(nsplits, arg_file_path)

   		split_jobs = []
   		for i, sj in enumerate(split_job_names):

   			if split_arg: 
   				arg_ = arg_objs[i]
   			else:
   				arg_ = self.arg

   			j = NERSCJob(self.script, sj, job_dir, arg_, script_args = self.script_args, 
   						 metadata = self.metadata)

   			j.init_sbatch_params(**self.sbatch_params, startup_cmds = self.startup_cmds)

   			if gen_sbatch:
   				j.gen_sbatch_file()

   			split_jobs.append(j)

   		return split_jobs

