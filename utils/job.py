import shlex
from utils import sbatch_line
# To do:
# Base job class
# NERSC-specific job class
# Scripts to create job structures
# Arg file object --> assign arg file object to a job
# Validator objects that test whether a job is setup correctly

# Consider a 'Result' type or some indication that is saved with the job of 
# tracking of partial progress, allowing one to resume without incident

# Where do we want to provide the saving/loading functionality for these classes?

class Job()
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

	arg_file : str
		Path to where an Args class object is saved away. These arguments will be used by the script 
		executed by the job	

	data_file : str
		Path to where the data will be saved. The actual saving of the data should be handled 

	script args : list
		Additional arguments to be passed onto the script
	
	metadata : A dictionary of additional metadata that is stored away with the job

'''

	def __init__(self, script, name, jobdir, 
				 arg_file=None, data_file=None, 
				 script_args = [], metadata=None):


		self.script = script
		self.name = name
		self.jobdir = jobdir

		self.arg_file = arg_file

		self.data_file = data_file

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
		pass


class NERSCJob(Job)
'''
		Class NERSCJob: Job class adapted for operating on NERSC. Supports operation within any qos and both
		Haswell and KNL architectures
'''

	def __init__(self, script, arg_file=None, data_file=None, metadata=None): 

		super(NERSC_job, self).__init__(script, arg_file, data_file, metadata)

		# Store all sbatch params in a dict for easy reference later on
		self.sbatch_params = {}

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


