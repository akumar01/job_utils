def sbatch_line(flag, value):
'''
	function sbatch_line: outputs properly formatted sbatch argument line

	sbatch_param : list
		A two element list with the flag and flag value
'''
	
	sbatch_line = '#SBATCH '

	if '--' in flag:
		sbatch_line += '%s=%s' % (flag, value)
	else:
		sbatch_line += '%s %s' % (flag, value)

	sbatch_line += '\n'

	return sbatch_line