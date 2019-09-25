import os
import shutil
import numpy as np
import h5py_wrapper
import pickle
import time
from glob import glob
from copy import deepcopy
from mpi_utils.ndarray import Gatherv_rows
import pdb

import awkward as awk


# Class to handle atomic level results containers 
class ResultsManager():

    def __init__(self, total_tasks, directory): 

        self.total_tasks = total_tasks
        self.directory = directory 
        # Initialize container for children results
        self.children = []

    def makedir(self):
        # Initialize directory structure
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        # save the total tasks away 
        with open('%s/total_tasks' % self.directory, 'wb') as f:
            pickle.dump(self.total_tasks, f)

    @classmethod 
    def restore_from_directory(rmanager, directory):
        '''
            function restore_from_directory : grab the inserted indexes and total_tasks
            from a directory
        '''

        try:
            f = open('%s/total_tasks' % directory, 'rb')
        except:
            raise FileNotFoundError('Could not find record of total_tasks in provided directory')

        total_tasks = pickle.load(f)
        rmanager_instance = rmanager(total_tasks, directory)

        # Grab all child files
        children = glob('%s/child*' % directory)
        children_idxs = [int(os.path.splitext(child)[0].split('child_')[1]) 
                         for child in children]

        # Store children information as a list of dicts (mutable)
        rmanager_instance.children = [{'idx': children_idxs[i], 'path': children[i]} 
                                      for i in range(len(children))]

        return rmanager_instance

    def inserted_idxs(self):
        ''' function inserted idxs: Grab all indices of children'''
        return self.children['idx']


    def add_child(self, data, idx, path=None): 
        '''
            function add_child: Save the child's data and append the child to self.children

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

        self.children.append({'idx': idx, 'path': path})

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
        t0 = time.time()
        print('Gathering!')
        children = comm.gather(self.children, root=root)
        print('Gather time: %f' % (time.time() - t0))

        if comm.rank == 0:
            children = [child for sublist in children for child in sublist]

        self.children = children

    def concatenate(self):

        if len(self.children) > 0:

            # Sequentially open up children and insert their results into an 
            # awkward array Table class

            # Eventually should replace use of h5py_wrapper with something consistent
            # with awkward_array

            child_data = []

            # Order correctly
            idxs = [child['idx'] for child in self.children]
            idx_order = np.argsort(idxs)

            for i in idx_order:
                child = self.children[i]
                child_data.append(h5py_wrapper.load(child['path']))

            # Convert the values of child_data to JaggedArrays
            master_table = awk.fromiter(child_data)

            master_data_filepath = os.path.abspath(os.path.join(self.directory, '..', '%s.dat' % self.directory))
            # Use pickle for greater compatibility
            with open(master_data_filepath, 'wb') as f:
                f.write(pickle.dumps(master_table))

        else:

            # If no children are present, create a dummy, empty .dat file to flag that cleanup
            # was still successful
            master_data_filepath = os.path.abspath(os.path.join(self.directory, '..', '%s.dat' % self.directory))
            with open(master_data_file_path, 'wb') as f:
                f.write(pickle.dumps(0))

    def cleanup(self):

        # Delete self.directory and all of its contents: 
        shutil.rmtree(self.directory)

# Assemble all subdirectories contained in path, initialize a results manager
# and then call its concatenate and cleanup methods 
def concat_subdirs(path):
    subdirs = [f.path for f in os.scandir(path) if f.is_dir()]

    for subdir in subdirs:
        try:
            rmanager = ResultsManager.restore_from_directory(subdir)
            rmanager.concatenate()
        except:
            pass

def clean_subdirs(path):
    subdirs = [f.path for f in os.scandir(path) if f.is_dir()]

    for subdir in subdirs:
        try:
            rmanager = ResultsManager.restore_from_directory(subdir)
            rmanager.cleanup()
        except:
            pass
