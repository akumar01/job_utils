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
        children_idxs = [int(os.path.splitext(child)[0].split('child_')[1]) for child in children]

        rmanager_instance.children = [{'path' : children[i], 'idx' : children_idxs[i]} for i in range(len(children))]
        return rmanager_instance

    def inserted_idxs(self):
        ''' function inserted idxs: Grab all indices of children'''
        inserted_idxs = [child['idx'] for child in self.children]

        return inserted_idxs

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

        self.children.append({'path' : path, 'idx' : idx})
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

            # Sequentially open up children and insert their results into a master
            # dictionary
            dummy_dict = h5py_wrapper.load(self.children[0]['path'])
            master_dict = init_structure(self.total_tasks, dummy_dict)

            for i, child in enumerate(self.children):
                child_data = h5py_wrapper.load(child['path'])
                master_dict = insert_data(master_dict, child_data, child['idx'])

            master_data_filepath = os.path.abspath(os.path.join(self.directory, '..', '%s.dat' % self.directory))
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
        elif type(value) == np.ndarray:
            dict_[key] = np.zeros((n,) + value.shape)

    return dict_


def init_structure(total_tasks, dummy_dict):
    '''

    function init_structure : Replicate the structure of dummy_dict, 
    but add an additional axis with size total_tasks

    '''

    master_dict = deepcopy(dummy_dict)
    master_dict = format_and_expand_dict(master_dict, total_tasks)

    return master_dict

# recursively traverse the dictionaries until the ndarray fields are found, and then 
# inser the child dictionary's data into the mater_dict in the appropriate location 
def insert_data(master_dict, child_dict, idx):

    for key, value in master_dict.items():
        if type(value) == dict:
            value = insert_data(value, child_dict[key], idx)
        elif type(value) == np.ndarray:
            value[idx, ...] = child_dict[key]

    return master_dict
