import os
import shutil
import numpy as np
import h5py_wrapper
import pickle
import time
import datetime
from glob import glob
from copy import deepcopy
from mpi_utils.ndarray import Gatherv_rows
from mpi4py import MPI
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

    def inserted_idxs_oct(self):

        epoch = datetime.datetime(2019, 10, 6).timestamp()

        # Return any children created prior to October 6th
        inserted_idxs = [child['idx'] for child in self.children if os.path.getmtime(child['path']) < epoch]

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
            print("Hi!")
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

        else:

            # Still create a dummy .dat file to indicate that the job completed
            dummy_dict = {}
            master_data_filepath = os.path.abspath(os.path.join(self.directory, '..', '%s.dat' % self.directory))
            h5py_wrapper.save(master_data_filepath, dummy_dict, write_mode = 'w')          

    # Use multiple MPI tasks to do file I/O and then gather + save
    def parallel_concatenate(self, comm, root=0):

        if len(self.children) > 0:

            rank = comm.rank
            # Have the parallel workers 
            children_chunks = np.array_split(self.children, comm.size)
            rank_children = children_chunks[rank]

            # Just gather a raw list of dictionaries
            child_index_lookup_table = np.zeros(len(rank_children))
            dict_list = []

            bad_children = []

            for i, child in enumerate(rank_children):
                try:
                    child_data = h5py_wrapper.load(child['path'])
                except:
                    bad_children.append(child)
                dict_list.append(child_data)
                child_index_lookup_table[i] = child['idx']

            # Gather across ranks
            dict_list = comm.gather(dict_list, root=root)
            lookup_table = comm.gather(child_index_lookup_table, root=root)
            bad_children = comm.gather(bad_children, root=root)

            if rank == 0:
                
                # Flatten the list(s)
                dict_list = [elem for sublist in dict_list for elem in sublist]
                lookup_table = np.array([elem for sublist in lookup_table for elem in sublist]).astype(int)
                bad_children = [elem for sublist in bad_children for elem in sublist]

                print(len(dict_list))

                # Follow the normal procedure from concatenate
                dummy_dict = dict_list[0]
                master_dict = init_structure(len(dict_list), dummy_dict)

                for i, dict_ in enumerate(dict_list):
                    master_dict = insert_data(master_dict, dict_, lookup_table[i])

                # Save
                file_name = os.path.abspath(self.directory).split('/')[-1]
                print(file_name)
                master_data_filepath = os.path.abspath('..') + '/%s.dat' % file_name
                h5py_wrapper.save(master_data_filepath, master_dict, write_mode = 'w')          
                return bad_children

        else:
            if comm.rank == 0:
                # Still create a dummy .dat file to indicate that the job completed
                dummy_dict = {}
                file_name = os.path.abspath(self.directory).split('/')[-1]
                master_data_filepath = os.path.abspath('..') + '/%s.dat' % file_name
                h5py_wrapper.save(master_data_filepath, dummy_dict, write_mode = 'w')          

                return []


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

if __name__ == '__main__':

    # Iterate through all folders in the current directory, instantiate
    # a results manager within each directory, and concatenate the results
    directories_to_do = glob('*/')    
    
    # There might be some files that are unable to be opened
    bad_files = []

    for directory in directories_to_do:
        rmanager = ResultsManager.restore_from_directory(directory)
        comm = MPI.COMM_WORLD
        bf = rmanager.parallel_concatenate(comm)
        bad_files.extend(bf)

    # Save away the list of files that could not be processed. 
    with open('bad_children.dat', 'wb') as f:
        f.write(pickle.dumps(bad_files))
