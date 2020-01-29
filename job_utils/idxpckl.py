import pickle
import struct 
import pandas as pd
import resource
import os
import glob
import time
import pdb

class Indexed_Pickle():
    '''
    class Indexed Pickle: Storage solution for saving/reading multiple arbitrary python objects
    stored in a single pickle file, which would normally need to be loaded sequentially

    OOP for reading: __init__ --> init_read --> read --> close_read
    OOP for saving : __init__ --> init_save --> save --> close_save

    file : str
        Path of file to be read/written to

    '''
    
    def __init__(self, file):
    
        self.file = file

    def init_read(self):
        '''
        function init_reader : Initialize reading of the file object by loading the
        file metadata and the pickled file index
        '''
        fobj = open(self.file, 'rb')
        fobj.seek(0, 0)
        index_loc = fobj.read(8)
        # If on Windows, this line will need to be modified!
        index_loc = struct.unpack('L', index_loc)[0]
        self.nobj = pickle.load(fobj)
        self.header = pickle.load(fobj)

        fobj.seek(index_loc, 0)
        self.index = pickle.load(fobj)
        self.fobj = fobj

    def read(self, idx):
        '''
        function read: Return the object stored at the index location idx
        '''
        self.fobj.seek(self.index[idx], 0)
        data = pickle.load(self.fobj)
        return data

    def close_read(self):
        '''
        function close: Close the file object, if it exists, after reading
        '''    
        if hasattr(self, 'fobj'):
            self.fobj.close()

        del self.index
        del self.fobj

    def init_save(self, nobj=0, header_data = {}):
        '''
        function init_saver: Initialize the file object for saving by 
        placing the preliminary metadata in place at the start of the file

        nobj : int
            Number of python objects to be stored in the file
            This functions essentially as redundant metadata and can safely 
            be ignored

        header_data : dict
            Any other metadata. Not counted in nobj or tracked in the index

        '''
        fobj = open(self.file, 'wb')
        fobj.seek(0, 0)

        # Buffer to be used later on
        fobj.write(struct.pack('L', 0))

        # First pickle away the number of objects to be stored in the file
        fobj.write(pickle.dumps(nobj))

        # Then any other header data to be stored. This will be considered s
        # separate to the indexed list of python objects
        fobj.write(pickle.dumps(header_data))

        self.fobj = fobj
        # keep track of locations of saved objects
        self.index = []

    def save(self, obj):
        '''
        function save : Save a python object to the file and append its location to the
        index

        obj : Picklable python object
        '''

        # Where are we in the file index?
        self.index.append(self.fobj.tell())
        self.fobj.write(pickle.dumps(obj))

    def close_save(self):
        '''
        function close_save: Save the index at the start of the file and close the fobj
        '''

        index_loc = self.fobj.tell()
        self.fobj.write(pickle.dumps(self.index))
        self.fobj.seek(0, 0)
        # Possibly needs to be changed if we are on Windows!
        self.fobj.write(struct.pack('L', index_loc))
        self.fobj.close()

    # Take the saved objects in the file and create a dataframe
    # Requires the saved data to be in the form of appropriately formatted
    # dictionaries
    def to_dataframe(self):

        if not hasattr(self, 'index'):
            self.init_read()

        df_seed = self.read(0)
        df = pd.DataFrame(df_seed)
        for i, obj in enumerate(range(len(self.index[1:]))):
            t0 = time.time()
            dictlist = self.read(obj)
            df = df.append(pd.DataFrame(dictlist))            
            print('%d, %f' % (i, time.time() -t0))

        self.close_read()

        return df




