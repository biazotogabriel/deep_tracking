import pandas as pd
import inspect
import pickle
import os.path
from IPython.display import clear_output
import zipfile
import io


class Process:
    def __init__(self, scope, action, function, description, tracked):
        self.scope = scope
        self.action = action
        self.function = function
        self.description = description
        self.tracked = tracked
        
    def run(self, data):
        data = data.copy()
        return self.function(data)
    
    def update(self, scope, action, function, description, tracked):
        self.scope = scope
        self.action = action
        self.function = function
        self.description = description
        self.tracked = tracked

class Tracker: 
    def __init__(self, data=None):
        self.__last_consolidated = -1
        self.__processes = []
        self.__backups = {}
        self.__data = data
        if self.__data is not None:
            self.__backups[-1] = self.__data
        
    def __get_data(self):
        return self.__data.copy()
    
    def __set_data(self, value):
        raise PermissionError('Changing Tracker.data from outside the class is not allowed')
    
    data = property(__get_data, __set_data)
    
    def get_latest_backup_id(self, older_equal_to=float('inf')):
        return max([key for key in self.__backups.keys() if key <= older_equal_to])
    
    def purge_backups(self, newer_than=-1):
        self.__backups = {key: value for key, value in self.__backups.items() if key <= newer_than}
        return None
    
    def rolback(self, order):
        if order < self.__last_consolidated:
            backup_id = self.get_latest_backup_id(order)
            self.__last_consolidated, self.__data = backup_id, self.__backups[backup_id].copy()
            self.purge_backups(backup_id)
        return None
    
    def is_same_function(self, functionA, functionB):
        return inspect.getsource(functionA) == inspect.getsource(functionB)
    
    def status(self):
        status = ' flags | order - scope - action - description \n'
        for order, process in enumerate(self.__processes):
            consolidated_flag = '>' if order == self.__last_consolidated else ' '
            backup_flag = 'b' if self.__backups.get(order) is not None else ' '
            tracked_flag = 'x' if process.tracked == False else ' '
            status += f" {consolidated_flag} {backup_flag} {tracked_flag} | {order:{5}} - {process.scope} - {process.action} - {process.description}\n"
        status += '\n[>: last consolidated | b: backed up] | x: tracked]'
        print(status)
        return None
    
    def consolidate(self, until=float('inf')):
        if self.__data is not None:
            last_process = len(self.__processes) - 1
            until = min(until, last_process)
            processes_list = range(self.__last_consolidated + 1, until + 1)            
            if len(processes_list) > 1:
                answer = input('More than 1 process to be consolidate. Do you want to continue? [Y to continue]')                
                clear_output()
                if answer != 'Y':
                    print('Consolidation not carried out.')
                    return None
            for order in processes_list:
                if self.__processes[order].tracked:
                    data = self.__processes[order].run(self.__data)
                    if not(isinstance(data, pd.DataFrame)):
                        raise TypeError(f'Function must return a DataFrame. Process order {order} [{processes[order].scope}/{processes[order].action}]')
                    self.__data = data
                    self.__processes[order].consolidate = True
                    self.__last_consolidated = order
        return None
    
    def get_process_order(self, scope, action):
        for order, process in enumerate(self.__processes):
            if (process.scope == scope) and (process.action == action):
                return order
        return -1
    
    def get_backup_before(self, scope=None, action=None, order=None):
        if (order is None) and (scope is None) and action is None:
            raise TypeError('At least one parameter must be send.')
        if order is None:
            order = self.get_process_order(scope, action)
            if order == -1:
                order = float('inf')
        backup_id = self.get_latest_backup_id(order)
        backup = self.__backups[backup_id] 
        return backup.copy()
    
    def process_exists(self, scope, action):
        if self.get_process_order(scope, action) == -1:
            return False
        return True

    def add_process(self, scope, action, function, description, tracked=True):
        if self.process_exists(scope, action):
            self.update_process(scope, action, function, description, tracked)
        else:
            process = Process(scope, action, function, description, tracked)
            self.__processes.append(process)
            if tracked: self.consolidate()
        return None
    
    def update_process(self, scope, action, function, description, tracked=True):
        order = self.get_process_order(scope, action)
        already_consolidated = self.__last_consolidated >= order
        different_tracked = self.__processes[order].tracked != tracked
        both_tracked = (self.__processes[order].tracked and tracked)
        different_functions = not(self.is_same_function(self.__processes[order].function, function))
        
        if not(already_consolidated and (different_tracked or (both_tracked and different_functions))):
            self.__processes[order].update(scope, action, function, description, tracked)
        else:                        
            last_valid_backup = self.get_latest_backup_id(order)
            answer = input(f'''Process {order} [{scope}/{action}] is tracked and consolidated.
If you continue, the consolidating of the data will be returned to the order {last_valid_backup} (last valid backup).
All processes after this one must be re-executed.
Do you want continue? [Y to continue] :''')
            clear_output()
            if answer != 'Y':
                print('Updating not carried out.')
                return None
            self.rolback(order - 1)        
            self.__processes[order].update(scope, action, function, description, tracked)
            self.consolidate(order)
        return None
    
    def backup(self, verbose=False):
        if self.__data is not None:
            self.__backups[self.__last_consolidated] = self.__data.copy()
            if verbose:
                print(f'Created backup of data with processes consolidated up to order {self.__last_consolidated}.')
        else:
            if verbose:
                print('Impossible to backup. The instance contains no data.')
        return None 
    
    def set_process_order(self, scope, action, new_order):
        order = self.get_process_order(scope, action)
        last_consolidated_order = min(order, new_order) - 1
        print(last_consolidated_order, order)
        self.rolback(last_consolidated_order)
        process = self.__processes.pop(order)
        self.__processes.insert(new_order, process)
        return None   
    
    def run(self, data, processes):
        for process in processes:
            order = self.get_process_order(process[0], process[1])
            data = self.__processes[order].run(data)
        return data
    
    def save(self, file_name):
        if os.path.exists(f'{file_name}.trk'):
            answer = input('There is already another tracker saved with this name. Do you want to continue? [Y to continue]')
            if answer != 'Y':
                return None
            
        data = (self.__last_consolidated,
                self.__processes,
                self.__backups,
                self.__data)
        data = pickle.dumps(data)
        print('Compressing data...')
        zipfile.ZipFile(f'{file_name}.trk', mode='w', compression=zipfile.ZIP_BZIP2, compresslevel=5).writestr('data.pickle', data)
            
        print(f'Tracker saved as {file_name}.trk!')
        return None
        
    def load(self, file_name):   
        if os.path.exists(f'{file_name}.trk'):
            if (len(self.__processes) > 0) or (self.__data is not None):
                answer = input('Tracker already has processes or data. Do you want to continue? [Y to continue]')
                if answer != 'Y':
                    return None    
            
            print('Decompressing data...')
            data = pickle.loads(zipfile.ZipFile(f'{file_name}.trk', 'r').open('data.pickle').read())
        
            self.__last_consolidated = data[0]
            self.__processes = data[1]
            self.__backups = data[2]
            self.__data = data[3]
            print(f'Tracker loaded from {file_name}.trk!')
        else:
            print('File does not exist!')
        return None
        