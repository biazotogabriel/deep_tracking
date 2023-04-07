import pandas as pd
import inspect
from IPython.display import clear_output


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
        self.last_consolidated = -1
        self.processes = []
        self.backups = {}
        self.__data = data
        if self.__data is not None:
            self.backups[-1] = self.__data
        
    def __get_data(self):
        return self.__data.copy()
    
    def __set_data(self, value):
        raise PermissionError('Changing Tracker.data from outside the class is not allowed')
    
    data = property(__get_data, __set_data)
    
    def get_latest_backup_id(self, older_equal_to=float('inf')):
        return max([key for key in self.backups.keys() if key <= older_equal_to])
    
    def purge_backups(self, newer_than=-1):
        self.backups = {key: value for key, value in self.backups.items() if key <= newer_than}
        return None
    
    def rolback(self, order):
        if order < self.last_consolidated:
            backup_id = self.get_latest_backup_id(order)
            self.last_consolidated, self.__data = backup_id, self.backups[backup_id].copy()
            self.purge_backups(backup_id)
        return None
    
    def is_same_function(self, functionA, functionB):
        return inspect.getsource(functionA) == inspect.getsource(functionB)
    
    def status(self):
        status = ' flags | order - scope - action - description \n'
        for order, process in enumerate(self.processes):
            consolidated_flag = '>' if order == self.last_consolidated else ' '
            backup_flag = 'b' if self.backups.get(order) is not None else ' '
            tracked_flag = 'x' if process.tracked == False else ' '
            status += f" {consolidated_flag} {backup_flag} {tracked_flag} | {order:{5}} - {process.scope} - {process.action} - {process.description}\n"
        status += '\n[>: last consolidated | b: backed up] | x: tracked]'
        print(status)
        return None
    
    def consolidate(self, until=float('inf')):
        if self.__data is not None:
            last_process = len(self.processes) - 1
            until = min(until, last_process)
            processes_list = range(self.last_consolidated + 1, until + 1)            
            if len(processes_list) > 1:
                answer = input('More than 1 process to be consolidate. Do you want to continue? [Y to continue]')                
                clear_output()
                if answer != 'Y':
                    print('Consolidation not carried out.')
                    return None
            for order in processes_list:
                if self.processes[order].tracked:
                    data = self.processes[order].run(self.__data)
                    if not(isinstance(data, pd.DataFrame)):
                        raise TypeError(f'Function must return a DataFrame. Process order {order} [{processes[order].scope}/{processes[order].action}]')
                    self.__data = data
                    self.processes[order].consolidate = True
                    self.last_consolidated = order
        return None
    
    def get_process_order(self, scope, action):
        for order, process in enumerate(self.processes):
            if (process.scope == scope) and (process.action == action):
                return order
        return -1
    
    def process_exists(self, scope, action):
        if self.get_process_order(scope, action) == -1:
            return False
        return True

    def add_process(self, scope, action, function, description, tracked=True):
        if self.process_exists(scope, action):
            self.update_process(scope, action, function, description, tracked)
        else:
            process = Process(scope, action, function, description, tracked)
            self.processes.append(process)
            if tracked: self.consolidate()
        return None
    
    def update_process(self, scope, action, function, description, tracked=True):
        order = self.get_process_order(scope, action)
        already_consolidated = self.last_consolidated >= order
        different_tracked = self.processes[order].tracked != tracked
        both_tracked = (self.processes[order].tracked and tracked)
        different_functions = not(self.is_same_function(self.processes[order].function, function))
        
        if not(already_consolidated and (different_tracked or (both_tracked and different_functions))):
            self.processes[order].update(scope, action, function, description, tracked)
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
            self.processes[order].update(scope, action, function, description, tracked)
            self.consolidate(order)
        return None
    
    def backup(self):
        if self.__data is not None:
            self.backups[self.last_consolidated] = self.__data.copy()
            print(f'Created backup of data with processes consolidated up to order {self.last_consolidated}')
        else:
            print('Impossible to backup. The instance contains no data.')
        return None 
    
    def set_process_order(self, scope, action, new_order):
        order = self.get_process_order(scope, action)
        last_consolidated_order = min(order, new_order) - 1
        print(last_consolidated_order, order)
        self.rolback(last_consolidated_order)
        process = self.processes.pop(order)
        self.processes.insert(new_order, process)
        return None   