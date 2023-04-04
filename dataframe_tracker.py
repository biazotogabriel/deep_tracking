# I'm going to create a lib for it
import inspect
from IPython.display import clear_output

class Tracker: 
    def __init__(self, data=None):
        self.last_process_consolidated = -1
        self.processes = []
        self.backups = {}
        self.data = data
        if self.data is not None:
            self.backups[-1] = self.data
        
    def get_last_backup_id(self, older_than=float('inf')):
        last_backup_id = max([key for key in self.backups.keys() if key < older_than])
        return last_backup_id
    
    def get_process_id(self, column, action):
        for _id, process in enumerate(self.processes):
            if (process['column'] == column) and (process['action'] == action):
                return _id                
        return None
    
    def is_same_function(self, _id, function):
        return inspect.getsource(function) == inspect.getsource(self.processes[_id]['function'])
    
    def purge_backups(self, newer_than=0):
        self.backups = {key: value for key, value in self.backups.items() if key <= newer_than}
        return None
    
    def rolback_consolidation(self, backup_id):
        self.last_process_consolidated, self.data = backup_id, self.backups[backup_id].copy()
        self.purge_backups(backup_id)   
        for _id, process in enumerate(self.processes):
            if _id > backup_id:
                process['consolidated'] = False
            
    def consolidate(self, until_process=float('inf')):
        if self.data is not None:
            last_process = len(self.processes) - 1
            until_process = min(until_process, last_process)         
            proccesses_list = range(self.last_process_consolidated + 1 ,until_process + 1)
            if len(proccesses_list) > 1:
                answer = input('More than 1 process to consolidate. Do you want to continue? [Y to continue]')
                clear_output()
                if answer != 'Y':       
                    print('Preprocessing data not consolidated.')
                    return None
            for _id in proccesses_list:
                self.data = self.processes[_id]['function'](self.data)
                self.processes[_id]['consolidated'] = True
                self.last_process_consolidated = _id 
        return None

    def track(self, column='_general_', action='_general_', function=lambda data: data, description='_general_'):
        process = {'column': column, 
                   'action': action, 
                   'function': function, 
                   'description': description,
                   'consolidated': False}
        process_id = self.get_process_id(column, action)      
        if process_id is None:
            self.processes.append(process)
            self.consolidate()
        else:
            if (self.data is not None) and self.processes[process_id]['consolidated']:
                if self.is_same_function(process_id, function):
                    process['consolidated'] = self.processes[process_id]['consolidated']
                else:
                    last_backup_id = self.get_last_backup_id(process_id)
                    answer = input(f'''Process  [{process_id} | {column} | {action}] already exists with another function.
If you continue, the consolidating of the data will be returned to the process {last_backup_id} (last consolidated backup).
All processes after this one must be re-executed.
Do you want continue? [Y to continue] :''')
                    clear_output()
                    if answer != 'Y':
                        print('Tracking aborted.')
                        return None
                    self.rolback_consolidation(last_backup_id)        
                    
            self.processes[process_id] = process
            self.consolidate(process_id)
        #print(description)
        return None
    
    def set_process_order(self, column, action, new_order, consolidate=True):
        process_id = self.get_process_id(column, action)
        last_consolidated_id = min(process_id, new_order) - 1
        last_backup_id = self.get_last_backup_id(last_consolidated_id)
        self.rolback_consolidation(last_backup_id)
        process = self.processes.pop(process_id)
        self.processes.insert(new_order, process)
        if consolidate:
            self.consolidate()
        return None    
    
    def status(self):
        status = ' flag | id - column - action - description \n'
        for _id, process in enumerate(self.processes):
            consolidated_flag = '>>' if _id == self.last_process_consolidated else '  '
            backup_flag = 'b' if self.backups.get(_id) is not None else ' '
            status += f" {consolidated_flag} {backup_flag} | {_id} - {process['column']} - {process['action']} - {process['description']}\n"
        status += '\n[>>: last consolidated | b: backed up]'
        print(status)
        return None
    
    def backup(self):
        if self.data is not None:
            self.backups[self.last_process_consolidated] = self.data.copy()
            print(f'Created backup of data with processes executed up to process {self.last_process_consolidated}')
        else:
            print('Impossible to backup. The preprocessing instance contains no data')
        return None
    
    def view_processes(self):
        processes_list = ''
        for process in self.processes:
            processes_list += f"('{process['column']}', '{process['action']}'), # {process['description']}" + '\n'
        print(processes_list)
        return None
        
    def process(self, data): 
        for process in self.processes:
            data = process['function'](data)
        return data