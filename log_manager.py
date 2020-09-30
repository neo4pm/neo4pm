from neo4pm.connection import connection_manager
from neo4pm.index import index_manager
from neo4pm.dfg import dfg_manager

class log_manager:

    def __init__(self, uri, userName, password):
        self.connection_manager = connection_manager(uri, userName, password)        
        self.index_manager = index_manager(self.connection_manager)
        self.index_manager.create_all_indexes()
        #self.csv_importer = csv_importer(self.connection_manager)
        self.dfg_manager = dfg_manager(self.connection_manager)
    
    def log_mapper(self, case_id, task_id, time_stamp, time_format):
        return self.csv_importer.log_mapper(case_id, task_id, time_stamp, time_format)
        
    def import_csv(self, file_name, log_name, log_mapping):
        res = self.csv_importer.import_csv(file_name, log_name, log_mapping)
        return res

    def drop_all(self):
        self.index_manager.drop_all_indexes()
        self.connection_manager.run_command('match(a) detach delete a')

    def get_dfg(self, log_name=None):
        return self.dfg_manager.get_dfg(log_name)