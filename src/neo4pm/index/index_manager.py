import neo4pm.common.identifiers as ids

class index_manager:
        
    index_db = [
        (ids.attribute, ['key', 'val']),
        (ids.log,   [ids.log_identifier]),
        (ids.trace, [ids.log_identifier]),
        (ids.trace, [ids.log_identifier, ids.case_identifier]),
        (ids.event, [ids.log_identifier, 'etl_load_name']),
        (ids.event, [ids.log_identifier, ids.case_identifier, ids.activity_identifier, ids.timestamp_identifier, 'etl_load_name'])
    ]

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
    
    def get_index_list(self):
        cypher_query = """
            CALL db.indexes
            YIELD labelsOrTypes as index_node, properties as index_attributes
        """.replace('\r', ' ').replace('\n', ' ')
        return self.connection_manager.get_query_result(cypher_query)
    
    def drop_index(self, index_node, index_attributes):
        #index_attributes.sort()
        index_clause= ','.join(str(i) for i in index_attributes)
        cypher_query = "DROP INDEX  ON :{0} ({1})".format(index_node, index_clause)
        self.connection_manager.run_command(cypher_query)
        msg = "Index is dropped."
        return msg
    
    #TODO: change it to only delete its own indexes
    def drop_all_indexes(self):
        indexes = self.get_index_list() 
        for row in indexes:
            index_node = row['index_node'][0]
            index_attributes = row['index_attributes']
            self.drop_index(index_node, index_attributes)
            
        msg = "All indexes are dropped."
        return msg
        
        
    def create_index(self, index_node, index_attributes):
        #index_attributes.sort()
        index_clause= ','.join('`{0}`'.format(i) for i in index_attributes)

        indexes = self.get_index_list()        

        index_flags = [False]
        for row in indexes:
            #row['index_attributes'].sort()
            index_flags.append(index_node in row['index_node'] and index_attributes == row['index_attributes'])

        index_exist = max(index_flags)
        if index_exist:
            msg = "Index {} already exist.".format(index_node)
        else:
            cypher_query = "CREATE INDEX ON :{0} ({1})".format(index_node, index_clause)
            self.connection_manager.run_command(cypher_query)
            msg = "Index {} is created.".format(index_node)

        return msg

    def create_all_indexes(self):
        msg_list = []
        for (i, a) in self.index_db:
            msg = self.create_index(i, a)
            msg_list.append(msg)

        return msg_list