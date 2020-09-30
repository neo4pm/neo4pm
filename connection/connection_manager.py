from py2neo import Graph

class connection_manager:

    def __init__(self, uri, userName, password):
        self.uri = uri
        self.user = userName
        self.password = password
        
        self.connection = Graph(uri, auth=(userName, password))
        
        
    def get_connection(self):
        return self.connection
    
    def run_command(self, cypher):
        try:
            res = self.connection.run(cypher)
        except:
            try:
                res = self.connection.run(cypher)
            except:
                raise
        return res
    
    def get_query_result(self, cypher):
        return self.run_command(cypher).data()

    
