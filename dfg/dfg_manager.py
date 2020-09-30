import neo4pm.common.identifiers as ids

class dfg_manager:

    def __init__(self, connection_manager):
        self.connection_manager = connection_manager
    
    def get_dfg(self, log_name=None):
        log_clause =  " "
        if not log_name is None:
            log_clause =  " {{{0}:'{1}'}}".format(ids.log_identifier, log_name)

        cypher_query="""
            match (t1:{3} {{key:'{1}'}})<--(:{2}{0})-[n:{4}]->(:{2}{0})-->(t2:{3} {{key:'{1}'}})
            return t1.val as dfg_from, t2.val as dfg_to, count(n) as dfg_freq
        """.format(log_clause, ids.activity_identifier, ids.event, ids.attribute, ids.relation_next)

        dfg = self.connection_manager.get_query_result(cypher_query)
        dfg = {(r['dfg_from'], r['dfg_to']): r['dfg_freq'] for r in dfg}
        return dfg
    