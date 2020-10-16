import neo4pm.common.identifiers as ids

import pandas as pd
import numpy as np
import datetime

# ids.log_identifier , ids.case_identifier , ids.activity_identifier , ids.timestamp_identifier 
previous_prefix = 'previous_'
event_id_label = 'event_id'
event_type_label = 'event_type'

node_type_log = ids.log
node_type_trace = ids.trace
node_type_event = ids.event
node_type_attribute = ids.attribute
relation_type = ids.relation

relation_type_label_has = ids.relation_has
relation_type_label_next = ids.relation_next
relation_type_label_contains = ids.relation_contains

def get_attributes(df, node_type):
    df_att_tmp = pd.DataFrame(df[node_type].unique(), columns={'val'}).dropna()
    df_att_tmp['key'] = node_type
    return df_att_tmp

class transformer:
        
    def transform_log(log_name, df_log, columns_tags):
        # cleaning dataframe
        #dropping duplicates
        df = df_log.drop_duplicates()

        #changing column names
        df = df.rename(columns={columns_tags[i]:i for i in columns_tags})

        #adding log info
        df[ids.log_identifier] = log_name

        # setting the time into right format
        df[ids.timestamp_identifier] = pd.to_datetime(df[ids.timestamp_identifier]) 
        df[ids.timestamp_identifier] = df.time_timestamp.map(lambda x: datetime.datetime.strftime(x, "%Y-%m-%dT%H:%M:%S.%f"))

        #sorting the dataset
        df = df.sort_values(by=[ids.log_identifier, ids.case_identifier, ids.timestamp_identifier])
        
        df[event_type_label]=node_type_event
        df['key']=np.NaN
        df['val']=np.NaN

        df[event_id_label] = df.index.astype(float)

        df[previous_prefix+event_id_label] = df.event_id.shift(1)
        df[previous_prefix+ids.case_identifier] = df.case_concept_name.shift(1)
        df.loc[df[ids.case_identifier]!=df[previous_prefix+ids.case_identifier], [previous_prefix+event_id_label]] = np.NaN
        df.loc[df[ids.case_identifier]!=df[previous_prefix+ids.case_identifier], [previous_prefix+ids.case_identifier]] = np.NaN

        df = df.drop([previous_prefix+ids.case_identifier], axis=1)

        df_tmp = pd.DataFrame(df[ids.log_identifier].unique(), columns={ids.log_identifier}).dropna()
        df_tmp[event_type_label]=node_type_log
        
        df = df.append(df_tmp, ignore_index=False)
        
        df_tmp = pd.DataFrame(df[ids.case_identifier].unique(), columns={ids.case_identifier}).dropna()
        df_tmp[ids.log_identifier]=log_name
        df_tmp[event_type_label]=node_type_trace
        
        df = df.append(df_tmp, ignore_index=False)

        df_att_tmp = get_attributes(df, ids.log_identifier)
        df_att_tmp = df_att_tmp.append(get_attributes(df, ids.case_identifier))
        df_att_tmp = df_att_tmp.append(get_attributes(df, ids.activity_identifier))
        #df_att_tmp = df_att_tmp.reset_index(drop=True)
        df_att_tmp[event_type_label] = node_type_attribute
        
        df = df.append(df_att_tmp, ignore_index=False)
        
        df_id_na =  df[df[event_id_label].isna()]
        df_id_na.index = range(max(df.index)+1,max(df.index)+len(df_id_na)+1)
        df_id_na[event_id_label] = df_id_na.index.astype(float)
        
        df = df[~df[event_id_label].isna()]
        
        df = df.append(df_id_na, ignore_index=False)
        
        # filtering logs
        df_log = df[df[event_type_label]==node_type_log][[event_id_label, ids.log_identifier, event_type_label]]
        df_log.event_id = df_log.event_id.astype('str')
        df_log = df_log.rename(columns={event_id_label: 'id:ID', event_type_label: ':LABEL'})
        
        #filteering traces
        df_trace = df[df[event_type_label]==node_type_trace][[event_id_label, ids.log_identifier, ids.case_identifier, event_type_label]]
        df_trace.event_id = df_trace.event_id.astype('str')
        df_trace = df_trace.rename(columns={event_id_label: 'id:ID', event_type_label: ':LABEL'})
        
        #filtering events
        df_event = df[df[event_type_label]==node_type_event][[event_id_label, ids.log_identifier, ids.case_identifier, ids.activity_identifier, ids.timestamp_identifier, event_type_label]]
        df_event.event_id = df_event.event_id.astype('str')
        df_event = df_event.rename(columns={event_id_label: 'id:ID', ids.timestamp_identifier: ids.timestamp_identifier+':datetime', event_type_label: ':LABEL'})
        
        #filtering attributes
        df_attribute = df[df[event_type_label]==node_type_attribute][[event_id_label, 'key', 'val', event_type_label]]
        df_attribute.event_id = df_attribute.event_id.astype('str')
        df_attribute = df_attribute.rename(columns={event_id_label: 'id:ID', event_type_label: ':LABEL'})
        
        #creating relations -----------
        
        # log -> attribute relation
        df_relations = df_log.merge(df_attribute[df_attribute['key']==ids.log_identifier], left_on=ids.log_identifier, right_on='val')[['id:ID_x', 'id:ID_y']]
        
        # Trace -> attribute relation
        df_tmp = df_trace.merge(df_attribute[df_attribute['key']==ids.case_identifier], left_on=ids.case_identifier, right_on='val')[['id:ID_x', 'id:ID_y']]
        df_relations = df_relations.append(df_tmp, ignore_index=True)
        
        # Event -> attribute relation
        df_tmp = df_event.merge(df_attribute[df_attribute['key']==ids.activity_identifier], left_on=ids.activity_identifier, right_on='val')[['id:ID_x', 'id:ID_y']]
        df_relations = df_relations.append(df_tmp, ignore_index=True)

        df_relations[':TYPE'] = relation_type_label_contains
        df_relations = df_relations.rename(columns={'id:ID_x': ':START_ID', 'id:ID_y': ':END_ID'})
        
        # next relation
        df_tmp = df[df[event_type_label]==node_type_event][~df[previous_prefix+event_id_label].isna() & ~df[event_id_label].isna()][[previous_prefix+event_id_label, event_id_label]]
        df_tmp[':TYPE'] = relation_type_label_next
        df_tmp = df_tmp.rename(columns={previous_prefix+event_id_label: ':START_ID', event_id_label: ':END_ID'})
        df_relations = df_relations.append(df_tmp, ignore_index=True)
        
        # Log -> Trace relation
        df_tmp = df_trace.merge(df_log, on=ids.log_identifier)[['id:ID_y', 'id:ID_x']]
        df_tmp[':TYPE'] = relation_type_label_has
        df_tmp = df_tmp.rename(columns={'id:ID_y': ':START_ID', 'id:ID_x': ':END_ID'})
        df_relations = df_relations.append(df_tmp, ignore_index=True)
        
        # Trace -> Event relation
        df_tmp = df_event.merge(df_trace, on=[ids.log_identifier, ids.case_identifier])[['id:ID_y', 'id:ID_x']]
        df_tmp[':TYPE'] = relation_type_label_has
        df_tmp = df_tmp.rename(columns={'id:ID_y': ':START_ID', 'id:ID_x': ':END_ID'})
        df_relations = df_relations.append(df_tmp, ignore_index=True)
        
        df_relations = df_relations.reset_index(drop=True)
        
        for c in df_relations.columns:
            df_relations[c] = df_relations[c].astype(str)

        result = {
            'nodes_'+node_type_log.lower(): df_log, 
            'nodes_'+node_type_trace.lower(): df_trace, 
            'nodes_'+node_type_event.lower(): df_event, 
            'nodes_'+node_type_attribute.lower(): df_attribute, 
            relation_type.lower(): df_relations
        }
        
        return result

