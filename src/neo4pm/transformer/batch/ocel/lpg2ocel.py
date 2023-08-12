ocel_event_id = 'ocel:eid'
ocel_object_id = 'ocel:oid'
ocel_activity_label = 'ocel:activity'
ocel_timestamp_label = 'ocel:timestamp'
ocel_type_label = 'ocel:type'

mdpm_node_log_label = ':Log'
mdpm_node_event_label = ':Event'
mdpm_node_entity_label = ':Entity'

mdpm_id_label = 'ID'
mdpm_log_label = 'Log'
mdpm_timestamp_label = 'timestamp'
mdpm_reified_label = 'Reified'
mdpm_correspond_label = 'CORR'
mdpm_activity_label = 'Activity'
mdpm_entity_type_label = 'EntityType'
mdpm_reified_column_label = 'Type.1'


mdpm_batch_export_id_label = '_id'
mdpm_batch_export_rel_start_label = '_start'
mdpm_batch_export_rel_end_label = '_end'
mdpm_batch_export_rel_type_label = '_type'
mdpm_batch_export_labels_label = '_labels'

class lpg2ocel:
    def apply(source_file, destination_folder, export_format='jsonocel'):
        import pandas as pd
        import numpy as np
        import pm4py
        from pm4py.objects.ocel.obj import OCEL
        from pm4py.objects.ocel.validation import jsonocel
        from pm4py.objects.ocel.validation import xmlocel
        from pathlib import Path
        import os

        def getNodes(df, entityType=np.nan):
            return df[df._labels.isnull() if pd.isna(entityType) else df._labels==entityType].dropna(how='all', axis='columns')
        
        try:
            assert export_format in ['xmlocel', 'jsonocel']
        except:
            print('Specified format is not supported')
            return
        
        file_name=Path(source_file).stem

        df = pd.read_csv(source_file, 
                         dtype={mdpm_batch_export_id_label:"str", mdpm_batch_export_rel_start_label:"str", mdpm_batch_export_rel_end_label:"str", mdpm_batch_export_labels_label:"str"}, 
                         parse_dates=[mdpm_timestamp_label], low_memory=False, index_col=False)
        ocels = {}

        for idx, row in getNodes(df, mdpm_node_log_label).iterrows():
            log__id = row[mdpm_batch_export_id_label]
            log_ID = row[mdpm_id_label]

            df_log_event_ids = getNodes(df)[getNodes(df)[mdpm_batch_export_rel_start_label]==log__id][[mdpm_batch_export_rel_end_label]].rename(columns={mdpm_batch_export_rel_end_label:mdpm_batch_export_id_label}).drop_duplicates()

            df_log_events = getNodes(df, mdpm_node_event_label).merge(df_log_event_ids).rename(columns={mdpm_batch_export_id_label:ocel_event_id, mdpm_activity_label:ocel_activity_label,mdpm_timestamp_label:ocel_timestamp_label}).drop_duplicates()
            col = df_log_events.columns.drop([mdpm_batch_export_labels_label, mdpm_log_label])
            df_log_events = df_log_events[col]

            df_rel_log_from_events = getNodes(df).merge(df_log_event_ids, left_on=mdpm_batch_export_rel_start_label, right_on=mdpm_batch_export_id_label).drop_duplicates()
            df_log_all_entity_ids = df_rel_log_from_events[df_rel_log_from_events[mdpm_batch_export_rel_type_label]==mdpm_correspond_label][[mdpm_batch_export_rel_end_label]].rename(columns={mdpm_batch_export_rel_end_label:mdpm_batch_export_id_label})
            df_rel_log_from_entity = getNodes(df).merge(df_log_all_entity_ids, left_on=mdpm_batch_export_rel_start_label, right_on=mdpm_batch_export_id_label).drop_duplicates()
            if mdpm_reified_column_label in df_rel_log_from_entity.columns:
                df_log_reified_entity_ids = df_rel_log_from_entity[df_rel_log_from_entity[mdpm_reified_column_label]==mdpm_reified_label][[mdpm_batch_export_rel_start_label]].rename(columns={mdpm_batch_export_rel_start_label:mdpm_batch_export_id_label})
            else:
                df_log_reified_entity_ids = df_rel_log_from_entity.head(0)[[mdpm_batch_export_rel_start_label]].rename(columns={mdpm_batch_export_rel_start_label:mdpm_batch_export_id_label})
            df_entity_merged_reified = getNodes(df, mdpm_node_entity_label).merge(df_log_reified_entity_ids, how='left', indicator=True).drop_duplicates()
            df_entity_non_reified = df_entity_merged_reified[df_entity_merged_reified['_merge']=='left_only'][[mdpm_batch_export_id_label]]
            df_log_objects = df_entity_merged_reified[df_entity_merged_reified['_merge']=='left_only'][[mdpm_batch_export_id_label,mdpm_entity_type_label]].rename(columns={mdpm_entity_type_label:ocel_type_label, mdpm_batch_export_id_label:ocel_object_id})
            df_log_rels = getNodes(df).merge(df_log_event_ids, left_on=mdpm_batch_export_rel_start_label, right_on=mdpm_batch_export_id_label).drop_duplicates()
            df_log_rels = df_log_rels[df_log_rels[mdpm_batch_export_rel_type_label]==mdpm_correspond_label]
            df_log_rels = df_log_rels.merge(df_entity_non_reified, left_on=mdpm_batch_export_rel_end_label, right_on=mdpm_batch_export_id_label)
            df_log_rels = df_log_events.merge(df_log_rels, left_on=ocel_event_id, right_on=mdpm_batch_export_rel_start_label).merge(df_log_objects, left_on=mdpm_batch_export_rel_end_label, right_on=ocel_object_id)[[ocel_event_id, ocel_activity_label, ocel_timestamp_label,ocel_object_id,ocel_type_label]]


            ocels[log_ID] = OCEL(
                events=df_log_events
                ,objects = df_log_objects[[ocel_type_label,ocel_object_id]]
                ,relations = df_log_rels
            )

        for logid in ocels:
            ocels[logid].events[ocel_timestamp_label] = ocels[logid].events[ocel_timestamp_label].dt.tz_convert(None)
            output_file = os.path.join(destination_folder, file_name+'_'+str(logid)+'.'+export_format)
            
            
            pm4py.write_ocel(ocels[logid], output_file)
            if export_format=='jsonocel':
                validation_result = jsonocel.apply(output_file, "./schema.json")
            else:
                validation_result = xmlocel.apply(output_file, "./schema.xml")
            
            if validation_result:
                print('{} is exported successfully.'.format(output_file))
            else:
                print('It was a problem in exporting the file properly.')