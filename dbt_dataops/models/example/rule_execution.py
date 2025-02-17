import pytz
import traceback
import calendar
import sys
import pandas as pd
import datetime as dt
from datetime import datetime
import snowflake.connector
from pytz import timezone
from redshift_connector import connect
from pathlib import Path

# Common paths 
path_home=str(Path.home())
common_path = Path(__file__).resolve().parent.parent / 'commons'
print(f"common path in exe: {common_path}")
sys.path.insert(0, str(common_path))

# Custom module 
import db_utils as db_ops
from utils import get_secret
from constants import environment, SNOWFLAKE_SECRET_NAME, aws_region


def snowflake_conn(conf_dict):
    """
        Connection to snowflake
        :param conf_dict: conf_dict
        :return string: connection object
    """
    try:
        connection = snowflake.connector.connect(**conf_dict)
    except Exception:
        sf_error_msg = "Connection to snowflake failed"
        print(sf_error_msg)
        raise Exception(sf_error_msg)
    return connection
    
def insert_df_to_snowflake(df, table, ctx):
    """
    Inserts a pandas dataframe to snowflake table
    :param df: dataframe to be inserted
    :param table: name of the table to be inserted to
    :param ctx: sf connection object
    :return:
    """
    try:
        # creating column list for insertion
        cols = ",".join([str(i) for i in df.columns.tolist()])	
        print(cols)
        vals = "%s," * (len(df.columns) - 1) + "%s"
        print(vals)
        sf_cursor = ctx.cursor()
        for _, row in df.iterrows():
            cols = ','.join(df.columns)
            
            vals = []
            for col in df.columns:
                val = row[col]
                if val is None:
                    vals.append('NULL')
                elif isinstance(val, (int, float)):   
                    vals.append(str(val))
                else:   
                    vals.append(repr(str(val)))
            
            vals_str = ','.join(vals)
            query = f"INSERT INTO {environment}_dwdb.dqm.{table}({cols}) VALUES ({vals_str})"
            print(query)
            dq_date_id = dt.date.today().strftime("%Y%m%d")
            update_issue_status_column_1 = f"update {environment}_dwdb.dqm.fact_dq_validation set issue_status = 'New' where issue_status is null and dq_date_id >= '{dq_date_id}' and DQ_RESULT NOT IN ('Passed')"
            update_issue_status_column_2 = f"update {environment}_dwdb.dqm.fact_dq_metric set issue_status = 'New' where issue_status is null and dq_date_id >= '{dq_date_id}' and DQ_RESULT NOT IN ('Passed')"
            sf_cursor.execute(update_issue_status_column_1)
            sf_cursor.execute(update_issue_status_column_2)
            
            try:
                sf_cursor.execute(query)
            except Exception as e:
                raise Exception(f"Failed to execute query: {query}. Error: {e}")
        sf_cursor.execute('commit')
        print(len(df), "record inserted.")
    except Exception as e:
        raise Exception(f"Error in Snowflake insert operation {e}")

def log_stat(msg):
    cur_dat_time = cur_dttm()
    print(cur_dat_time+" |"+msg)
    
def cur_dttm():
    cur_dat_time = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
    return cur_dat_time
     
def get_rule_map_entry(rule_id):
    log_stat("Fetching entries from rule map table for : "+str(rule_id))
    fetch_query = f"select e.entity_rule_id,e.dq_entity,m.master_rule_id,m.rule_type,e.entity_rule_nm,m.qual_dimension_id,m.qual_dimension_nm,p.project_id,p.project_nm,e.subject_area,e.metric_nm,e.dm_dimension,e.check_pnt_layer,e.target_schema,e.target_table,e.target_clmns,e.source_schema,e.source_table,e.source_clmns,e.target_sql,e.source_sql,e.last_upd_by_user_id,e.last_upd_timestamp,e.job_type,e.rule_thrsld,e.rule_range_max,e.rule_range_min,e.execution_group,e.effective_date,e.is_active,e.is_custom_sql,e.modifications_comments from {environment}_dwdb.dqm.ent_dq_entity_rule_map e join {environment}_dwdb.dqm.dim_dq_rule_master m on m.master_rule_id=e.master_rule_id join {environment}_dwdb.dqm.dim_dq_project_master p on p.project_id=e.project_id where e.is_active='y' AND m.is_active='y' AND  entity_rule_id in ('"+str(rule_id)+"') order by e.entity_rule_id"
    #result_set = db_ops.fetch_redshift_result(fetch_query,'etluser_dqm',header = False)
    result_set = db_ops.fetch_snowflake_result(fetch_query,'SVC_DQM',header = False)
    print(result_set)
    log_stat("Rule info:")
    log_stat(str(result_set))
    return result_set[0]

def list_convert_to_df(final_result_list,fact_table):
    if fact_table == "fact_dq_metric":
        fact_df = pd.DataFrame(final_result_list, columns = ['record_id','dq_date_id','entity_rule_id','dq_entity','master_rule_id','rule_nm','qual_dimension_id','qual_dimension_nm','project_id','project_nm','subject_area','metric_nm','dm_dimension','dm_dimension_val','check_pnt_layer','target_schema','target_table','target_clmns','source_schema','source_table','source_clmns','target_total','source_total','diff_total','dq_result','is_reprocessed','last_upd_by_user_id','last_upd_timestamp','target_sql','source_sql','issue_created_timestamp'])
        return fact_df
    if fact_table == "fact_dq_validation":
        fact_df = pd.DataFrame(final_result_list, columns = ['record_id','dq_date_id','entity_rule_id','dq_entity','master_rule_id','rule_nm','qual_dimension_id','qual_dimension_nm','project_id','project_nm','subject_area','metadata_nm','metadata_val','vldtn_rslt_count','dq_result','last_upd_by_user_id','last_upd_timestamp','target_sql','source_sql','issue_created_timestamp'])
        return fact_df  
        
def result_threshold_check(rule_type,rule_thrsld,rule_range_max,rule_range_min,final_total):
    dq_result="Passed";
    rule_validity_flag="y";
    if (rule_thrsld is None) or (rule_thrsld == ""): ###############Need to check if this expression works
        var_thrsld = 0    #set var_thrsld to 0 if rule_thrsld is null
    else:
        var_thrsld = rule_thrsld    #get threshold value if rule_thrsld is not null
    if (final_total is None) or (final_total == ""):
        var_target_total=0;    #set var_target_total to 0 if target_total is null
    else:
        var_target_total= final_total    #get threshold value if target_total is not null
    log_stat("Rule Type: "+rule_type)
    if rule_type == "Unique Constraint / Duplicate":    #Check if rule_type is duplicate check    
        if var_target_total > var_thrsld:
            dq_result = "Failed"
    elif rule_type == "Reference Check" :                #check if rule_type is reference check
        if var_target_total > var_thrsld:
            dq_result = "Failed"
    elif rule_type == "Data Integrity Constraint":        #check if rule_type is data integrity check
        if var_target_total > var_thrsld:
            dq_result = "Failed"
    elif rule_type ==  "Non-Zero Constraint" :            #check if rule_type is non zero check
        if var_target_total > var_thrsld:
            dq_result = "Failed"
    elif rule_type ==  "Special Characters Check" :        #check if rule_type is special characters check
        if var_target_total > var_thrsld:
            dq_result = "Failed"
    elif rule_type ==  "Lookup-Value check":            #check if rule_type is lookup value check
        if var_target_total > var_thrsld:
            dq_result = "Failed"
    elif rule_type ==  "Field Matching" :                #check if rule_type is field matching check
        if var_target_total > var_thrsld:
            dq_result ="Failed"
    elif rule_type ==  "Business Rule Check" :            #check if rule_type is business rule check
        if var_target_total > var_thrsld:
            dq_result="Failed"
    elif rule_type == "Mandatory Constraint" :            #check if rule_type is mandatory constraint check
        if var_target_total > var_thrsld:
            dq_result="Failed"
    elif rule_type == "Trend Deviation Check" :            #check if rule_type is trend deviation check
        if var_target_total > var_thrsld:
            dq_result="Failed"
    elif rule_type == "Range Constraint":                #check if rule_type is range comparison
        if(rule_range_min is not None) or (rule_range_max is not None):
            if (var_target_total < rule_range_min) or var_target_total > rule_range_max:
                dq_result="Failed"
        else:
            log_stat("There is no range defined: "+rule_type)
    else:
        if var_target_total > var_thrsld:
            dq_result="Failed"                #if difference is not null and greater than 0 set dq result 'Failed'
    return dq_result
    
def source_target_threshold_check(rule_type,rule_thrsld,rule_range_max,rule_range_min,difference_total,source_total,target_total):
    dq_result="Passed";
    if (rule_thrsld is None) or (rule_thrsld == ""): 
        var_thrsld = 0    #set var_thrsld to 0 if rule_thrsld is null
    else:
        var_thrsld = rule_thrsld    #get threshold value if rule_thrsld is not null
    if rule_type == "Value Comparision":    #if Value Comparision    
        if difference_total > var_thrsld:
            dq_result = "Failed"
        else:
            log_stat("There is no Threshold defined for #dq entity : "+rule_type)
    elif rule_type == "Reconciliation Break":    #if Reconciliation Break    
        if difference_total > var_thrsld:
            dq_result = "Failed"
    elif rule_type == "Business Rule Check" :    #if Business Rule Check
        if target_total > var_thrsld:
            dq_result = "Failed"                #if target total is greater than var_thrsld set dq result 'Failed'
    elif rule_type == "Row Count" :                #if Row Count
        if difference_total > var_thrsld:
            dq_result = "Failed"                #if difference  is  greater than 0 set dq result 'Failed'
    elif rule_type == "Range Constraint" :        #if Range Constraint
        if ((rule_range_min is not None) or (rule_range_min != "")) or ((rule_range_max is not None) or (rule_range_max != "")): #check if diffenece is not falling in range provided
            if (difference_total < rule_range_min) or (difference_total > rule_range_max):
                dq_result = "Failed"
        else:
            log_stat("There is no Range defined for #dq entity : "+rule_type)    
    elif rule_type == "Percent Comparision" :    #if Percent Comparision
        if difference_total > (float(source_total) * float(var_thrsld) / 100):
            dq_result = "Failed"                #if difference  doesnt fall under certain threshold percentage of source toal set dq result to 'Failed'
    else:
        if difference_total > var_thrsld:        #if difference is not null and greater than 0 set dq result 'Failed'
            dq_result = "Failed"
            log_stat("This rule is not defined in rule master for #dq entity : "+rule_type)
    return dq_result    

def dq_rule_execution(rule_id):
    snowflake_credentials = get_secret(SNOWFLAKE_SECRET_NAME, aws_region)

    log_stat("Rule execution started for : "+str(rule_id))
    cur_dat = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    cur_dat_dq = datetime.now(pytz.timezone('America/New_York')).strftime('%Y%m%d')
    return_cd = 0

    snowflake_database = snowflake_credentials['database'].strip()
    snowflake_account = snowflake_credentials['host'].strip()
    snowflake_user_name = snowflake_credentials['username'].strip()
    snowflake_user_password = snowflake_credentials['password'].strip()
    snowflake_role = snowflake_credentials['role'].strip()

    sf_config = {
        "database": snowflake_database,
        "account": snowflake_account,
        "user": snowflake_user_name,
        "password": snowflake_user_password,
        "warehouse": 'WH_DQM_TRANSFORMING_XS',
        "role": snowflake_role,
        "schema": 'DQM'
      }
    try:
        rule_data = get_rule_map_entry(rule_id)
        if not rule_data :
            cur_dat_time = cur_dttm()
            log_stat("No Table for enrty in rule map table at: "+str(cur_dat_time))
            is_rule = "n"
            return_cd = 1
            sys.exit(0)
        
        else:
            dq_date_id = int(cur_dat_dq)
            entity_rule_id = int(rule_data[0])
            dq_entity = rule_data[1].lower()
            master_rule_id = int(rule_data[2])
            rule_type = rule_data[3]
            entity_rule_nm = rule_data[4]
            qual_dimension_id = int(rule_data[5])
            qual_dimension_nm = rule_data[6]
            project_id = int(rule_data[7])
            project_nm = rule_data[8]
            subject_area = rule_data[9].lower()
            metric_nm = rule_data[10]
            dm_dimension = rule_data[11].lower()
            check_pnt_layer = rule_data[12]
            target_schema = rule_data[13]
            target_table = rule_data[14]
            target_clmns = rule_data[15]
            source_schema = rule_data[16]
            source_table = rule_data[17]
            source_clmns = rule_data[18]
            target_sql = rule_data[19]
            source_sql = rule_data[20]
            last_upd_by_user_id = rule_data[21]
            last_upd_timestamp = rule_data[22]
            job_type = rule_data[23]
            rule_thrsld = rule_data[24]
            rule_range_max = rule_data[25]
            rule_range_min = rule_data[26]
            execution_group = rule_data[27]
            effective_date = rule_data[28]
            is_active = rule_data[29]
            is_custom_sql = rule_data[30]
            modifications_comments = rule_data[31]
            is_rule = "y"
            source_total = None
            diff_total = None
            is_reprocessed = "n"
            last_upd_by_user_id = "etluser_dqm"
            #last_upd_timestamp
            
            
            log_stat("DQ Entity : "+dq_entity)
            ###############################################snowflake###############################################
            final_result_list = []
            if job_type == "snowflake":
                threshold_check = "y"
                fact_table = "fact_dq_metric"
                target_exect_sql = "select coalesce(rule_id,0) as rule_id,coalesce(metric_nm,'others'), coalesce(dm_dimension_val,'n/a') ,total from (select "+str(rule_id)+" as rule_id ) dummy left join ("+target_sql+") on 1=1"
                dq_result_set = db_ops.fetch_snowflake_result(target_exect_sql,'SVC_DQM',header = False)
                #data = db_ops.fetch_snowflake_result_df(v_sql,'SVC_DQM', None, 'WH_DQM_TRANSFORMING_XS')
                #print("dq_result_set-------")
                #print(dq_result_set)
                for row in dq_result_set:
                    if (row[3] == '') or (row[3] is None):
                        row[3] = 0
                for dat in dq_result_set:
                    final_id = dat[0]
                    final_metric_nm = dat[1].lower()
                    final_dm_dimension_val = dat[2].lower()
                    final_total = int(dat[3])
                    final_dq_result = result_threshold_check(rule_type,rule_thrsld,rule_range_max,rule_range_min,final_total)
                    dat.append(final_dq_result)
                    t=datetime.now(pytz.timezone('America/New_York'))
                    final_time = str(calendar.timegm(t.timetuple())) + str(rule_id)
                    record_id = int(final_time)
                    dat.append(record_id)
                dateTimeObj_final = datetime.now(pytz.timezone('America/New_York'))
                dateTimeObj_final = dateTimeObj_final.strftime("%Y-%m-%d %H:%M:%S")
                issue_created_timestamp = dateTimeObj_final
                for i in dq_result_set:
                        final_result_list.append((i[5],dq_date_id,entity_rule_id,dq_entity,master_rule_id,entity_rule_nm,qual_dimension_id,qual_dimension_nm,project_id,project_nm,subject_area,i[1],dm_dimension,i[2],check_pnt_layer,target_schema,target_table,target_clmns,source_schema,source_table,source_clmns,i[3],source_total,diff_total,i[4],is_reprocessed,last_upd_by_user_id,dateTimeObj_final,target_sql,source_sql,issue_created_timestamp))
                final_df = list_convert_to_df(final_result_list,fact_table)
                final_df['target_sql'] = final_df['target_sql'].str.replace('\'','')
                final_df['source_sql'] = final_df['source_sql'].str.replace('\'','')
                final_df = final_df.convert_dtypes()
                import pandas as pd
                pd.set_option('display.max_colwidth', None)
                print(final_df)
                con = snowflake_conn(sf_config)
                insert_df_to_snowflake(final_df,fact_table,con)
                con.close()
                return return_cd
            ###############################################snowflake_vldtn###############################################
            if job_type == "snowflake_vldtn":
                threshold_check = "n"
                fact_table = "fact_dq_validation"
                target_exect_sql = "select coalesce(rule_id,0), coalesce(metadata_nm,'others') ,coalesce(metadata_val,'n/a'), null as vldtn_rslt_count ,coalesce(dq_result,'Passed') from (select "+str(rule_id)+" as rule_id  ) dummy left join ("+target_sql+") tgt on 1=1"
                dq_result_set = db_ops.fetch_snowflake_result(target_exect_sql,'SVC_DQM',header = False)
                print(dq_result_set)
                for dat in dq_result_set:
                    final_id = dat[0]
                    final_metadata_nm = dat[1].lower()
                    final_dm_dimension_val = dat[2].lower()
                    vldtn_rslt_count = dat[3]
                    final_dq_result = dat[4]
                    t=datetime.now(pytz.timezone('America/New_York'))
                    final_time = str(calendar.timegm(t.timetuple())) + str(rule_id)
                    record_id = int(final_time)
                    dat.append(record_id)
                dateTimeObj_final = datetime.now(pytz.timezone('America/New_York'))
                dateTimeObj_final = dateTimeObj_final.strftime("%Y-%m-%d %H:%M:%S")
                issue_created_timestamp = dateTimeObj_final
                for i in dq_result_set:
                        final_result_list.append((i[5],dq_date_id,entity_rule_id,dq_entity,master_rule_id,entity_rule_nm,qual_dimension_id,qual_dimension_nm,project_id,project_nm,subject_area,i[1].lower(),i[2].lower(),i[3],i[4],last_upd_by_user_id,dateTimeObj_final,target_sql,source_sql,issue_created_timestamp))
                print(final_result_list)
                final_df = list_convert_to_df(final_result_list,fact_table)
                final_df['target_sql'] = final_df['target_sql'].str.replace('\'','')

                final_df = final_df.convert_dtypes()

                print(final_df)
                try:                
                    con = snowflake_conn(sf_config)
                    insert_df_to_snowflake(final_df,fact_table,con)
                    con.close()
                except Exception as e:
                    log_stat("Exception to execute the DQ rule - "+str(e))
                    traceback.print_exc()
                return return_cd
            ###############################################snowflake_snowflake###############################################    
            if job_type == "snowflake_snowflake":
                threshold_check = "y"
                fact_table = "fact_dq_metric"
                final_list_of_list = []
                source_exect_sql = "select coalesce(rule_id,0) as rule_id,coalesce(metric_nm,'others'), coalesce(dm_dimension_val,'n/a') ,total from (select "+str(rule_id)+" as rule_id ) dummy left join ("+source_sql+") src on 1=1"
                target_exect_sql = "select coalesce(rule_id,0) as rule_id,coalesce(metric_nm,'others'), coalesce(dm_dimension_val,'n/a') ,total from (select "+str(rule_id)+" as rule_id ) dummy left join ("+target_sql+") tgt on 1=1" 
                source_result_set = db_ops.fetch_snowflake_result(source_exect_sql,'SVC_DQM',header = False)
                target_result_set = db_ops.fetch_snowflake_result(target_exect_sql,'SVC_DQM',header = False)
                source_result_set = sorted(source_result_set)
                target_result_set = sorted(target_result_set)
                for row in source_result_set:
                    if (row[3] == '') or (row[3] is None):
                        row[3] = 0
                for row in target_result_set:
                    if (row[3] == '') or (row[3] is None):
                        row[3] = 0     
                for s, t in zip(source_result_set, target_result_set):
                    var_diff_total =  s[3] - t[3]
                    target_total = t[3]
                    source_total = s[3]
                    final_dq_result = source_target_threshold_check(rule_type,rule_thrsld,rule_range_max,rule_range_min,var_diff_total,source_total,target_total)
                    t=datetime.now(pytz.timezone('America/New_York'))
                    final_time = str(calendar.timegm(t.timetuple())) + str(rule_id)
                    record_id = int(final_time)
                    final_list_of_list.append([s[0],s[1],s[2],s[3],target_total,var_diff_total,final_dq_result,record_id])
                dateTimeObj_final = datetime.now(pytz.timezone('America/New_York'))
                dateTimeObj_final = dateTimeObj_final.strftime("%Y-%m-%d %H:%M:%S")
                issue_created_timestamp = dateTimeObj_final
                for i in final_list_of_list:
                        final_result_list.append((i[7],dq_date_id,entity_rule_id,dq_entity,master_rule_id,entity_rule_nm,qual_dimension_id,qual_dimension_nm,project_id,project_nm,subject_area,i[1],dm_dimension,i[2],check_pnt_layer,target_schema,target_table,target_clmns,source_schema,source_table,source_clmns,i[4],i[3],i[5],i[6],is_reprocessed,last_upd_by_user_id,dateTimeObj_final,target_sql,source_sql,issue_created_timestamp))
                final_df = list_convert_to_df(final_result_list,fact_table)
                final_df['target_sql'] = final_df['target_sql'].str.replace('\'','')
                final_df['source_sql'] = final_df['source_sql'].str.replace('\'','')
                final_df = final_df.convert_dtypes()
                con = snowflake_conn(sf_config)
                insert_df_to_snowflake(final_df,fact_table,con)
                con.close()
                return return_cd
    except Exception as e:
        log_stat("Exception to execute the DQ rule - "+str(e))
        traceback.print_exc()
        return_cd = 1
        return return_cd
