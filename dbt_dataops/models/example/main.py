import pytz
import datetime
import sys
import random
import traceback
from datetime import datetime
from pytz import timezone
from pathlib import Path
import rule_execution as reexe

# Common paths 
path_home=str(Path.home())
common_path = Path(__file__).resolve().parent.parent / 'commons'
sys.path.insert(0, str(common_path))
print(f"Common Path: {common_path}")  

#Custom module
from constants import environment 
import db_utils as db_ops
import nrt_slack_notification

def cur_dttm():
    cur_dat_time = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
    return cur_dat_time

def upd_brdg_pross_obj_health(process_id, objectid, run_date, status, dq_status, upd_dq_status):
    cur_dat_time = cur_dttm()
    v_sql = "UPDATE abac.brdg_process_object_health set dq_status = '"+upd_dq_status+"' where process_id = "+str(process_id)+" and objectid = "+str(objectid)+" and run_date = '"+str(run_date)+"' and status = '"+status+"' and dq_status = '"+dq_status+"'"
    db_ops.execute_abac_query(v_sql,'etluser_dqm')


def ins_log_table(process_rec_id, process_id, objectid, run_date, schema_name, table_name, abac_status, tbl_db_name, tbl_db_type, pipeline_type, task_id, exe_start_dttm):
    cur_dat_time = cur_dttm()
    v_sql = "INSERT INTO dqm.dq_nrt_rule_execution_log ( process_record_id, abac_process_id, abac_objectid, run_date, schema_name, table_name, abac_status, tbl_db_name,  tbl_db_type, pipeline_type, process_task_id, exe_start_dttm, insert_user, insert_dttm ) VALUES ( "+process_rec_id+", "+str(process_id)+", "+str(objectid)+", '"+str(run_date)+"', '"+schema_name+"', '"+table_name+"', '"+abac_status+"', '"+tbl_db_name+"', '"+tbl_db_type+"', '"+pipeline_type+"', "+str(task_id)+", '"+exe_start_dttm+"', 'etl_user', '"+cur_dat_time+"')"
    db_ops.execute_abac_query(v_sql,'etluser_dqm')


def upd_log_table(rule_exe, succ_rule_exe, fail_rule_exe, process_status, comments, excp_msg, exe_end_dttm, process_rec_id, process_id, objectid, run_date, schema_name, table_name):
    cur_dat_time = cur_dttm()
    v_sql = "UPDATE dqm.dq_nrt_rule_execution_log set no_rule_executed = "+str(rule_exe)+", no_rule_execution_success = "+str(succ_rule_exe)+", no_rule_execution_failed = "+str(fail_rule_exe)+", process_status = '"+process_status+"', comments = '"+comments+"', exception_message = '"+excp_msg+"', exe_end_dttm = '"+str(exe_end_dttm)+"', last_upd_user = 'etl_user', last_upd_dttm = '"+cur_dat_time+"' where process_record_id = "+str(process_rec_id)+" and abac_process_id = "+str(process_id)+" and abac_objectid = "+str(objectid)+" and run_date = '"+str(run_date)+"' and schema_name = '"+schema_name+"' and table_name = '"+table_name+"'"

    db_ops.execute_abac_query(v_sql,'etluser_dqm')
    
def get_view_table(cur_dat):
    v_sql = "SELECT process_id, objectid, run_date, schema_name, object_name, status, dq_status, db_id, db_name, db_type, pipeline_type from ( SELECT vw.process_id, vw.objectid, vw.run_date, vw.schema_name, vw.object_name, vw.status, vw.dq_status, vw.db_id, vw.db_name, db.db_type, vw.pipeline_type, vw.updated_date from abac.vw_process_object_health vw left join dt_ops.dt_db_defn db on vw.db_id = db.db_id where lower(vw.dq_status) = 'pending' and lower(vw.status) in ('completed') and lower(db.db_type) = 'snowflake' and (vw.process_id, vw.objectid, vw.db_id, vw.db_name, db.db_type) not in (select abac_process_id, abac_objectid, tbl_db_id, tbl_db_name, tbl_db_type from dqm.dq_nrt_table_control where execution_flag = 'n') and (vw.schema_name, vw.db_name, db.db_type, vw.object_name) not in (select schema_name, tbl_db_name, tbl_db_type, table_name from dqm.dq_nrt_rule_execution_log where process_status = 'In Progress' and run_date = '"+cur_dat+"') and vw.run_date = '"+cur_dat+"'  union SELECT vw.process_id, vw.objectid, vw.run_date, vw.schema_name, vw.object_name, vw.status, vw.dq_status, vw.db_id, vw.db_name, db.db_type, vw.pipeline_type, vw.updated_date from abac.vw_process_object_health vw left join dt_ops.dt_db_defn db on vw.db_id = db.db_id where lower(vw.dq_status) = 'pending' and lower(vw.status) in ('failed') and lower(db.db_type) = 'snowflake' and (vw.process_id, vw.objectid, vw.db_id, vw.db_name, db.db_type) not in (select abac_process_id, abac_objectid, tbl_db_id, tbl_db_name, tbl_db_type from dqm.dq_nrt_table_control where execution_flag = 'n') and (vw.schema_name, vw.db_name, db.db_type, vw.object_name) not in (select schema_name, tbl_db_name, tbl_db_type, table_name from dqm.dq_nrt_rule_execution_log where process_status = 'In Progress' and run_date = '"+cur_dat+"') and vw.run_date = '"+cur_dat+"' and hour(vw.updated_date) < 20 and TIMESTAMPDIFF(hour,updated_date, CONVERT_TZ(current_timestamp,'UTC','US/Eastern')) >= 4  union SELECT vw.process_id, vw.objectid, vw.run_date, vw.schema_name, vw.object_name, vw.status, vw.dq_status, vw.db_id, vw.db_name, db.db_type, vw.pipeline_type, vw.updated_date from abac.vw_process_object_health vw left join dt_ops.dt_db_defn db on vw.db_id = db.db_id where lower(vw.dq_status) = 'pending' and lower(vw.status) in ('failed') and lower(db.db_type) = 'snowflake' and (vw.process_id, vw.objectid, vw.db_id, vw.db_name, db.db_type) not in (select abac_process_id, abac_objectid, tbl_db_id, tbl_db_name, tbl_db_type from dqm.dq_nrt_table_control where execution_flag = 'n') and (vw.schema_name, vw.db_name, db.db_type, vw.object_name) not in (select schema_name, tbl_db_name, tbl_db_type, table_name from dqm.dq_nrt_rule_execution_log where process_status = 'In Progress' and run_date = '"+cur_dat+"') and vw.run_date = '"+cur_dat+"' and hour(vw.updated_date) >= 20 ) dat order by updated_date  "
    data = db_ops.fetch_abac_result(v_sql,'etluser_dqm')
    print("View Data")
    print(data)
    print(cur_dat)
    if data == []:
        return "0"
    else:
        return data

def get_rule_ID(schema_name, table_name, tbl_db_type):
    v_sql = "SELECT rmap.entity_rule_id FROM dqm.ent_dq_entity_rule_map rmap where lower(rmap.is_active) in ('y','x') and lower(trim(rmap.target_schema)) = lower(trim('"+schema_name+"')) and lower(trim(rmap.target_table)) = lower(trim('"+table_name+"')) and execution_group not in ('DBT_TEST','DBT_TEST_SF','DFLT001') and job_type in ('snowflake_snowflake','snowflake','snowflake_vldtn') and  split_part(rmap.job_type,'_',1) = lower('"+tbl_db_type+"') order by  rmap.entity_rule_id "
    data = db_ops.fetch_snowflake_result(v_sql,'SVC_DQM',header = False)
    print(data)
    if data == []:
        return "0"
    else:
        return data
 
def get_process_record_id():
    dttme = datetime.now(pytz.timezone('America/New_York')).strftime('%y%m%d%H%M%S')
    rand = random.randrange(1,9999999999)
    gen_key_tmp = str(rand)
    gen_key = str(dttme) + str(gen_key_tmp)
    gen_key = gen_key[:18]
    return gen_key

def get_dq_score(table_name, dq_date):
    v_sql = "SELECT dq_score from dqm.vw_dq_score_table where dq_date_id = '"+dq_date+"' and target_table = lower('"+table_name+"')"
    data = db_ops.fetch_snowflake_result(v_sql,'SVC_DQM',header = False)
    if data == []:
        return "0"
    else:
        return data[0]

def upd_brdg_pross_obj_health_dq_score(process_id, objectid, run_date, status, dq_status, upd_dq_status, dq_score):
    cur_dat_time = cur_dttm()
    v_sql = "UPDATE abac.brdg_process_object_health set dq_status = '"+upd_dq_status+"', dq_score = CAST('"+dq_score+"' AS DECIMAL(6,2)), dq_run_timestamp = '"+cur_dat_time+"'  where process_id = '"+str(process_id)+"' and objectid = '"+str(objectid)+"' and run_date = '"+str(run_date)+"' and status = '"+status+"' and dq_status = '"+dq_status+"'"
    db_ops.execute_abac_query(v_sql,'etluser_dqm')
        
def daemon_table_completion_check():
    cur_dat = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d')
    cur_dat_dq = datetime.now(pytz.timezone('America/New_York')).strftime('%Y%m%d')
    process_id = ""
    objectid = ""
    run_date = ""
    schema_name = ""
    table_name = ""
    status = ""
    dq_status = ""
    db_id = ""
    tbl_db_name = ""
    tbl_db_type = ""
    pipeline_type = ""
    is_table = ""
    is_rule = ""
    excp_TAC = "NO_Exception"
    dc2_exe_group = ""
    redshift_tgt_DBHOST = ""
    return_cd = 0
    try:
        data = get_view_table(cur_dat)
        if data == "0":
            cur_dat_time = cur_dttm()
            is_table = "n"
            return_cd = 1
        else:
            for dat in data:
                print("View Data inside loop")
                print(dat)
                process_id = dat[0]
                objectid = dat[1]
                run_date = dat[2]
                print("Run Date")
                print(run_date)
                schema_name = dat[3]
                table_name = dat[4]
                status = dat[5]
                dq_status = dat[6]
                db_id = dat[7]
                tbl_db_name = dat[8]
                tbl_db_type = dat[9]
                pipeline_type = dat[10]
                is_table = "y"
                if pipeline_type == "operations":
                    is_table = ""
                    upd_dq_status = "not applicable"
                    try:
                        upd_brdg_pross_obj_health(process_id, objectid, run_date, status, dq_status, upd_dq_status)
                        print("Updated abac.brdg_process_object_health Table")
                    except Exception as e:
                        print("Exception to Update brdg_process_object_health table - "+str(e))
                        traceback.print_exc()
                
                if is_table == "y":
                    process_rec_id = get_process_record_id()
                    exe_start_dttm = cur_dttm()
                    print("Process Record ID: "+process_rec_id)
                    print("ABAC Process ID: "+str(process_id))
                    print("ABAC Object ID: "+str(objectid))
                    print("Schema Name: "+schema_name)
                    print("Table Name: "+table_name)
                    print("ABAC Status: "+status)
                    print("Table DB ID: "+str(db_id))
                    print("Table DB Name: "+tbl_db_name)
                    print("Table DB Type: "+tbl_db_type)
                    print("Table Pipeline Type: "+pipeline_type)
                    upd_dq_status = "in progress"
                    excp_msg = ""
                try:
                    upd_brdg_pross_obj_health(process_id, objectid, run_date, status, dq_status, upd_dq_status)
                    print("Updated abac.brdg_process_object_health Table")
                except Exception as e:
                    print("Exception to Update brdg_process_object_health table - "+str(e))
                    traceback.print_exc()
            
                try:
                    ins_log_table(process_rec_id, process_id, objectid, run_date, schema_name, table_name, status, tbl_db_name, tbl_db_type, pipeline_type, 9999999, exe_start_dttm)
                    print("Inserted DQ log Table")
                except Exception as e:
                    print("Exception to Insert Log - "+str(e))
                    traceback.print_exc()
            
                if db_id == 1 and tbl_db_name == "prod-dmpredshift":
                    print("DB Type: redshift DC2 --> "+tbl_db_name)
            
                try:
                    bulk_rule_ids = get_rule_ID(schema_name, table_name, tbl_db_type)
                    if bulk_rule_ids == "0":
                        is_rule = "n"
                        print("No Active DQ Rule for "+table_name)
                        exe_end_dttm = cur_dttm()
                        rule_exe = 0
                        succ_rule_exe = 0
                        fail_rule_exe = 0
                        process_status = "Completed"
                        comments = "No Active DQ Rule IDs in DQM Framework"
                        excp_msg = ""
            
                        try:
                            dq_status = "in progress"
                            upd_dq_status = "completed"
                            dq_score_temp = get_dq_score(table_name, cur_dat_dq)
                            dq_score = str(dq_score_temp[0])
                            upd_brdg_pross_obj_health_dq_score(process_id, objectid, run_date, status, dq_status, upd_dq_status, dq_score)
                            print("Updated abac.brdg_process_object_health Table")
                        except Exception as e:
                            print("Exception to Update brdg_process_object_health table - "+str(e))
                            traceback.print_exc()
            
                        try:
                            upd_log_table(rule_exe, succ_rule_exe, fail_rule_exe, process_status, comments, excp_msg, exe_end_dttm, process_rec_id, process_id, objectid, run_date, schema_name, table_name)
                            print("Updated DQ log Table")
                        except Exception as e:
                            print("Exception to Update Log for 0 Rules - "+str(e))
            
                    else:
                        active_rule_cnt = len(bulk_rule_ids)
                        print("No. of Active DQ Rules: "+str(active_rule_cnt))
                        rule_ids = ','.join(str(y) for x in bulk_rule_ids for y in x if len(x) > 0)
                        is_rule = "y"
                        print("Active DQ Rule IDs: "+rule_ids)
            
            
                except Exception as e:
                    print("Exception to Retrive Rule IDs - "+str(e))
                    
                if is_rule == "y":
                    rule_exe = 0
                    succ_rule_exe = 0
                    fail_rule_exe = 0
                    excp_rule_ID = ""
                    excp_msg = ""
                    exe_stat = 0
                    for x in bulk_rule_ids:
                        for y in x:
                            if len(x) > 0:
                                rule_id = y
                                print("Started DQ execution for Rule ID: "+str(rule_id))
                                exe_stat = reexe.dq_rule_execution(rule_id)
                                rule_exe = rule_exe + 1
                                if exe_stat == 0:
                                    succ_rule_exe = succ_rule_exe + 1
                                else:
                                    excp_TAC = "Exception"
                                    fail_rule_exe = fail_rule_exe + 1
                                    if fail_rule_exe == 1:
                                        excp_rule_ID = str(rule_id)
                                    else:
                                        excp_rule_ID = excp_rule_ID+", "+str(rule_id)
                                    excp_msg = "Execution Failed Rule IDs : "
                                    excp_Status = str(rule_id)
                                print("Ended DQ execution for Rule ID: "+str(rule_id))
                print("ExecTac")
                print(excp_TAC)
                if excp_TAC == "Exception":
                    print("Exception in DQ Process")
                    try:
                        dq_status = "in progress"
                        upd_dq_status = "pending"
                        upd_brdg_pross_obj_health(process_id, objectid, run_date, status, dq_status, upd_dq_status)
                    except Exception as e:
                        print("Exception to Update brdg_process_object_health table - "+str(e))
                        traceback.print_exc()
                    exe_end_dttm = cur_dttm()
                    process_status = "Failed"
                    comments = ""
                    excp_msg = excp_msg+excp_rule_ID
                    try:
                        upd_log_table(rule_exe, succ_rule_exe, fail_rule_exe, process_status, comments, excp_msg, exe_end_dttm, process_rec_id, process_id, objectid, run_date, schema_name, table_name)
                    except Exception as e:
                        print("Exception to Update Log for TAC exception - "+str(e))
                    return_cd = 2
                    excp_TAC = "NO_Exception"
                if excp_TAC == "NO_Exception" and is_rule == "y":
                    print("Nooooo Exception")
                    exe_end_dttm = cur_dttm()
                    process_status = "Completed"
                    comments = ""
                    excp_msg = excp_msg+excp_rule_ID
            
                    try:
                        dq_status = "in progress"
                        upd_dq_status = "completed"
                        dq_score_temp = get_dq_score(table_name, cur_dat_dq)
                        dq_score = str(dq_score_temp[0])

                        print("DQ Score :")

                        upd_brdg_pross_obj_health_dq_score(process_id, objectid, run_date, status, dq_status, upd_dq_status, dq_score)
                        print("Updated abac.brdg_process_object_health Table")
                    except Exception as e:
                        print("Exception to Update brdg_process_object_health table - "+str(e))
                        traceback.print_exc()
            
                    print("No. of Rules Executed: "+str(rule_exe))
                    print("No. of Rules Executed Successfully: "+str(succ_rule_exe))
                    print("No. of Rules Failed to Execute: "+str(fail_rule_exe))
            
                    if excp_msg != "":
                        try:
                            slack_msg = str(excp_rule_ID)+"|"+schema_name+"."+table_name
                            nrt_slack_notification.slack_notification(slack_msg)
                            print("Send Slack Notification for process failure: "+slack_msg)
                        except Exception as e:
                            print("Exception to send Slack Notification - "+str(e))
            
                    try:
                        upd_log_table(rule_exe, succ_rule_exe, fail_rule_exe, process_status, comments, excp_msg, exe_end_dttm, process_rec_id, process_id, objectid, run_date, schema_name, table_name)
                        print("Updated DQ log Table")
                    except Exception as e:
                        print("Exception to Update Log - "+str(e))
            
                    return_cd = 0
    
            return return_cd
        
    except Exception as e:
        print("Exception to Get Table from View - "+str(e))
        
if __name__ == "__main__":
    print(f"Running in {environment} environment")
    daemon_table_completion_check()
