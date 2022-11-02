# Set up logging
from email import message
import json
import os
from datetime import datetime, timedelta, date
import utilities
import psycopg2
from structlog import get_logger


log = get_logger()

# Import Boto 3 for AWS Glue
import boto3
from botocore.exceptions import ClientError

client = boto3.client("glue")

# Variables for the job:
glueJobName = "maa-fund-profile-transformation"
ENV = os.getenv("SysLevel")
lambda_name = "MADBGenericTrigger"

session = boto3.session.Session()
aws_region = session.region_name
sm_client = boto3.client("secretsmanager")
glue_client = boto3.client("glue")

aurora_details = utilities.retrieve_aurora_details(sm_client, aws_region, ENV)
TABLE_NAME = "etl_process_audit"
PG_USER = aurora_details["user"]
PG_PASSWORD = aurora_details["pass"]
PG_HOST = aurora_details["host"]
PG_DATABASE = aurora_details["database"]

# Dictionary of allowed file types and the corrisponding glue job names
# Add new glue jobs here
GLUE_JOBS_DICT = {
    "FP": "maa-fund-profile-transformation",
    "RISK_ANALYTICS": "maa-taxable-risk-metric-transformation",
    "FUND_VALUATION": "maa-fund-valuation",
    "GNE": ["gne-calc-loader", "GNE-rmg_der_risk_gne_positions_loader"],
    "GN2_RECON": ["GN2-rmg_der_risk_gne_inbound_recon_loader"],
    "GN2_VAR": ["GN2-rmg_der_risk_gne_inbound_var_loader"],
    "GN2_BACKTESTING": "GNE-GN2-GLUE-WORKFLOW",
    "FUND_RETURN": "maa-fund-return"
}


def create_new_log(
    trigger_key: str,
    lambda_name: str,
    message_dict: dict,
    glue_job_args: dict,
    is_lambda_success: bool,
    glue_job_name: str,
    message: str = "",
):
    """
    The logs for the request_id does not exist yet! So, we are adding it to the audit table
    """
    if message_dict["file_type"] == "FP":
        data_component = "FUND_PROFILE"
    else:
        data_component = message_dict["file_type"]

    #glue_job_args = json.dumps(glue_job_args, indent=4)  # converting dict to json
    
    if "--glueJobData" in glue_job_args:
        glue_job_args["--glueJobData"] = json.loads(glue_job_args["--glueJobData"].replace("'",'"'))[0]
    glue_job_args = json.dumps(glue_job_args, indent=4)
    trigger_key = json.dumps(
        trigger_key
    )  # needs to be converted since it is being passed as a dict
    message = message.replace("'", '"')
    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )
    with conn:
        cursor = conn.cursor()
        query = f"""INSERT INTO {TABLE_NAME}(trigger_key, lambda_name, glue_job_args, data_component, data_type, is_lambda_success, message, lambda_timestamp, target_glue_job)
                    VALUES ('{trigger_key}', '{lambda_name}', '{glue_job_args}', '{data_component}' ,'FUND', '{is_lambda_success}', '{message}', '{str(datetime.now())}', '{glue_job_name}')"""
        cursor.execute(query)


def get_blackrock_port_ids(file_key: str) -> str:
    """
    There are around 277 portfolios in MADB currently with valid BRS Ticker Code,
    since there are 7 triggers for gp-avro files, use base of 40 will cover all 277 portfolios
    assuming all of them have valid gp avro file
    """
    port_ids_list_base = 40

    # For file key 'gp_avro-000001_0', following will return value 1
    port_ids_pos_multiplier = int(file_key[13:14])
    db_conn = psycopg2.connect(
        host=PG_HOST, database=PG_DATABASE, user=PG_USER, password=PG_PASSWORD
    )
    with db_conn:
        cursor = db_conn.cursor()
        brs_port_ids_query = f"""select blackrock_port_id from fund_identifiers where blackrock_port_id is not null order by blackrock_port_id desc"""
        cursor.execute(brs_port_ids_query)
        port_id_list = cursor.fetchall()
        port_id_list_filtered = []
        # for each gp avro triggers, it will only get port ids starting from 40*(0,1,2,3,4,5,6)
        # and end on 40*(0,1,2,3,4,5,6) + 39
        sliced_port_id_list = port_id_list[
            port_ids_pos_multiplier
            * port_ids_list_base : port_ids_pos_multiplier
            * port_ids_list_base
            + 39
        ]
        for row in sliced_port_id_list:
            if row[0] not in port_id_list_filtered:
                port_id_list_filtered.append(row[0])
        if len(port_id_list_filtered) > 0:
            brs_port_ids = ",".join(port_id_list_filtered)
        else:
            brs_port_ids = None
        return brs_port_ids


def trigger_glue_job(job_name, job_args, trigger_key, message_dict):
    try:
        response = client.start_job_run(JobName=job_name, Arguments=job_args)
        log.msg(
            "Successfully triggered glue job",
            job_name=job_name,
            job_run_id=response["JobRunId"],
            arguments=job_args,
            state="SUCCEEDED",
        )
        success_message = f"Successfully submitted the task to Glue Job with ID: {response['JobRunId']}"
        create_new_log(
            trigger_key,
            lambda_name,
            message_dict,
            glue_job_args=job_args,
            is_lambda_success=True,
            glue_job_name=job_name,
            message=success_message,
        )
    except Exception as e:
        log.exception(
            "Failed to trigger glue job",
            job_name=job_name,
            e=e,
            arguments=job_args,
            state="FAILED",
        )
        failure_message = f"Failed to submit to Glue Job with exception: {e}"
        create_new_log(
            trigger_key,
            lambda_name,
            message_dict,
            glue_job_args=job_args,
            is_lambda_success=False,
            glue_job_name=job_name,
            message=failure_message,
        )
    return response


def start_glue_workflow(workflow_name, run_args):
    try:
        response = client.start_workflow_run(Name=workflow_name, RunProperties=run_args)
        log.msg(
            "Successfully started glue workflow",
            workflow_name=workflow_name,
            run_id=response["RunId"],
            arguments=run_args,
            state="SUCCEEDED",
        )
        return response
    except ClientError as e:
        raise Exception("boto3 client error in start_glue_workflow: " + e.__str__())
    except Exception as e:
        raise Exception("Unexpected error in start_glue_workflow: " + e.__str__())


def lambda_handler(event, context):
    body = []
    messages = []
    log.msg("This is the event", trigger=event)
    os.environ.update(json.loads(os.environ.get("CustomEnvironment", "{}")))
    env = os.getenv("sysLevel")
    crnt_date = datetime.today()
    time_delta = timedelta(hours=-5)
    current_date = crnt_date + time_delta
    effective_date = current_date.strftime("%Y-%m-%d")
    effective_datetime = current_date.strftime("%Y%m%d-%H:%M:%S")
    file_type_key = "file_type"
    log.msg("Found sys level to be", sys_level=env)
    for record in event["Records"]:
        body_sqs = json.loads(record["body"])
        body.append(body_sqs)
        messages.append(json.loads(body_sqs["Message"]))
    log.msg("This is the event", trigger=event, body=body, messages=messages)
    message_dict = messages[0]
    log.msg("This is the message event", message_dict)
    run_histo = message_dict.get("run_histo")
    if run_histo == None:
        run_histo = "false"
    trigger_key = (effective_datetime, run_histo)
    if message_dict.get("detail") != None:
        if "priceTypeCode" in message_dict.get("detail").get("content").get("payload").get("udf").get("data")[0]:
            priceTypeCode = message_dict.get("detail").get("content").get("payload").get("udf").get("data")[0].get("priceTypeCode")
            eventType = message_dict.get("detail").get("content").get("payload").get("udf").get("data")[0].get("eventType")
            glue_job_data = message_dict.get("detail").get("content").get("payload").get("udf").get("data")
    else: 
        priceTypeCode == None
        eventType == None
        glue_job_data == None
        log.msg("This is the Normal event")
    

    if file_type_key in message_dict and message_dict[file_type_key] in GLUE_JOBS_DICT:
        if message_dict[file_type_key] == "FP":
            glue_job_name = GLUE_JOBS_DICT[message_dict[file_type_key]]
            glue_job_args = {
                "--effective_date": effective_date,
                "--trigger_time": effective_datetime,
                "--run_histo": run_histo,
            }
            response = trigger_glue_job(
                glue_job_name, glue_job_args, trigger_key, message_dict
            )

        elif message_dict[file_type_key] in ["GN2_VAR", "GNE"]:
            glue_job_names = GLUE_JOBS_DICT[message_dict[file_type_key]]
            effective_date = message_dict["effective_date"]
            glue_job_args = {"--effective_date": effective_date}

            for job in glue_job_names:
                response = trigger_glue_job(
                    job, glue_job_args, trigger_key, message_dict
                )

        elif message_dict[file_type_key] == "GN2_RECON":
            glue_job_names = GLUE_JOBS_DICT[message_dict[file_type_key]]
            effective_date = message_dict["effective_date"]
            batch_id = message_dict["batch_id"]
            glue_job_args = {"--effective_date": effective_date, "--batch_id": batch_id}

            for job in glue_job_names:
                response = trigger_glue_job(
                    job, glue_job_args, trigger_key, message_dict
                )

        elif message_dict[file_type_key] == "GN2_BACKTESTING":
            glue_workflow_name = GLUE_JOBS_DICT[message_dict[file_type_key]]
            backtesting_effective_date = message_dict["effective_date"]
            backtesting_effective_date_minus_one = message_dict[
                "effective_date_minus_one"
            ]
            glue_run_args = {
                "effective_date": backtesting_effective_date,
                "effective_date_minus_one": backtesting_effective_date_minus_one,
            }
            response = start_glue_workflow(glue_workflow_name, glue_run_args)

        elif message_dict[file_type_key] == "RISK_ANALYTICS":
            lastBusDay = current_date
            if date.weekday(current_date) < 1:  # if it's Monday
                lastBusDay = lastBusDay - timedelta(days=3)  # then make it Friday
            else:
                lastBusDay = lastBusDay - timedelta(days=1)

            previous_day_effective_date = lastBusDay.strftime("%Y%m%d")
            log.msg(f"Previous Day Date: {previous_day_effective_date}")
            current_effective_date = current_date.strftime("%Y%m%d")
            log.msg(f"Current Day Date: {current_effective_date}")
            file_key = message_dict["file_key"]
            log.msg(f"RISK_ANALYTICS File Type: {file_key}")
            glue_job_name = GLUE_JOBS_DICT[message_dict[file_type_key]]

            if file_key in ["adx_a", "via_adx_a_global", "via_adx_a_dom", "sm_weekly"]:
                glue_job_args = {
                    "--effective_date": current_effective_date,
                    "--file_key": file_key,
                    "--run_histo": run_histo,
                    "--brs_port_ids": "NA",
                }

            else:
                if file_key in [
                    "gp_avro-000000_0",
                    "gp_avro-000001_0",
                    "gp_avro-000002_0",
                    "gp_avro-000003_0",
                    "gp_avro-000004_0",
                    "gp_avro-000005_0",
                    "gp_avro-000006_0",
                ]:
                    brs_port_ids = get_blackrock_port_ids(file_key)
                    if not brs_port_ids:
                        brs_port_ids = "NA"
                else:
                    brs_port_ids = "NA"

                glue_job_args = {
                    "--effective_date": previous_day_effective_date,
                    "--file_key": file_key,
                    "--run_histo": run_histo,
                    "--brs_port_ids": brs_port_ids,
                }

            response = trigger_glue_job(
                glue_job_name, glue_job_args, trigger_key, message_dict
            )

        elif message_dict[file_type_key] == "FUND_VALUATION":
            tna_flag = message_dict["skipTna"]
            glue_job_name = GLUE_JOBS_DICT[message_dict[file_type_key]]
            glue_job_args = {"--skipTna": tna_flag}
            response = trigger_glue_job(
                glue_job_name, glue_job_args, trigger_key, message_dict
            )

    
        elif message_dict[file_type_key] == "FUND_RETURN":
            glue_job_name = GLUE_JOBS_DICT[message_dict[file_type_key]]
            glue_job_args = {}
            response = trigger_glue_job(
                glue_job_name, glue_job_args, trigger_key, message_dict
            )

        # Add new glue jobs here

        elif message_dict[file_type_key] == "FUND_VALUATION":
            log.msg("Executing fund valuation cron")
            tna_flag = message_dict["skipTna"]
            glue_job_name = GLUE_JOBS_DICT[message_dict[file_type_key]]
            glue_job_args = {
                "--skipTna": tna_flag    
            }
            response = trigger_glue_job(glue_job_name, glue_job_args, trigger_key, message_dict)

    elif priceTypeCode == "NAV" and eventType == "UPDATE":
        log.msg("PriceTypeCode is NAV and type is Update")
        nav_flag = "N"
        tna_flag = "Y"
        #glue_job_name = GLUE_JOBS_DICT["FUND_VALUATION"]
        glue_job_name = "maa-fund-valuation"
        message_dict[file_type_key] = "FUND_VALUATION"
        glue_job_args = {
            "--skipTna": tna_flag,
            "--skipNav": nav_flag,
            "--glueJobData":str(glue_job_data)
        }
        response = trigger_glue_job(glue_job_name, glue_job_args, trigger_key, message_dict)

    else:
        log.msg("File type is incorrect, cannot proceed", file_type=event["file_type"])
    return response
