import os, requests, json, boto3, base64, time, logging, pandas as pd, math
from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event,context):

    session = boto3.session.Session()
    s3 = session.resource('s3')

    object_prefix = event['raw_object_prefix']
    
    raw_response = client.list_objects_v2(
        Bucket = datalake_bucket['raw_bucket_name']
        ,Prefix=object_prefix + year
    )
    logger.info(raw_response['Contents'])
    
    #find the most recently uploaded object that matches the pattern
    raw_objects = pd.DataFrame(raw_response['Contents'])
    max_modified_date = raw_objects['LastModified'].max()
    
    raw_objects = raw_objects[raw_objects['LastModified'] == max_modified_date]

    
    #here's our boy
    source_object_key = raw_objects['Key'].iloc[0]
    
    s3.Bucket(source_bucket['bucket_name']).download_file(source_object_key,'/tmp/workspace_scan.parquet')
    workspace_scan = pd.read_parquet("/tmp/workspace_scan.parquet",engine='fastparquet')

  
    reports = make_report_df(workspace_scan)
    reports['meta_insert_timestamp'] = datetime_now
    logger.info("made report df")
    reports[['report_reportType', 'report_id', 'report_name', 'report_datasetId','report_modifiedDateTime', 'report_modifiedBy', 'report_modifiedById','report_createdDateTime', 'report_createdBy','report_createdById', 'workspace_id', 'workspace_name', 'report_appId','report_description','meta_insert_timestamp']].to_parquet("/tmp/pbi_reports.parquet",engine='fastparquet',index=False)
    logger.info("wrote report to parquet")
    s3.Bucket(datalake_bucket['stage_bucket_name']).upload_file("/tmp/pbi_reports.parquet","pbi_api/pbi_meta/pbi_reports/pbi_reports.parquet")
    os.remove("/tmp/pbi_reports.parquet")
    

    access = make_access_df(reports)
    access['meta_insert_timestamp'] = datetime_now
    logger.info("got access")
    access[['reportUserAccessRight', 'emailAddress', 'displayName', 'identifier','graphId', 'principalType', 'report_id','report_name', 'userType','meta_insert_timestamp']].to_parquet("/tmp/pbi_access.parquet",engine='fastparquet',index=False)
    logger.info("wrote access to parquet")
    s3.Bucket(datalake_bucket['stage_bucket_name']).upload_file("/tmp/pbi_access.parquet","pbi_api/pbi_meta/pbi_access/pbi_access.parquet")
    os.remove("/tmp/pbi_access.parquet")
    

    datasets = make_dataset_df(workspace_scan)
    datasets['meta_insert_timestamp'] = datetime_now
    logger.info("got datasets")
    datasets[['dataset_id', 'dataset_name', 'dataset_configuredBy','dataset_configuredById','dataset_createdDate', 'dataset_contentProviderType', 'dataset_users','workspace_id', 'workspace_name','dataset_refreshCadence','dataset_refreshTimes','dataset_schemaMayNotBeUpToDate', 'dataset_expressions','dataset_description', 'dataset_roles','meta_insert_timestamp']].to_parquet("/tmp/pbi_datasets.parquet",engine='fastparquet',index=False)
    logger.info("wrote datasets to parquet")
    s3.Bucket(datalake_bucket['stage_bucket_name']).upload_file("/tmp/pbi_datasets.parquet","pbi_api/pbi_meta/pbi_datasets/pbi_datasets.parquet")
    os.remove("/tmp/pbi_datasets.parquet")
    

    dataflows = make_dataflow_df(workspace_scan)
    dataflows['meta_insert_timestamp'] = datetime_now
    logger.info("got dataflows")
    dataflows[['dataflow_objectId', 'dataflow_name','dataflow_description', 'dataflow_configuredBy', 'dataflow_modifiedBy','dataflow_modifiedDateTime','dataflow_users','workspace_id','workspace_name','dataflow_refreshCadence','dataflow_refreshTimes','meta_insert_timestamp']].to_parquet("/tmp/pbi_dataflows.parquet",engine='fastparquet',index=False)
    logger.info("wrote dataflows to parquet")
    s3.Bucket(datalake_bucket['stage_bucket_name']).upload_file("/tmp/pbi_dataflows.parquet","pbi_api/pbi_meta/pbi_dataflows/pbi_dataflows.parquet")
    os.remove("/tmp/pbi_dataflows.parquet")
    

    tables = make_tables_df(datasets)
    tables['meta_insert_timestamp'] = datetime_now
    logger.info("got tables")
    tables[['name', 'measures', 'isHidden', 'column_name','column_dataType', 'column_isHidden', 'column_columnType','data_source', 'dataset_id','dataset_name', 'workspace_id', 'workspace_name', 'column_expression','description','long_source','meta_insert_timestamp']].to_parquet("/tmp/pbi_tables.parquet",engine='fastparquet',index=False)
    logger.info("wrote tables to parquet")
    s3.Bucket(datalake_bucket['stage_bucket_name']).upload_file("/tmp/pbi_tables.parquet","pbi_api/pbi_meta/pbi_tables/pbi_tables.parquet")
    os.remove("/tmp/pbi_tables.parquet")     
    

    catalog_df = make_catalog_df(reports,datasets,dataflows)
    catalog_df.to_parquet("/tmp/pbi_catalog.parquet",engine='fastparquet',index=False)
    logger.info("wrote catalog to parquet")
    s3.Bucket(datalake_bucket['stage_bucket_name']).upload_file("/tmp/pbi_catalog.parquet","pbi_api/pbi_meta/pbi_catalog/pbi_catalog.parquet")
    os.remove("/tmp/pbi_catalog.parquet")        

    workspace_scan = workspace_scan.rename(columns={'workspaceId':'workspace_id','workspaceName':'workspace_name','workspaceDescription':'workspace_description','isOnDedicatedCapacity':'workspace_premium_flag','capacityId':'workspace_capacityId'})
    workspace_scan = workspace_scan[['workspace_id','workspace_name','workspace_description','workspace_premium_flag','workspace_capacityId','meta_insert_timestamp']]
    workspace_scan.to_parquet("/tmp/pbi_workspaces.parquet",engine='fastparquet',index=False)
    s3.Bucket(datalake_bucket['stage_bucket_name']).upload_file("/tmp/pbi_workspaces.parquet","pbi_api/pbi_meta/pbi_workspaces/pbi_workspaces.parquet")
    os.remove("/tmp/pbi_workspaces.parquet")



def make_report_df(info_df):
    report_df = pd.DataFrame()
    
    for i in range(0,len(info_df)):
        
        temp_df = pd.DataFrame(info_df['reports'].iloc[i])
        
        #set the workspace id for the reports
        temp_df = temp_df.rename(mapper=lambda x:'report_' + x,axis='columns')
        temp_df['workspace_id'] = info_df['workspaceId'].iloc[i]    
        temp_df['workspace_name'] =  info_df['workspaceName'].iloc[i]
        
        report_df = pd.concat([report_df,temp_df])
    
    return report_df
    
def make_access_df(report_df):
    
    users_df = pd.DataFrame()
    
    for i in range(0,len(report_df)):
        
        if type(report_df['report_users'].iloc[i]) == list:
            
            temp_df = pd.DataFrame(report_df['report_users'].iloc[i])
            temp_df['report_id'] = report_df['report_id'].iloc[i]
            temp_df['report_name'] = report_df['report_name'].iloc[i]
            
            users_df = pd.concat([users_df,temp_df])
    return users_df
        
def make_dataset_df(info_df):
    
    dataset_df = pd.DataFrame()
    
    for i in range(0,len(info_df)):
        temp_df = pd.DataFrame(info_df['datasets'].iloc[i])
        temp_df = temp_df.rename(mapper=lambda x:'dataset_' + x,axis='columns')
        temp_df['workspace_id'] = info_df['workspaceId'].iloc[i]
        temp_df['workspace_name'] = info_df['workspaceName'].iloc[i]
        
        temp_df['dataset_refreshCadence'] = ''
        temp_df['dataset_refreshTimes'] = ''
        
        for j in range(0,len(temp_df)):
            
            if 'dataset_refreshSchedule' in temp_df.columns:
                
                if type(temp_df['dataset_refreshSchedule'].iloc[j]) == dict:
                    
                    if temp_df['dataset_refreshSchedule'].iloc[j]['days'] == ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']:
                        temp_df.at[j,'dataset_refreshCadence'] = 'Daily'
                    else:
                        temp_df.at[j,'dataset_refreshCadence'] = ','.join(temp_df['dataset_refreshSchedule'].iloc[j]['days'])
                    
                    temp_df.at[j,'dataset_refreshTimes'] = ','.join(temp_df['dataset_refreshSchedule'].iloc[j]['times']) + ' ' + temp_df['dataset_refreshSchedule'].iloc[j]['localTimeZoneId']
                    
        dataset_df = pd.concat([dataset_df,temp_df])
    
    return dataset_df

def make_dataflow_df(info_df):
    
    dataflow_df = pd.DataFrame()
    
    for i in range(0,len(info_df)):
        temp_df = pd.DataFrame(info_df['dataflows'].iloc[i])
        temp_df = temp_df.rename(mapper=lambda x:'dataflow_' + x,axis='columns')
        temp_df['workspace_id'] = info_df['workspaceId'].iloc[i]
        temp_df['workspace_name'] = info_df['workspaceName'].iloc[i]
        
        temp_df['dataflow_refreshCadence'] = ''
        temp_df['dataflow_refreshTimes'] = ''
        
        for j in range(0,len(temp_df)):
            if 'dataflow_refreshSchedule' in temp_df.columns:
                if type(temp_df['dataflow_refreshSchedule'].iloc[j]) == dict:
                    
                    if temp_df['dataflow_refreshSchedule'].iloc[j]['days'] == ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']:
                        temp_df.at[j,'dataflow_refreshCadence'] = 'Daily'
                    else:
                        temp_df.at[j,'dataflow_refreshCadence'] = ','.join(temp_df['dataflow_refreshSchedule'].iloc[j]['days'])
                    
                    temp_df.at[j,'dataflow_refreshTimes'] = ','.join(temp_df['dataflow_refreshSchedule'].iloc[j]['times']) + ' ' + temp_df['dataflow_refreshSchedule'].iloc[j]['localTimeZoneId']
                 
        
        
        dataflow_df = pd.concat([dataflow_df,temp_df])
        
    return dataflow_df

def make_tables_df(dataset_df):
    
    tables_df = pd.DataFrame()
    
    for i in range(0,len(dataset_df)):
        
        dataset_id = dataset_df['dataset_id'].iloc[i]
        dataset_name = dataset_df['dataset_name'].iloc[i]
        workspace_id = dataset_df['workspace_id'].iloc[i]
        workspace_name = dataset_df['workspace_name'].iloc[i]
        
        tables = pd.DataFrame(dataset_df['dataset_tables'].iloc[i])
    
        if tables.empty == False:
            
            tables['data_source'] = ''
            tables['long_source'] = ''
            for i in range (0,len(tables)):
                if 'source' in tables.columns:
                    if type(tables['source'].iloc[i]) == list:
                        source_string = tables['source'].iloc[i][0]['expression']
                        tables.at[i,'data_source'] = source_string[source_string.find("Source")+9:source_string.find(")")]
                        tables.at[i,'long_source'] = source_string[0:]


            tables = tables.explode('columns')
        
            tables = tables.reset_index(drop=True)
            table_columns = tables[tables['columns'].isnull() == False]
            
            columns_df = pd.DataFrame([table_columns['columns'].iloc[x] for x in range(0,len(table_columns))])
            columns_df = columns_df.rename(mapper=lambda x:'column_' + x,axis = 'columns')
            
            tables = pd.concat([tables,columns_df],axis = 1)
            
            tables['dataset_id'] = dataset_id
            tables['dataset_name'] = dataset_name
            tables['workspace_id'] = workspace_id
            tables['workspace_name'] = workspace_name
            
            tables_df = pd.concat([tables_df,tables])
            
    return tables_df

def make_catalog_df(report_df,dataset_df,dataflow_df):
    
    report_df = report_df.rename(columns={
        'report_id':'id'
        ,'report_name':'name'
        ,'report_datasetId':'connectionObjectId'
        ,'report_modifiedDateTime':'modifiedDateTime'
        ,'report_modifiedBy':'modifiedBy'
        ,'report_createdDateTime':'createdDateTime'
        ,'report_createdBy':'createdBy'
        ,'report_description':'description'
    })
    report_df = report_df[['id','name','description','connectionObjectId','modifiedDateTime','modifiedBy','createdDateTime','createdBy','workspace_id','workspace_name','meta_insert_timestamp']]
    report_df['objectType'] = 'Report'
    
    
    dataset_df = dataset_df.rename(columns={
        'dataset_id':'id'
        ,'dataset_name':'name'
        ,'dataset_configuredBy':'configuredBy'
        ,'dataset_createdDate':'createdDateTime'
        ,'dataset_refreshCadence':'refreshCadence'
        ,'dataset_refreshTimes':'refreshTimes'
        ,'dataset_description':'description'
    })
    dataset_df = dataset_df[['id','name','description','configuredBy','createdDateTime','refreshCadence','refreshTimes','workspace_id','workspace_name','meta_insert_timestamp']]
    dataset_df['objectType'] = 'Dataset'
    
    
    dataflow_df = dataflow_df.rename(columns={
        'dataflow_objectId':'id'
        ,'dataflow_name':'name'
        ,'dataflow_configuredBy':'configuredBy'
        ,'dataflow_description':'description'
        ,'dataflow_modifiedBy': 'modifiedBy'
        ,'dataflow_modifiedDateTime':'modifiedDateTime'
        ,'dataflow_refreshCadence':'refreshCadence'
        ,'dataflow_refreshTimes':'refreshTimes'
    })
    dataflow_df = dataflow_df[['id','name','description','configuredBy','modifiedBy','modifiedDateTime','refreshCadence','refreshTimes','workspace_id','workspace_name','meta_insert_timestamp']]
    dataflow_df['objectType'] = 'Dataflow'
    
    
    catalog_df = pd.concat([report_df,dataset_df,dataflow_df])
    
    return catalog_df
    
    
def get_secret():
    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.
    
    secretName = "pbi_creds"
    session = boto3.session.Session()
    client = session.client('secretsmanager',"us-west-2")
    logger.info('acquiring secret ' + secretName)
    
    
    try:
        logger.info('trying to get secret')
        get_secret_value_response = client.get_secret_value(
            SecretId = secretName
        )
        logger.info('secret value bucket' + str(get_secret_value_response))
        
    except ClientError as e:
        raise e
    
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret
