import os, requests, json, boto3, base64, time, logging, pandas as pd, numpy as np
from botocore.exceptions import ClientError
from datetime import date
from datetime import timedelta

#test comment to see if changes from local happened

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event,context):
    

    logger.info('starting pbi usage to s3 lambda')
    
    #gets a number of credentials for power bi from the secrets manager (function below)
    pbi_details = get_secret('pbi_creds')
    logger.info('got secret shhhhhhhh')
    
    pbi_details = json.loads(pbi_details)
    
    access_token = get_api_token(pbi_details['client_id'],pbi_details['tenant_id'],pbi_details['client_secret'])
    usage_df = get_pbi_usage_df(access_token)
    usage_df.to_parquet("/tmp/pbi_usage.parquet",engine='fastparquet',index = False)
    
    datalake_bucket = json.loads(get_secret('datalake_bucket'))
    
    #initialize s3 session
    session = boto3.session.Session()
    s3 = session.resource('s3')

    #get current time for naming the raw file
    now = time.strftime("%Y%m%d-%H%M%S")

    try:
          #write the raw usage to the raw bucket
          s3.Bucket(datalake_bucket['raw_bucket_name']).upload_file("/tmp/pbi_usage.parquet","pbi_api/pbi_usage/pbi_usage_"+now+".parquet")
          os.remove("/tmp/pbi_usage.parquet")
    except:
          logger.info('an exception occurred with the usage writing')

    try:
          workspace_scan = get_worksace_scan(pbi_creds['client_id'],pbi_creds['tenant_id'],pbi_creds['client_secret'])
          logger.info("got workspace scan")
          workspace_scan['meta_insert_timestamp'] = datetime_now
          workspace_scan.to_parquet("/tmp/pbi_workspace_scan.parquet",engine = 'fastparquet',index = False)
          logger.info("wrote workspaces to parquet")
          s3.Bucket(datalake_bucket['raw_bucket_name']).upload_file("/tmp/pbi_workspace_scan.parquet","pbi_api/pbi_workspace_scan/pbi_scan_"+now+".parquet")
          os.remove("/tmp/pbi_workspace_scan.parquet")
      
    except:
         logger.info("An exception occurred with the workspace scan") 
       
    
  
    try:
        datasource_df = get_pbi_datasources(usage_df,access_token)
  
        if len(datasource_df) > 0:
            datasource_df.to_parquet("/tmp/pbi_datasources.parquet",engine='fastparquet',index=False)
            s3.Bucket(datalake_bucket['raw_bucket_name']).upload_file("/tmp/pbi_datasources.parquet","pbi_api/pbi_datasources/pbi_datasource_"+now+".parquet")
            os.remove("/tmp/pbi_datasources.parquet")
    except:
        logger.info("An exception occurred with writing the datasources")

def get_api_token(client_id,tenant_id,client_secret):

    #auth variables
    tenant = tenant_id
    client = client_id
    secret = client_secret
    scope = 'https://analysis.windows.net/powerbi/api/.default'
    token_url = 'https://login.microsoftonline.com/{}/oauth2/v2.0/token'.format(tenant)

    token_data = {
        'grant_type':'client_credentials'
        ,'client_id':client
        ,'client_secret':secret
        ,'authority':'https://login.microsoftonline.com/{}'.format(tenant)
        ,'scope':scope
        ,'endpoint':"https://api.powerbi.com/v1.0/myorg"
        }

    token_response = requests.post(token_url,data=token_data)
    access_token = token_response.json().get('access_token')
    
    return access_token
    
def get_pbi_usage_df(token):
    
    access_token = token
    
    #parameters for the url (it needs a start and end time)
    desired_date = str(date.today() - timedelta(days = 1))
    next_day = str(date.today())
    
    #desired_date = '2023-10-17'
    #next_day = '2023-10-18'
    
    #I need to do the variables like this because html is a butthead about ' and "
    #also the API is in UTC so to get a days worth of pacific time usage it needs to be split over two days and the ai won't let you mix days
    startDateTime1 = "'{}T08:00:00'".format(desired_date)
    endDateTime1 = "'{}T23:59:59'".format(desired_date)

    startDateTime2 = "'{}T00:00:00'".format(next_day) 
    endDateTime2  = "'{}T07:59:59'".format(next_day)

    #todo, parameterize the columns and column mappings.  These need to be consistent so that the staging load doesn't break when microsoft adds a new column or there needs to be a better alarm system for when it breaks
    columns = ['Id', 'RecordType', 'CreationTime', 'Operation', 'OrganizationId', 'UserType', 'UserKey', 'Workload','UserId','ClientIP','Activity','ItemName','WorkSpaceName','CapacityId','CapacityName','WorkspaceId','ObjectId','DataflowId','DataflowName','IsSuccess','DataflowRefreshScheduleType','DataflowType','RequestId','ActivityId','UserAgent','DataflowAccessTokenRequestParameters','DatasetName','ReportName','DatasetId','ReportId','ArtifactId','ArtifactName','ReportType','DistributionMethod','ConsumptionMethod','ArtifactKind','HasFullReportAttachment','ModelsSnapshots','ExportEventStartDateTimeParameter','ExportEventEndDateTimeParameter','ExportEventActivityTypeParameter','DataConnectivityMode','LastRefreshTime','ExportedArtifactInfo','ImportId','ImportSource','ImportType','ImportDisplayName','RefreshType','SharingInformation','Datasets','SharingAction','ShareLinkId','AggregatedWorkspaceInformation','Schedules','GatewayId','Monikers','DatasourceObjectIds','ModelId','IsTenantAdminApi','DatasourceId','GatewayClusterId','GatewayClusters','DataflowAllowNativeQueries','DashboardName','DashboardId','FolderObjectId','FolderDisplayName','FolderAccessRequests','GatewayClusterDatasources','TemplateAppObjectId','PinReportToTabInformation','CustomVisualAccessTokenResourceId','CustomVisualAccessTokenSiteUri','TargetWorkspaceId','CopiedReportName','CopiedReportId','WorkspaceAccessList','SwitchState']
    datatypes = {"Id":"string"  ,"RecordType":"double"  ,"CreationTime":"string"  ,"Operation":"string"  ,"OrganizationId":"string"  ,"UserType":"double"  ,"UserKey":"string"  ,"Workload":"string"  ,"UserId":"string"  ,"ClientIP":"string"  ,"Activity":"string"  ,"ItemName":"string"  ,"WorkSpaceName":"string"  ,"CapacityId":"string"  ,"CapacityName":"string"  ,"WorkspaceId":"string"  ,"ObjectId":"string"  ,"DataflowId":"string"  ,"DataflowName":"string"  ,"IsSuccess":"float"  ,"DataflowRefreshScheduleType":"string"  ,"DataflowType":"string"  ,"RequestId":"string"  ,"ActivityId":"string"  ,"UserAgent":"string"  ,"DataflowAccessTokenRequestParameters":"string"  ,"DatasetName":"string"  ,"ReportName":"string"  ,"DatasetId":"string"  ,"ReportId":"string"  ,"ArtifactId":"string"  ,"ArtifactName":"string"  ,"ReportType":"string"  ,"DistributionMethod":"string"  ,"ConsumptionMethod":"string"  ,"ArtifactKind":"string"  ,"HasFullReportAttachment":"string"  ,"ModelsSnapshots":"string"  ,"ExportEventStartDateTimeParameter":"string"  ,"ExportEventEndDateTimeParameter":"string"  ,"ExportEventActivityTypeParameter":"string"  ,"DataConnectivityMode":"string"  ,"LastRefreshTime":"string"  ,"ExportedArtifactInfo":"string"  ,"ImportId":"string"  ,"ImportSource":"string"  ,"ImportType":"string"  ,"ImportDisplayName":"string"  ,"RefreshType":"string"  ,"SharingInformation":"string"  ,"Datasets":"string"  ,"SharingAction":"string"  ,"ShareLinkId":"string"  ,"AggregatedWorkspaceInformation":"string"  ,"Schedules":"string"  ,"GatewayId":"string"  ,"Monikers":"string"  ,"DatasourceObjectIds":"string"  ,"ModelId":"string"  ,"IsTenantAdminApi":"string"  ,"DatasourceId":"string"  ,"GatewayClusterId":"string"  ,"GatewayClusters":"string"  ,"DataflowAllowNativeQueries":"float"  ,"DashboardName":"string"  ,"DashboardId":"string"  ,"FolderObjectId":"string"  ,"FolderDisplayName":"string"  ,"FolderAccessRequests":"string"  ,"GatewayClusterDatasources":"string"  ,"TemplateAppObjectId":"string"  ,"PinReportToTabInformation":"string"  ,"CustomVisualAccessTokenResourceId":"string"  ,"CustomVisualAccessTokenSiteUri":"string"  ,"TargetWorkspaceId":"string"  ,"CopiedReportName":"string"  ,"CopiedReportId":"string"  ,"WorkspaceAccessList":"string"  ,"SwitchState":"string"  }

  
    output = pd.DataFrame()
    
    logger.info('set up api call ')
    
    output = usage_api_call(output,access_token,startDateTime1,endDateTime1)
    logger.info('got day 1')

    #the usage_api_call appends to save me from making another variable
    output = usage_api_call(output,access_token,startDateTime2,endDateTime2)
    logger.info('got day 2')
    logger.info(desired_date)
  
    #put it in pacific time
    output['CreationTime'] = output['CreationTime'].dt.tz_convert('US/Pacific')
    output = output.reset_index(drop=True)

    #force the usage data to have the desired columns
    output = pd.concat([pd.DataFrame([],columns = columns),output])
    output = output[columns]
    output = output.astype(datatypes)
    
    return output


def usage_api_call(starting_df, token, startDateTime, endDateTime):

    logger.info(f'Bearer {token}')
    header = {'Authorization': f'Bearer {token}'}
    api_url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime={}&endDateTime={}".format(startDateTime,endDateTime)
    
    # HTTP GET Request
    response = requests.get(api_url, headers=header)
    #making a dataframe from the dictionary of json
    response_df = pd.DataFrame(response.json())
    logger.info(response)
    
    #this tells me if the call worked
    if response.status_code == 200:
        logger.info('API called successfully')
    else:
        logger.info(response.reason)
        return None

    try:
        #I only need the activity event entities part of the json dictionary from the response. which is itself a dict so that's a bit silly
        temp_output = pd.DataFrame([response_df['activityEventEntities'].iloc[x] for x in range(0,len(response_df))])
        #datetime shenanigans to make it utc
        temp_output['CreationTime'] = pd.to_datetime(temp_output['CreationTime'],utc = True)
    
        #put the result from the api call on the end of what I already have
        output = pd.concat([starting_df,temp_output],ignore_index = True)
    
        #need to check for a continuation token to make sure there aren't more events
        while response_df['continuationToken'].iloc[0] != None:
            
            #get the api url from the previous call
            continuation_uri = response_df['continuationUri'].iloc[0]
            
            #call the api
            groups_cont = requests.get(continuation_uri,headers=header)
            
            #turn the call into a dataframe that has a continuation token and a different column for actual logs
            response_df = pd.DataFrame(groups_cont.json())
            
            #check if we hit the ned of the day and break if we did
            if response_df.empty: break
            
            #set up the log dataframe
            cont_output = pd.DataFrame([response_df['activityEventEntities'].iloc[x] for x in range(0,len(response_df))])
            #datetime shenanigans
            cont_output['CreationTime'] = pd.to_datetime(cont_output['CreationTime'],utc=True)
            
            #put this on the end of the call we alread have
            output = pd.concat([output,cont_output],ignore_index=True)
    
    except:
        logger.info('continuation tokens messed up')
    else:
        logger.info('Usage successfully retreived')
        return output


def get_workspace_scan(access_token,startDateTime):

    since_date = "'{}T08:00:00'".format(str(date.today() - timedelta(days = 1)))
    api_url = "https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified?modifiedSince={}&excludePersonalWorkspaces=True&excludeInActiveWorkspaces=True".format(since_date)
    
    header = {'Authorization': f'Bearer {access_token}'}
    
    groups = requests.get(api_url, headers=header)
    
    if groups.status_code == 200:
        logger.info('Got workspaces')
    
    groups = pd.DataFrame(groups.json())
    groups = pd.DataFrame([groups[x].['Id'] for x in range(0,len(groups))])
    
    
    if groups_mid.empty == True:
        logger.info('No modified workspaces')
    
    else:
        workspaces_list = list(groups_mid['id'].unique())
        
        num_of_ws_sets = math.ceil(len(workspaces_list)/100)
        
        scan_id = list(range(0,num_of_ws_sets))

        info_df = pd.DataFrame()
        
        for i in range(0,num_of_ws_sets):
            
            start = i*100
            end = (i+1)*100
            
            if i == num_of_ws_sets - 1:
                    end = len(workspaces_list)%100 + i*100
            
            body = {'workspaces': workspaces_list[start:end]}
            
            api_url = 'https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?getArtifactUsers=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True'
        
            response = requests.post(api_url,headers=header,json = body)
            info_mid = pd.DataFrame([response.json()])
            
            scan_id[i] = info_mid['id'].iloc[0]
        
        
        scan_status = 'In Progress'
        while scan_status != 'Succeeded':
              api_url = 'https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{}'.format(scan_id[0])
              response = requests.get(api_url,headers=header)
              scan_status = pd.DataFrame([response.json()])['status'][0]
              time.sleep(30)
              if scan_status == 'Succeeded':
                  print('Scan finished')
          
          
          for i in range(0,len(scan_id)):
              
              api_url = api_url = 'https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{}'.format(scan_id[i])
              response = requests.get(api_url,headers = header)
              info_mid = pd.DataFrame(response.json())
              info_df = pd.concat([info_df,pd.DataFrame([info_mid['workspaces'].iloc[x] for x in range(0,len(info_mid))])])
             
              
          print('Got scan results')
          
          #rename a select few columns so that the staging stuff is more clear.  I know that it's not technically raw now but it got really confusing when everything was just Id and Name
          info_df = info_df.rename(columns={'id':'workspaceId'
                                    ,'name':'workspaceName'
                                    ,'description':'workspaceDescription'})
          return info_df

def get_pbi_datasources(usage_df,token):
    
    header = {'Authorization': f'Bearer {token}'}

  #get list of dataset/dataflow id's that have been created or updated and aren't in personal workspaces
    id_list = usage_df[(usage_df['Activity'].isin(['CreateDataflow','CreateDataset','EditDataset','UpdateDataflow'])) & (usage_df['WorkSpaceName'].str[:17] != 'PersonalWorkspace')][['DatasetId','DataflowId','Activity','DatasetName','DataflowName']].drop_duplicates()
    
    datasource_df = pd.DataFrame()
    
    for i in range(0,len(id_list)):
        
        logger.info(id_list['Activity'].iloc[i])
        #datasets and dataflows have different api endpoints so I need to split the job up    
        if (id_list['Activity'].iloc[i] == 'CreateDataflow' or id_list['Activity'].iloc[i] == 'UpdateDataflow'):
            
            object_id = id_list['DataflowId'].iloc[i]
            
            api_url = 'https://api.powerbi.com/v1.0/myorg/admin/dataflows/{}/datasources'.format(object_id)
    
            #call the api and put it in a dataframe
            datasource = requests.get(api_url,headers=header)
    
            if datasource.status_code == 200:
                
                datasource_temp_df = pd.DataFrame(datasource.json()['value'])
                datasource_temp_df['connectionObjectId'] = object_id
                datasource_temp_df['name'] = id_list['DataflowName'].iloc[i]
                
                #continue filling out the datasource dataframe
                datasource_df = pd.concat([datasource_df,datasource_temp_df],ignore_index = True)
        
        elif (id_list['Activity'].iloc[i] == 'CreateDataset' or id_list['Activity'].iloc[i] == 'EditDataset'):
            
            object_id = id_list['DatasetId'].iloc[i]
            
            #make a new api call from the dataset id to get it's datasources
            api_url = 'https://api.powerbi.com/v1.0/myorg/admin/datasets/{}/datasources'.format(object_id)
            
            #call the api
            datasource = requests.get(api_url,headers=header)
            logger.info(datasource)
            #this tells you if the call was successful. 200 is good
            if datasource.status_code == 200:
            
                #turn the response into a dataframe
                datasource_temp_df = pd.DataFrame(datasource.json()['value'])
                
                #make a column in the table with the dataset id so I know which one the sources are connected to
                datasource_temp_df['connectionObjectId'] = object_id
                datasource_temp_df['name'] = id_list['DatasetName'].iloc[i]
                
                #start filling out the datasource dataframe
                datasource_df = pd.concat([datasource_df,datasource_temp_df],ignore_index = True)
        
    return datasource_df

     
def get_secret(secretName):
    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.
    
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
