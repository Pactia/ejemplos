import boto3
import pandas
import csv
import time

def get_var_char_values(d):
   return [obj['VarCharValue'] for obj in d['Data']]

def query_results(session, params, wait = True):    
    client = session.client('athena')
    
    ### Ejecución del query y retorno del id de la ejecución
    response_query_execution_id = client.start_query_execution(
        QueryString = params['query'],
        QueryExecutionContext = {
            'Database' : "default"
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        }
    )

    if not wait:
        return response_query_execution_id['QueryExecutionId']
    else:
        response_get_query_details = client.get_query_execution(
            QueryExecutionId = response_query_execution_id['QueryExecutionId']
        )
        status = 'RUNNING'
        iterations = 30

        while (iterations > 0):
            iterations = iterations - 1
            response_get_query_details = client.get_query_execution(
            QueryExecutionId = response_query_execution_id['QueryExecutionId']
            )
            ### cual es el status del query que lanzamos
            status = response_get_query_details['QueryExecution']['Status']['State']
            
            if (status == 'FAILED') or (status == 'CANCELLED') :
                return False, False

            elif status == 'SUCCEEDED':
                location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

                ## Fetch de los resultados del query si este ya termino
                response_query_result = client.get_query_results(
                    QueryExecutionId = response_query_execution_id['QueryExecutionId']
                )
                result_data = response_query_result['ResultSet']
                
                if len(response_query_result['ResultSet']['Rows']) > 1:
                    header = response_query_result['ResultSet']['Rows'][0]
                    rows = response_query_result['ResultSet']['Rows'][1:]
                
                    header = [obj['VarCharValue'] for obj in header['Data']]
                    result = [dict(zip(header, get_var_char_values(row))) for row in rows]
                    
                    #### devolvemos la localizacion en s3 del resultado y los datos (como records)
                    return location, result
                else:
                    return location, None
        else:
                time.sleep(5)

        return False