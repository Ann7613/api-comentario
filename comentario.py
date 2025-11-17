import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    print(event)
    
    if isinstance(event.get('body'), str):
        body = json.loads(event['body'])
    else:
        body = event.get('body', event)
    
    tenant_id = body['tenant_id']
    texto = body['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]
    
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        }
    }

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)
    

    s3_client = boto3.client('s3')
    

    s3_key = f"{tenant_id}/{uuidv1}.json"
    

    comentario_json = json.dumps(comentario, indent=2)
    

    s3_response = s3_client.put_object(
        Bucket=nombre_bucket,
        Key=s3_key,
        Body=comentario_json,
        ContentType='application/json'
    )

    print(comentario)
    print(f"Archivo guardado en S3: s3://{nombre_bucket}/{s3_key}")
    
    return {
        'statusCode': 200,
        'comentario': comentario,
        'dynamodb_response': response,
        's3_location': f"s3://{nombre_bucket}/{s3_key}",
        's3_response': {
            'ETag': s3_response.get('ETag'),
            'VersionId': s3_response.get('VersionId')
        }
    }
