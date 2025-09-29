import * as cdk from 'aws-cdk-lib';
import {Construct} from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSource from 'aws-cdk-lib/aws-lambda-event-sources';
import * as apigw from 'aws-cdk-lib/aws-apigateway';

export class DevicePipelineStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const bucket = new s3.Bucket(this, 'RawDataBucket', {
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            encryption: s3.BucketEncryption.S3_MANAGED,
            enforceSSL: true,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            autoDeleteObjects: true,
        });

        const dlq = new sqs.Queue(this, 'ItemsDLQ', {
            retentionPeriod: cdk.Duration.days(1),
        });

        const queue = new sqs.Queue(this, 'ItemsQueue', {
            visibilityTimeout: cdk.Duration.seconds(60),
            deadLetterQueue: {queue: dlq, maxReceiveCount: 3},
        });

        const table = new dynamodb.Table(this, 'AggregatesTable', {
            partitionKey: {name: 'date', type: dynamodb.AttributeType.STRING},
            sortKey: {name: 'type_state', type: dynamodb.AttributeType.STRING},
            billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
        });

        const commonsEnv = {
            TABLE_NAME: table.tableName,
            QUEUE_URL: queue.queueUrl,
            BUCKET_NAME: bucket.bucketName,
        };

        // Lambda factory method
        const createLambda = (
            lambdaId: string,
            handlerName: string,
            timeoutSec: number = 15,
            memorySize: number = 256,
        ): lambda.Function => {
            return new lambda.Function(this, lambdaId, {
                runtime: lambda.Runtime.PYTHON_3_9,
                handler: handlerName,
                code: lambda.Code.fromAsset('lambda'),
                timeout: cdk.Duration.seconds(timeoutSec),
                memorySize: memorySize,
                environment: commonsEnv,
            });
        };

        const ingestLambda = createLambda('IngestLambda', 'ingest_lambda.handler');
        bucket.grantPut(ingestLambda);
        queue.grantSendMessages(ingestLambda);

        const processLambda = createLambda('ProcessLambda', 'process_lambda.handler');
        table.grantReadWriteData(processLambda);
        processLambda.addEventSource(new lambdaEventSource.SqsEventSource(queue, {
            batchSize: 10,
            reportBatchItemFailures: true,
        }));

        const queryLambda = createLambda('QueryLambda', 'query_lambda.handler');
        table.grantReadData(queryLambda);

        // API Gateway
        const api = new apigw.RestApi(this, 'DevicePipelineApi', {
            restApiName: 'DevicePipelineApi',
            defaultCorsPreflightOptions: {
                allowOrigins: apigw.Cors.ALL_ORIGINS,
                allowMethods: ['GET', 'POST', 'OPTIONS'],
            },
        });

        api.root.addResource('ingest').addMethod('POST', new apigw.LambdaIntegration(ingestLambda));
        api.root.addResource('stats').addMethod('GET', new apigw.LambdaIntegration(queryLambda));

        // HTML table-view
        api.root.addResource('stats-html').addMethod('GET', new apigw.LambdaIntegration(queryLambda));

        new cdk.CfnOutput(this, 'ApiUrl', {value: api.url ?? 'N/A'});
        new cdk.CfnOutput(this, 'BucketName', {value: commonsEnv.BUCKET_NAME});
        new cdk.CfnOutput(this, 'QueueUrl', {value: commonsEnv.QUEUE_URL});
        new cdk.CfnOutput(this, 'TableName', {value: commonsEnv.TABLE_NAME});
    }
}
