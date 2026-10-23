import * as cdk from 'aws-cdk-lib';
import { RustFunction } from 'cargo-lambda-cdk';
import { Construct } from 'constructs';
import * as iam from "aws-cdk-lib/aws-iam";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as sources from "aws-cdk-lib/aws-lambda-event-sources";
import * as s3 from "aws-cdk-lib/aws-s3";

export const CACHE_BUCKET_NAME = "extstats-cache";

let BGG_QUEUE1: sqs.IQueue | undefined = undefined;
let BGG_QUEUE2: sqs.IQueue | undefined = undefined;
let CACHE_BUCKET: s3.IBucket | undefined = undefined;

export class RustDownloaderStack extends cdk.Stack {
  lookupExternalResources() {
    // resources external to this stack
    const q1 = cdk.Fn.importValue('downloader-BGGQueue1ARN');
    const q2 = cdk.Fn.importValue('downloader-BGGQueue2ARN');
    BGG_QUEUE1 = sqs.Queue.fromQueueAttributes(this, "bggQueue1", { queueArn: q1 });
    BGG_QUEUE2 = sqs.Queue.fromQueueAttributes(this, "bggQueue2", { queueArn: q2 });
    CACHE_BUCKET = s3.Bucket.fromBucketName(this, "cacheBucket", CACHE_BUCKET_NAME);
  }

  defineDownloaderRole(bggQueue1: sqs.IQueue, bggQueue2: sqs.IQueue, cacheBucket: s3.IBucket): iam.IRole {
    const policies: Record<string, iam.PolicyDocument> = {};

    const bggParameters = new iam.PolicyStatement();
    bggParameters.addActions("ssm:GetParameter", "ssm:GetParametersByPath", "ssm:GetParameters", "ssm:PutParameter");
    bggParameters.addResources(`arn:aws:ssm:${process.env.CDK_DEFAULT_REGION}:${process.env.CDK_DEFAULT_ACCOUNT}:parameter/extstats/downloader*`);
    const bggSecrets = new iam.PolicyStatement();
    bggSecrets.addActions("secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret");
    bggSecrets.addResources(`arn:aws:secretsmanager:${process.env.CDK_DEFAULT_REGION}:${process.env.CDK_DEFAULT_ACCOUNT}:secret:extstats/bgg*`);

    policies[`policy_downloader_bgg`] = new iam.PolicyDocument({
      statements: [bggParameters, bggSecrets]
    });

    const sqsStuff = new iam.PolicyStatement();
    sqsStuff.addActions("sqs:ListQueues");
    sqsStuff.addResources("*");
    policies[`policy_sqs_stuff`] = new iam.PolicyDocument({
      statements: [sqsStuff]
    });

    const sendToBGGQueue = new iam.PolicyStatement();
    sendToBGGQueue.addActions("sqs:SendMessage");
    sendToBGGQueue.addResources(bggQueue1.queueArn, bggQueue2.queueArn);
    policies[`policy_bgg_queue`] = new iam.PolicyDocument({
      statements: [sendToBGGQueue]
    });

    const durableLambda = new iam.PolicyStatement();
    durableLambda.addActions("lambda:CheckpointDurableExecution", "lambda:SendDurableExecutionCallbackSuccess", "lambda:SendDurableExecutionCallbackFailure");
    durableLambda.addResources("*");
    policies[`policy_downloader_durable`] = new iam.PolicyDocument({
      statements: [durableLambda]
    });

    const useCacheBucket2 = new iam.PolicyStatement();
    useCacheBucket2.addActions("s3:*Object");
    useCacheBucket2.addResources(cacheBucket.bucketArn + "/*");
    policies[`policy_cache_bucket`] = new iam.PolicyDocument({
      statements: [ useCacheBucket2]
    });

    const invokeLambdas = new iam.PolicyStatement();
    invokeLambdas.addActions("lambda:InvokeFunction");
    invokeLambdas.addResources(`arn:aws:lambda:${process.env.CDK_DEFAULT_REGION}:${process.env.CDK_DEFAULT_ACCOUNT}:function:downloader_complete*`);
    policies[`policy_invoke_complete`] = new iam.PolicyDocument({
      statements: [invokeLambdas]
    });

    const managedPolicies: iam.IManagedPolicy[] = [
      iam.ManagedPolicy.fromManagedPolicyName(this, "createEC2s", "CreateEC2sForLambda"),
      iam.ManagedPolicy.fromManagedPolicyName(this, "writeLogs", "WriteToCloudwatchLogs"),
    ];

    return new iam.Role(this, `role_rust_downloader`, {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      inlinePolicies: policies,
      managedPolicies: managedPolicies,
    });
  }

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    this.lookupExternalResources();

    let downloaderRole = this.defineDownloaderRole(BGG_QUEUE1!, BGG_QUEUE2!, CACHE_BUCKET!);

    const addTimestampFunction = new RustFunction(this, 'rust_fn_add_timestamp',{
      manifestPath: '../bggdl',
      binaryName: "add_timestamp",
      runtime: 'provided.al2023',
      timeout: cdk.Duration.seconds(3),
      allowPublicSubnet: true,
      role: downloaderRole,
      memorySize: 128,
      reservedConcurrentExecutions: 1
    });
    const downloadFunction = new RustFunction(this, 'rust_fn_download',{
      manifestPath: '../bggdl',
      binaryName: "download",
      runtime: 'provided.al2023',
      timeout: cdk.Duration.seconds(900),
      allowPublicSubnet: true,
      role: downloaderRole,
      memorySize: 128,
      durableConfig: {
        executionTimeout: cdk.Duration.seconds(900),
      }
    });

    if (BGG_QUEUE1) {
      const mapping = new sources.SqsEventSource(BGG_QUEUE1, {batchSize: 1, enabled: false});
      addTimestampFunction.latestVersion.addEventSource(mapping);
      new cdk.CfnOutput(this, 'downloaderAddTimestampMapping', {
        value: mapping.eventSourceMappingId,
        exportName: 'downloader-AddTimestampMappingUUID'
      });
    } else {
      console.log("Didn't find QUEUE 1");
    }

    if (BGG_QUEUE2) {
      const mapping = new sources.SqsEventSource(BGG_QUEUE2, {batchSize: 1, enabled: false});
      downloadFunction.latestVersion.addEventSource(mapping);
      new cdk.CfnOutput(this, 'downloaderDownloadMapping', {
        value: mapping.eventSourceMappingId,
        exportName: 'downloader-DownloadMappingUUID'
      });
    } else {
      console.log("Didn't find QUEUE 2");
    }

    new cdk.CfnOutput(this, 'rust_add_timestamp', {
      description: 'BGG add timestamp function in Rust',
      value: addTimestampFunction.functionArn,
    });
    new cdk.CfnOutput(this, 'rust_download', {
      description: 'BGG download function in Rust',
      value: downloadFunction.functionArn,
    });
  }
}
