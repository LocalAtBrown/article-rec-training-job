import * as cdk from "@aws-cdk/core";
import * as ecs from "@aws-cdk/aws-ecs";
import * as ec2 from "@aws-cdk/aws-ec2";
import * as iam from "@aws-cdk/aws-iam";
import * as helpers from "./helpers";
import { Schedule } from "@aws-cdk/aws-events";
import { ScheduledFargateTask } from "@aws-cdk/aws-ecs-patterns";

// TODO this needs to be propagated to the tags
export interface AppStackProps extends cdk.StackProps {
  stage: helpers.STAGE;
}

export class AppStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props: AppStackProps) {
    super(scope, id, props);

    const taskRole = new iam.Role(this, `${id}Role`, {
      assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
      inlinePolicies: {
        SecretsManagerAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              sid: `SecretsManagerAccess`,
              actions: ["secretsmanager:Get*"],
              resources: ["*"],
            }),
          ],
        }),
        SSMAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              sid: `SSMAccess`,
              actions: ["ssm:*"],
              resources: ["*"],
            }),
          ],
        }),
        CloudwatchPutAccess: new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              sid: `CloudwatchPutAccess`,
              actions: ["cloudwatch:Put*"],
              resources: ["*"],
            }),
          ],
        }),
      },
    });

    // const bucketName = helpers.makeBucketName("change-this-bucket-name", props.stage);
    // const bucket = helpers.makeBucket(this, bucketName, taskRole, props.stage);

    const policy = new iam.Policy(this, `${id}S3AccessPolicy`, {
      statements: [
        new iam.PolicyStatement({
          sid: `${id}S3ReadAccess`,
          actions: ["s3:Get*", "s3:List*"],
          resources: ["*"],
        }),
      ],
    });

    taskRole.attachInlinePolicy(policy);

    const vpc = helpers.getVPC(this, props.stage);
    const { cluster } = helpers.getECSCluster(this, props.stage, vpc);

    const dbName = 'ArticleRecDB';
    const secretName = `/${props.stage}/article-rec-training-job/db-password`;
    helpers.makeDatabase(this, props.stage, vpc, dbName, secretName);

    const image = ecs.ContainerImage.fromAsset("../", {
      extraHash: Date.now().toString(),
    });

    // find more cpu and memory options for fargate here:
    // https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
    const cpu = 1024;
    // take half a t3.small instance in dev
    // take half a t3.large instance in prod
    const memoryLimitMiB = props.stage == helpers.STAGE.PRODUCTION ? 4096 : 1024,

    const taskDefinition = new ecs.FargateTaskDefinition(this, `${id}TaskDefinition`, {
      taskRole,
      cpu,
      memoryLimitMiB,
    });
    // find more container definition options here:
    // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-ecs.ContainerDefinitionOptions.html
    taskDefinition.addContainer(`${id}TaskContainer`, {
      image,
       environment: {
          STAGE: props.stage,
          REGION: props.env?.region || 'us-east-1',
        },
        cpu,
        memoryLimitMiB,
        logging: ecs.LogDriver.awsLogs({
          streamPrefix: id,
          logRetention: 30,
        })
    });

    new ScheduledFargateTask(this, `${id}ScheduledFargateTask`, {
      // find more cron scheduling options here:
      // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-events.CronOptions.html
      schedule: Schedule.rate(cdk.Duration.hours(2)),
      desiredTaskCount: 1,
      cluster,
      subnetSelection: { subnetType: ec2.SubnetType.PUBLIC },
      scheduledFargateTaskDefinitionOptions: { taskDefinition }
    })
  }
}
