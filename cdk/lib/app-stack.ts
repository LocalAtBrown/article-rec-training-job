import * as cdk from "@aws-cdk/core";
import * as ecs from "@aws-cdk/aws-ecs";
import * as ec2 from "@aws-cdk/aws-ec2";
import * as iam from "@aws-cdk/aws-iam";
import * as helpers from "./helpers";
import { Schedule } from "@aws-cdk/aws-events";

// TODO this needs to be propagated to the tags
export interface AppStackProps extends cdk.StackProps {
  stage: helpers.STAGE;
  repoName: string;
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

    const taskDefinition = new ecs.Ec2TaskDefinition(this, `${id}TaskDefinition`, { taskRole });
    // find more container definition options here:
    // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-ecs.ContainerDefinitionOptions.html
    const containerDefinition = taskDefinition.addContainer(`${id}TaskContainer`, {
      image,
       environment: {
          STAGE: props.stage,
          REGION: props.env?.region || 'us-east-1',
          SERVICE: props.repoName,
        },
        cpu: 128,
        memoryLimitMiB: 512,
        logging: ecs.LogDriver.awsLogs({
          streamPrefix: id,
          logRetention: 30,
        })
    });

    // map ports to send metrics via datadog
    const sslPort = 443
    const ntpPort = 123

    containerDefinition.addPortMappings({
      containerPort: ntpPort,
      hostPort: ntpPort,
      protocol: ecs.Protocol.UDP,
    });

    containerDefinition.addPortMappings({
      containerPort: sslPort,
      hostPort: sslPort,
    });

    helpers.makeScheduledTask(this, id, props.stage, {
      // find more cron scheduling options here:
      // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-events.CronOptions.html
      schedule: Schedule.cron({
        hour: "1",  // 1:00 am (UTC)
        minute: "0",
      }),
      desiredTaskCount: 1,
      cluster,
      subnetSelection: { subnetType: ec2.SubnetType.PUBLIC },
      scheduledEc2TaskDefinitionOptions: { taskDefinition }
    });
  }
}
