import * as cdk from "@aws-cdk/core";
import * as ecs from "@aws-cdk/aws-ecs";
import * as ec2 from "@aws-cdk/aws-ec2";
import * as iam from "@aws-cdk/aws-iam";
import * as helpers from "./helpers";
import { partners } from "./partners";
import { Schedule } from "@aws-cdk/aws-events";
import { ScheduledFargateTask } from "@aws-cdk/aws-ecs-patterns";
import { LogGroup, ILogGroup } from "@aws-cdk/aws-logs";

// TODO this needs to be propagated to the tags
export interface AppStackProps extends cdk.StackProps {
  stage: helpers.STAGE;
  site: helpers.Organization;
  index: number;
  logGroup: ILogGroup;
}

function getCron(stage: helpers.STAGE, index: number) {
  let n = partners.filter(f => f.enabled).length
  // Offset the jobs evenly across 1 hour.
  // Cron makes it really hard to do more complicated scheduling than that
  let offset = Math.floor(60 / n) * index

  return {
    minute: `${offset}`,
    hour: '0,2,4,6,8,10,12,14,16,18,20,22',
  }
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

    const policy = new iam.Policy(this, `${id}S3AccessPolicy`, {
      statements: [
        new iam.PolicyStatement({
          sid: `${id}S3Access`,
          actions: ["s3:Get*", "s3:List*", "s3:Put*"],
          resources: ["*"],
        }),
      ],
    });

    taskRole.attachInlinePolicy(policy);

    const vpc = helpers.getVPC(this, props.stage);
    const { cluster } = helpers.getECSCluster(this, props.stage, vpc);

    const image = ecs.ContainerImage.fromAsset("../", {
      extraHash: Date.now().toString(),
    });

    const cpu = props.site.cpu
    const memoryLimitMiB = props.site.memoryLimitMiB

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
          SITE: props.site.orgName
        },
        cpu,
        memoryLimitMiB,
        logging: ecs.LogDriver.awsLogs({
          logGroup: props.logGroup,
          streamPrefix: id,
        })
    });

    new ScheduledFargateTask(this, `${id}ScheduledFargateTask`, {
      // find more cron scheduling options here:
      // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-events.CronOptions.html
      schedule: Schedule.cron(getCron(props.stage, props.index)),
      desiredTaskCount: 1,
      cluster,
      subnetSelection: { subnetType: ec2.SubnetType.PUBLIC },
      scheduledFargateTaskDefinitionOptions: { taskDefinition },
      vpc
    })
  }
}
