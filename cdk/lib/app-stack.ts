import * as cdk from "@aws-cdk/core";
import * as ecs from "@aws-cdk/aws-ecs";
import * as ec2 from "@aws-cdk/aws-ec2";
import * as iam from "@aws-cdk/aws-iam";
import * as helpers from "./helpers";
import { Schedule } from "@aws-cdk/aws-events";

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

    const vpc = helpers.getVPC(this, props.stage);
    const { cluster } = helpers.getECSCluster(this, props.stage, vpc);

    helpers.makeDatabase(this, props.stage, id, vpc);

    const image = ecs.ContainerImage.fromAsset("../", {
      extraHash: Date.now().toString(),
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
      // find more task image options here:
      // https://docs.aws.amazon.com/cdk/api/latest/docs/@aws-cdk_aws-ecs-patterns.ScheduledEc2TaskImageOptions.html
      scheduledEc2TaskImageOptions: {
        image,
        environment: {
          STAGE: props.stage,
          REGION: props.env?.region || 'us-east-1',
        },
        cpu: 128,
        memoryLimitMiB: 128,
        logDriver: ecs.LogDriver.awsLogs({
          streamPrefix: this.node.id,
          logRetention: 30,
        })
      },
    });
  }
}
