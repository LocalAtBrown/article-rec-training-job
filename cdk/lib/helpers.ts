import * as cdk from "@aws-cdk/core";
import * as ec2 from "@aws-cdk/aws-ec2";
import * as ecs from "@aws-cdk/aws-ecs";
import * as iam from "@aws-cdk/aws-iam";
import * as s3 from '@aws-cdk/aws-s3';
import * as codebuild from "@aws-cdk/aws-codebuild";
import { LocalCacheMode } from "@aws-cdk/aws-codebuild";
import { Schedule } from "@aws-cdk/aws-events";
import * as logs from "@aws-cdk/aws-logs";
import { Secret } from "@aws-cdk/aws-secretsmanager";

export const baseAppStackName = "ArticleRecTrainingJob";

export enum STAGE {
  PRODUCTION = "prod",
  DEVELOPMENT = "dev",
}

export enum RESOURCE {
  CENTRAL_RESOURCES_STACK = "CentralResourcesStack",
  LOG_GROUP = "LogGroup"
}

export function getResourceName(stage: STAGE, resource: RESOURCE): string {
  let prefix = ''
  if (stage == STAGE.DEVELOPMENT) {
    prefix = 'Dev'
  }
  return prefix + baseAppStackName + resource
}


/**
 * @description Organization interface
 * @param {string} orgName hyphenated snake case organization name
 * @param {string} pascalName pascal case organization name
 * @param {number} cpu vCPUs for the training job
 * @param {number} memoryLimitMiB max memory for the training job
 * @param {boolean} enabled is the job enabled?
 * @param {number[]} cronHours hours to run the job
 */
export interface Organization {
  orgName: string,
  pascalName: string,
  cpu: number,
  memoryLimitMiB: number,
  enabled: boolean,
  cronHours: number[],
}


export function getVPC(scope: cdk.Construct, stage: STAGE) {
  const resourcePrefix = titleCase(stage);
  const vpcName = `infrastructure/${resourcePrefix}PublicVPC`;
  const vpc = ec2.Vpc.fromLookup(scope, "VPC", { vpcName });
  return vpc;
}


export function getECSCluster(scope: cdk.Construct, stage: STAGE, vpc: ec2.IVpc) {
  let clusterName, securityGroups;
  const resourcePrefix = titleCase(stage);

  const securityGroupIds = cdk.Fn.importValue(
    `${resourcePrefix}PublicFargateCluster-security-group-ids`
  ).split(",");

  clusterName = cdk.Fn.importValue(`${resourcePrefix}PublicFargateCluster-name`);
  securityGroups = securityGroupIds.map((x, i) =>
    ec2.SecurityGroup.fromSecurityGroupId(scope, `SecurityGroup${i}`, x)
  );


  const cluster = ecs.Cluster.fromClusterAttributes(scope, `${resourcePrefix}PublicFargateCluster`, {
    clusterName,
    vpc,
    securityGroups,
  });

  return { cluster, securityGroups };
}

export function titleCase(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

export function kebabToPascal(s: string) {
  const components = s.split('-');
  const titleCased = components.map(function(w) { return (titleCase(w)) });
  return titleCased.join('');
}

export function pascalToKebab(s: string) {
  let kebab = '';
  const components = s.match(/[A-Z][a-z]+/g);
  if (components !== null) {
    const lowerCased = components.map(function(w) { return w.toLowerCase() });
    kebab = lowerCased.join('-');
  }
  return kebab;
}

export function makeBucketName(bucketName: string, stage: STAGE): string {
  let prefix;
  if (stage == STAGE.PRODUCTION) {
    prefix = 'lnl-'
  } else {
    prefix = `lnl-${stage}-`
  }
  return `${prefix}${bucketName}`;
}

export function makeBucket(
  scope:cdk.Construct,
  bucketName: string,
  taskRole: iam.Role,
  stage: STAGE)
{
  const bucketPrefix = 'lnl-'
  if (bucketName.startsWith(bucketPrefix) !== true) {
    throw TypeError(`bucketName must begin with prefix: ${bucketPrefix}`);
  }
  const pascalName = kebabToPascal(bucketName.slice(bucketPrefix.length));

  let removalPolicy = cdk.RemovalPolicy.RETAIN;
  if (stage != STAGE.PRODUCTION) {
    removalPolicy = cdk.RemovalPolicy.DESTROY;
  }

  const bucket = new s3.Bucket(scope, `${pascalName}Bucket`, {
    bucketName,
    removalPolicy,
  });
  const policy = new iam.Policy(scope, `${pascalName}BucketPolicy`, {
    statements: [
      new iam.PolicyStatement({
        sid: `Access${pascalName}Bucket`,
        actions: ["s3:*"],
        resources: [
          `arn:aws:s3:::${bucketName}`,
          `arn:aws:s3:::${bucketName}/*`,
        ],
      }),
    ],
  });

  taskRole.attachInlinePolicy(policy);

  return bucket;
}

export function makeCDKDeployProject(
  scope: cdk.Construct,
  appStackName: string,
  role: iam.IRole
) {


  return new codebuild.PipelineProject(scope, `${appStackName}CDKDeploy`, {
    role,
    // consider using an explicit buildspec.yml file which may be easier to read and modify
    // aws_codebuild.BuildSpec.from_source_filename(filename='cdk/buildspec.yml')
    buildSpec: codebuild.BuildSpec.fromObject({
      version: "0.2",
      phases: {
        install: {
          commands: ["cd cdk", "npm install"],
        },
        build: {
          commands: [
            "docker login -u=$DOCKER_USER -p=$DOCKER_PASSWORD",
            "npm run build",
            `npx cdk deploy ${appStackName} --require-approval never --ci`,
          ],
        },
      },
    }),
    environment: {
      buildImage: codebuild.LinuxBuildImage.STANDARD_2_0,
      privileged: true
    },
    environmentVariables: {
        DOCKER_PASSWORD: {
          value: '/codebuild/docker-login',
          type: codebuild.BuildEnvironmentVariableType.PARAMETER_STORE,
        },
        DOCKER_USER: {
          value: 'localnewslab',
          type: codebuild.BuildEnvironmentVariableType.PLAINTEXT,
        }
    },
    cache: codebuild.Cache.local(LocalCacheMode.DOCKER_LAYER)
  });
}
