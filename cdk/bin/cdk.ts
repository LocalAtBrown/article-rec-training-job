#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { AppStack } from "../lib/app-stack";
import { PipelineStack } from "../lib/pipeline-stack";
import { STAGE, Organization, Status } from "../lib/helpers";
import { partners } from "../lib/partners";

const app = new cdk.App();
const env = { account: "348955818350", region: "us-east-1" };
const repoName = "article-rec-training-job";
const appStackName = "ArticleRecTrainingJob";

new PipelineStack(app, `${appStackName}Pipeline`, {
  ...env,
  repo: { name: repoName },
  appStackName: appStackName,
});

function getAppStackId(
    partner: Organization, 
    stage: STAGE, 
    baseName: string,
) {
    let prefix = ''
    if (stage == STAGE.DEVELOPMENT) {
        prefix = 'Dev'
    }
    return prefix + partner.pascalName + baseName
}


let i = 0;
for (const partner of partners.filter(p => p.enabled)) {
  for (const stage of [STAGE.PRODUCTION, STAGE.DEVELOPMENT]) {
    new AppStack(app, getAppStackId(partner, stage, appStackName), {
      env,
      site: partner,
      stage: stage,
      index: i,
    });
    i++;
  }
}
