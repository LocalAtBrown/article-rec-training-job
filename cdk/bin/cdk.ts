#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { AppStack } from "../lib/app-stack";
import { PipelineStack } from "../lib/pipeline-stack";
import { STAGE, Organization } from "../lib/helpers";
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


// Create a new app stack for every enabled partner and STAGE combination
for (const stage of [STAGE.PRODUCTION, STAGE.DEVELOPMENT]) {
  for (const [i, partner] of partners.filter(p => p.enabled).entries()) {
    new AppStack(app, getAppStackId(partner, stage, appStackName), {
      env,
      site: partner,
      stage: stage,
      index: i,
    });
  }
}
