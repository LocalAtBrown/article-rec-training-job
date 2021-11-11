#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { AppStack } from "../lib/app-stack";
import { PipelineStack } from "../lib/pipeline-stack";
import { STAGE, Organization, baseAppStackName } from "../lib/helpers";
import { partners } from "../lib/partners";
import { CentralResourcesStack } from '../lib/central-resources-stack';

const app = new cdk.App();
const env = { account: "348955818350", region: "us-east-1" };
const repoName = "article-rec-training-job";

function getAppStackId(
    partner: Organization, 
    stage: STAGE, 
) {
    let prefix = ''
    if (stage == STAGE.DEVELOPMENT) {
        prefix = 'Dev'
    }
    return prefix + partner.pascalName + baseAppStackName
}


const activePartners = partners.filter(p => p.enabled)

new PipelineStack(app, `${baseAppStackName}Pipeline`, {
  ...env,
  repo: { name: repoName },
  appStackNames: activePartners.map(p => getAppStackId(p, STAGE.PRODUCTION)),
});

// Create a new app stack for every enabled partner and STAGE combination
for (const stage of [STAGE.PRODUCTION, STAGE.DEVELOPMENT]) {
  const centralResources = new CentralResourcesStack(
    app, 
    stage, 
    env,
  )
  for (const [i, partner] of activePartners.entries()) {
    new AppStack(app, getAppStackId(partner, stage), {
      env,
      site: partner,
      stage: stage,
      index: i,
      logGroup: centralResources.logGroup,
    });
  }
}
