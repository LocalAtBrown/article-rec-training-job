#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { AppStack } from "../lib/app-stack";
import { PipelineStack } from "../lib/pipeline-stack";
import { STAGE } from "../lib/helpers";

const app = new cdk.App();
const env = { account: "348955818350", region: "us-east-1" };
const repoName = "article-rec-training-job";
const appStackName = "ArticleRecTrainingJob";


new AppStack(app, appStackName, {
  env,
  repoName,
  stage: STAGE.PRODUCTION,
});

new PipelineStack(app, `${appStackName}Pipeline`, {
  ...env,
  repo: { name: repoName },
  appStackName: appStackName,
});

new AppStack(app, `Dev${appStackName}`, {
  env,
  repoName,
  stage: STAGE.DEVELOPMENT,
});
