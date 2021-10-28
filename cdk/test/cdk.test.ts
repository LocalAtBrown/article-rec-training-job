import { expect as expectCDK, haveResource } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import { AppStack} from '../lib/app-stack';
import { STAGE } from "../lib/helpers";
import { partners } from "../lib/partners";

const env = { account: "348955818350", region: "us-east-1" };
const partner = partners[0]


test('ProdAppStack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new AppStack(app, 'MyTestStack', {
        env, 
        stage: STAGE.PRODUCTION, 
        site: partner, 
        index: 0,
    });
    // THEN
    expectCDK(stack).to(haveResource("AWS::ECS::TaskDefinition"));
});

test('DevAppStack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new AppStack(app, 'MyTestStack', {
        env, 
        stage: STAGE.DEVELOPMENT,
        site: partner,
        index: 0,
    });
    // THEN
    expectCDK(stack).to(haveResource("AWS::ECS::TaskDefinition"));
});