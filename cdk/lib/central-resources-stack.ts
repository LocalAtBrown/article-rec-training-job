import { Construct, Environment, Stack } from "@aws-cdk/core"
import { LogGroup, ILogGroup } from "@aws-cdk/aws-logs";
import { getResourceName, STAGE, RESOURCE } from "./helpers";

function getLogGroup(
  construct: Construct,
  stage: STAGE,
): ILogGroup {
    const logGroupName = getResourceName(stage, RESOURCE.LOG_GROUP)
    return new LogGroup(construct, logGroupName, {
      logGroupName: logGroupName,
      retention: 30,
    });
}

/**
 * @summary The CentralResourcesStack class for shared training job resources 
 * primarily, log groups
 */
export class CentralResourcesStack extends Stack {
  public readonly logGroup: ILogGroup

  constructor(scope: Construct, stage: STAGE, env: Environment) {
    const id = getResourceName(stage, RESOURCE.CENTRAL_RESOURCES_STACK)
    const props = {stage: stage, env: env}
    super(scope, id, props)
    this.logGroup = getLogGroup(this, stage)
  }
}
