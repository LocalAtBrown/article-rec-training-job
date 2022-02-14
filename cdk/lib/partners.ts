import { Organization } from "./helpers"

// find more cpu and memory options for fargate here:
// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
const DEFAULT_CPU = 2048;
const DEFAULT_MEM = 8192;

export const partners: Array<Organization> = [
  {
    orgName: "washington-city-paper",
    pascalName: "WashingtonCityPaper",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: true,
  },
  {
    orgName: "philadelphia-inquirer",
    pascalName: "PhiladelphiaInquirer",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: true,
  },
  {
    orgName: "texas-tribune",
    pascalName: "TexasTribune",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: true,
  },

]
