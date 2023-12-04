import { Organization } from "./helpers"

// find more cpu and memory options for fargate here:
// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
const DEFAULT_CPU = 2048;
const DEFAULT_MEM = 8192;
const DEFAULT_CRON_HOURS = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22];

export const partners: Array<Organization> = [
  {
    orgName: "washington-city-paper",
    pascalName: "WashingtonCityPaper",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: true,
    cronHours: [6], // 6 AM UTC = 2 AM EST, run once a day, during the night, until WCP is ready to run more often
  },
  {
    orgName: "texas-tribune",
    pascalName: "TexasTribune",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: true,
    cronHours: DEFAULT_CRON_HOURS,
  },
  {
    orgName: "philadelphia-inquirer",
    pascalName: "PhiladelphiaInquirer",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: 16384,
    enabled: true,
    cronHours: DEFAULT_CRON_HOURS,
  },
]
