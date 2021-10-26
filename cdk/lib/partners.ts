import { Organization } from "./helpers"

const DEFAULT_CPU = 2048;
const DEFAULT_MEM = 8192;

export const partners: Array<Organization> = [
  {
    orgName: "washington-city-paper",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: true,
  },
  {
    orgName: "philadelphia-inquirer",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: false,
  },
  {
    orgName: "texas-tribune",
    cpu: DEFAULT_CPU,
    memoryLimitMiB: DEFAULT_MEM,
    enabled: false,
  },

]
