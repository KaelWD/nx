import { execSync } from 'node:child_process';
import { isCI } from '../ci/is-ci';
import { getPackageManagerCommand } from '../package-manager';

export const NxCloudChoices = [
  'github',
  'circleci',
  'gitlab',
  'azure',
  'bitbucket-pipelines',
  'skip',
  'yes', // Deprecated but still handled
];

const messageOptions: Record<string, MessageData[]> = {
  setupCI: [
    {
      code: 'which-ci-provider',
      message: `Which CI provider would you like to use?`,
      initial: 0,
      choices: [
        { value: 'github', name: 'GitHub Actions' },
        { value: 'circleci', name: 'Circle CI' },
        { value: 'gitlab', name: 'Gitlab' },
        { value: 'azure', name: 'Azure DevOps' },
        { value: 'bitbucket-pipelines', name: 'BitBucket Pipelines' },
        { value: 'skip', name: '\nDo it later' },
      ],
      footer:
        '\nRemote caching, task distribution, and test splitting are provided by Nx Cloud. Read more at https://nx.dev/ci',
      fallback: { value: 'skip', key: 'setupNxCloud' },
    },
    {
      code: 'set-up-ci',
      message: `Set up CI with caching, distribution and test deflaking`,
      initial: 0,
      choices: [
        { value: 'github', name: 'GitHub Actions' },
        { value: 'circleci', name: 'CircleCI' },
        { value: 'gitlab', name: 'Gitlab' },
        { value: 'azure', name: 'Azure DevOps' },
        {
          value: 'bitbucket-pipelines',
          name: 'BitBucket Pipelines',
        },
        { value: 'skip', name: '\nDo it later' },
      ],
      footer:
        '\nRemote caching, task distribution, and test splitting are provided by Nx Cloud. Read more at https://nx.dev/ci',
      fallback: { value: 'skip', key: 'setupNxCloud' },
    },
  ],
  setupNxCloud: [
    {
      code: 'enable-caching',
      message: `Would you like remote caching to make your build faster?`,
      initial: 0,
      choices: [
        { value: 'yes', name: 'Yes' },
        { value: 'skip', name: 'Skip for now' },
      ],
      footer:
        '\nRead more about remote caching at https://nx.dev/ci/features/remote-cache',
      hint: `\n(it's free and can be disabled any time)`,
      fallback: undefined,
    },
  ],
};

export type MessageKey = keyof typeof messageOptions;
interface MessageData {
  code: string;
  message: string;
  initial: number;
  choices: Array<{ value: string; name: string }>;
  footer: string;
  hint?: string;
  fallback?: { value: string; key: MessageKey };
}

export class PromptMessages {
  private selectedMessages: { [key in MessageKey]?: number } = {};

  getPrompt(key: MessageKey): MessageData {
    if (this.selectedMessages[key] === undefined) {
      if (process.env.NX_GENERATE_DOCS_PROCESS === 'true') {
        this.selectedMessages[key] = 0;
      } else {
        this.selectedMessages[key] = Math.floor(
          Math.random() * messageOptions[key].length
        );
      }
    }
    return messageOptions[key][this.selectedMessages[key]!];
  }

  codeOfSelectedPromptMessage(key: MessageKey): string {
    const selected = this.selectedMessages[key];
    if (selected === undefined) {
      return '';
    } else {
      return messageOptions[key][selected].code;
    }
  }
}

export const messages = new PromptMessages();

/**
 * We are incrementing a counter to track how often create-nx-workspace is used in CI
 * vs dev environments. No personal information is collected.
 */
export async function recordStat(opts: {
  command: string;
  nxVersion: string;
  useCloud: boolean;
  meta: string[];
}) {
  try {
    if (!shouldRecordStats()) {
      return;
    }
    const axios = require('axios');
    await (axios['default'] ?? axios)
      .create({
        baseURL: 'https://cloud.nx.app',
        timeout: 400,
      })
      .post('/nx-cloud/stats', {
        command: opts.command,
        isCI: isCI(),
        useCloud: opts.useCloud,
        meta: opts.meta.filter((v) => !!v).join(','),
      });
  } catch (e) {
    if (process.env.NX_VERBOSE_LOGGING === 'true') {
      console.error(e);
    }
  }
}

function shouldRecordStats(): boolean {
  const pmc = getPackageManagerCommand();
  if (!pmc.getRegistryUrl) {
    // Fallback on true as Package management doesn't support reading config for registry.
    // currently Bun doesn't support fetching config settings https://github.com/oven-sh/bun/issues/7140
    return true;
  }
  try {
    const stdout = execSync(pmc.getRegistryUrl, { encoding: 'utf-8' });
    const url = new URL(stdout.trim());

    // don't record stats when testing locally
    return url.hostname !== 'localhost';
  } catch {
    // fallback to true if we can't detect the registry
    return true;
  }
}
