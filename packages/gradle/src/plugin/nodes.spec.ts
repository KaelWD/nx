import { CreateNodesContext } from '@nx/devkit';

import { TempFs } from 'nx/src/internal-testing-utils/temp-fs';
import { type GradleReport } from '../utils/get-gradle-report';

let gradleReport: GradleReport;
jest.mock('../utils/get-gradle-report', () => {
  return {
    GRADLE_BUILD_FILES: new Set(['build.gradle', 'build.gradle.kts']),
    populateGradleReport: jest.fn().mockImplementation(() => void 0),
    getCurrentGradleReport: jest.fn().mockImplementation(() => gradleReport),
  };
});

import { createNodesV2 } from './nodes';

describe('@nx/gradle/plugin', () => {
  let createNodesFunction = createNodesV2[1];
  let context: CreateNodesContext;
  let tempFs: TempFs;
  let cwd: string;

  beforeEach(async () => {
    tempFs = new TempFs('test');
    gradleReport = {
      gradleFileToGradleProjectMap: new Map<string, string>([
        ['proj/build.gradle', 'proj'],
      ]),
      buildFileToDepsMap: new Map<string, string>(),
      gradleFileToOutputDirsMap: new Map<string, Map<string, string>>([
        ['proj/build.gradle', new Map([['build', 'build']])],
      ]),
      gradleProjectToTasksTypeMap: new Map<string, Map<string, string>>([
        ['proj', new Map([['test', 'Verification']])],
      ]),
      gradleProjectToProjectName: new Map<string, string>([['proj', 'proj']]),
    };
    cwd = process.cwd();
    process.chdir(tempFs.tempDir);
    context = {
      nxJsonConfiguration: {
        namedInputs: {
          default: ['{projectRoot}/**/*'],
          production: ['!{projectRoot}/**/*.spec.ts'],
        },
      },
      workspaceRoot: tempFs.tempDir,
      configFiles: [],
    };

    await tempFs.createFiles({
      'proj/build.gradle': ``,
      gradlew: '',
    });
  });

  afterEach(() => {
    jest.resetModules();
    process.chdir(cwd);
  });

  it('should create nodes based on gradle', async () => {
    const results = await createNodesFunction(
      ['proj/build.gradle'],
      {
        buildTargetName: 'build',
      },
      context
    );

    expect(results).toMatchInlineSnapshot(`
      [
        [
          "proj/build.gradle",
          {
            "projects": {
              "proj": {
                "metadata": {
                  "targetGroups": {
                    "Verification": [
                      "test",
                    ],
                  },
                  "technologies": [
                    "gradle",
                  ],
                },
                "name": "proj",
                "targets": {
                  "test": {
                    "cache": true,
                    "command": "./gradlew proj:test",
                    "dependsOn": [
                      "classes",
                    ],
                    "inputs": [
                      "default",
                      "^production",
                    ],
                    "metadata": {
                      "technologies": [
                        "gradle",
                      ],
                    },
                  },
                },
              },
            },
          },
        ],
      ]
    `);
  });

  it('should create nodes based on gradle for nested project root', async () => {
    gradleReport = {
      gradleFileToGradleProjectMap: new Map<string, string>([
        ['nested/nested/proj/build.gradle', 'proj'],
      ]),
      buildFileToDepsMap: new Map<string, string>(),
      gradleFileToOutputDirsMap: new Map<string, Map<string, string>>([
        ['nested/nested/proj/build.gradle', new Map([['build', 'build']])],
      ]),
      gradleProjectToTasksTypeMap: new Map<string, Map<string, string>>([
        ['proj', new Map([['test', 'Verification']])],
      ]),
      gradleProjectToProjectName: new Map<string, string>([['proj', 'proj']]),
    };
    await tempFs.createFiles({
      'nested/nested/proj/build.gradle': ``,
    });

    const results = await createNodesFunction(
      ['nested/nested/proj/build.gradle'],
      {
        buildTargetName: 'build',
      },
      context
    );

    expect(results).toMatchInlineSnapshot(`
      [
        [
          "nested/nested/proj/build.gradle",
          {
            "projects": {
              "nested/nested/proj": {
                "metadata": {
                  "targetGroups": {
                    "Verification": [
                      "test",
                    ],
                  },
                  "technologies": [
                    "gradle",
                  ],
                },
                "name": "proj",
                "targets": {
                  "test": {
                    "cache": true,
                    "command": "./gradlew proj:test",
                    "dependsOn": [
                      "classes",
                    ],
                    "inputs": [
                      "default",
                      "^production",
                    ],
                    "metadata": {
                      "technologies": [
                        "gradle",
                      ],
                    },
                  },
                },
              },
            },
          },
        ],
      ]
    `);
  });

  it('should create nodes with atomized tests targets based on gradle for nested project root', async () => {
    gradleReport = {
      gradleFileToGradleProjectMap: new Map<string, string>([
        ['nested/nested/proj/build.gradle', 'proj'],
      ]),
      buildFileToDepsMap: new Map<string, string>(),
      gradleFileToOutputDirsMap: new Map<string, Map<string, string>>([
        ['nested/nested/proj/build.gradle', new Map([['build', 'build']])],
      ]),
      gradleProjectToTasksTypeMap: new Map<string, Map<string, string>>([
        ['proj', new Map([['test', 'Test']])],
      ]),
      gradleProjectToProjectName: new Map<string, string>([['proj', 'proj']]),
    };
    await tempFs.createFiles({
      'nested/nested/proj/build.gradle': ``,
    });
    await tempFs.createFiles({
      'proj/src/test/java/test/rootTest.java': ``,
    });
    await tempFs.createFiles({
      'nested/nested/proj/src/test/java/test/test.java': ``,
    });
    await tempFs.createFiles({
      'nested/nested/proj/src/test/java/test/test1.java': ``,
    });

    const results = await createNodesFunction(
      [
        'nested/nested/proj/build.gradle',
        'proj/src/test/java/test/rootTest.java',
        'nested/nested/proj/src/test/java/test/test.java',
        'nested/nested/proj/src/test/java/test/test1.java',
      ],
      {
        buildTargetName: 'build',
        ciTargetName: 'test-ci',
      },
      context
    );

    expect(results).toMatchInlineSnapshot(`
      [
        [
          "nested/nested/proj/build.gradle",
          {
            "projects": {
              "nested/nested/proj": {
                "metadata": {
                  "targetGroups": {
                    "Test": [
                      "test-ci--test",
                      "test-ci--test1",
                      "test-ci",
                      "test",
                    ],
                  },
                  "technologies": [
                    "gradle",
                  ],
                },
                "name": "proj",
                "targets": {
                  "test": {
                    "cache": false,
                    "command": "./gradlew proj:test",
                    "dependsOn": [
                      "classes",
                    ],
                    "inputs": [
                      "default",
                      "^production",
                    ],
                    "metadata": {
                      "technologies": [
                        "gradle",
                      ],
                    },
                  },
                  "test-ci": {
                    "cache": true,
                    "dependsOn": [
                      {
                        "params": "forward",
                        "projects": "self",
                        "target": "test-ci--test",
                      },
                      {
                        "params": "forward",
                        "projects": "self",
                        "target": "test-ci--test1",
                      },
                    ],
                    "executor": "nx:noop",
                    "inputs": [
                      "default",
                      "^production",
                    ],
                    "metadata": {
                      "description": "Runs Gradle Tests in CI",
                      "nonAtomizedTarget": "test",
                      "technologies": [
                        "gradle",
                      ],
                    },
                  },
                  "test-ci--test": {
                    "cache": true,
                    "command": "./gradlew proj:test --tests test",
                    "dependsOn": [
                      "classes",
                    ],
                    "inputs": [
                      "default",
                      "^production",
                    ],
                    "metadata": {
                      "description": "Runs Gradle test nested/nested/proj/src/test/java/test/test.java in CI",
                      "technologies": [
                        "gradle",
                      ],
                    },
                  },
                  "test-ci--test1": {
                    "cache": true,
                    "command": "./gradlew proj:test --tests test1",
                    "dependsOn": [
                      "classes",
                    ],
                    "inputs": [
                      "default",
                      "^production",
                    ],
                    "metadata": {
                      "description": "Runs Gradle test nested/nested/proj/src/test/java/test/test1.java in CI",
                      "technologies": [
                        "gradle",
                      ],
                    },
                  },
                },
              },
            },
          },
        ],
      ]
    `);
  });
});
