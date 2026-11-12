import { mkdir as ensureFixtureDirectory } from 'node:fs/promises';
await ensureFixtureDirectory('fixtures', {recursive:true});

import { mkdir, writeFile, link, symlink } from 'node:fs/promises';
await mkdir('fixtures/instrument', { recursive: true });
await writeFile('fixtures/instrument/measurement.csv', 'probe,value\nP1,3.1\n');
await link('fixtures/instrument/measurement.csv', 'fixtures/instrument/archive.csv');
await writeFile('fixtures/outside.csv', '不在授权根目录\n');
await symlink('../outside.csv', 'fixtures/instrument/external.csv');
