import { lstat, readFile, readdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
export const sample = 'fixtures/instrument/measurement.csv';
export async function inspect(filename) {
  const status = await lstat(filename, { bigint: true });
  return { file: filename, kind: status.isSymbolicLink() ? 'link' : status.isFile() ? 'file' : 'other',
    size: String(status.size), inode: String(status.ino), device: String(status.dev),
    modifiedNs: String(status.mtimeNs), entries: (await readdir(dirname(filename))).sort() };
}
