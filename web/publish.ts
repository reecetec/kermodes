// Publishes the packages apps build on (@kermodes/client, @kermodes/react, @kermodes/theme) at
// the release's version: `bun publish.ts <version> <registry> [--dry-run]`, the token in
// NPM_TOKEN. The workspace imports their src/; what is published is a copy in .publish/ built
// for apps: JavaScript with its dependencies left to the app's bundler, type declarations, and
// a manifest stamped with the version, its workspace dependencies pinned to it.
import { $ } from 'bun';
import { cp, mkdir, rm } from 'node:fs/promises';
import path from 'node:path';

type Manifest = {
  name: string;
  license?: string;
  dependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  exports?: Record<string, string>;
};

const [version, registry] = Bun.argv.slice(2);
const dryRun = Bun.argv.includes('--dry-run');
if (version == null || registry == null || !/^\d+\.\d+\.\d+$/.test(version)) {
  throw new Error('usage: bun publish.ts <version> <registry> [--dry-run]');
}
const web = import.meta.dir;
const staging = path.join(web, '.publish');
await rm(staging, { recursive: true, force: true });

const read = async (dir: string) =>
  (await Bun.file(path.join(web, dir, 'package.json')).json()) as Manifest;
const pinned = (deps: Record<string, string> | undefined) =>
  deps == null
    ? undefined
    : Object.fromEntries(
        Object.entries(deps).map(([name, range]) => [
          name,
          range.startsWith('workspace:') ? version : range,
        ])
      );

/** A TypeScript package: bundled to dist/index.js and declared in dist/index.d.ts */
async function build(dir: string, entry: string, declarations: Record<string, string[]>) {
  const manifest = await read(dir);
  const out = path.join(staging, dir);
  const external = [
    ...Object.keys(manifest.dependencies ?? {}),
    ...Object.keys(manifest.peerDependencies ?? {}),
  ];
  const built = await Bun.build({
    entrypoints: [path.join(web, dir, entry)],
    outdir: path.join(out, 'dist'),
    naming: 'index.js',
    target: 'browser',
    format: 'esm',
    // React's production JSX runtime, not the development one
    define: { 'process.env.NODE_ENV': '"production"' },
    jsx: { development: false },
    external: [...external, 'react/jsx-runtime'],
  });
  if (!built.success) {
    throw new AggregateError(built.logs, `${manifest.name} did not build`);
  }
  // Declarations from the package's own source; packages it depends on are read from their
  // published declarations, so none of their source is emitted with it.
  const tsconfig = path.join(out, 'tsconfig.json');
  await Bun.write(
    tsconfig,
    JSON.stringify({
      extends: path.join(web, dir, 'tsconfig.json'),
      compilerOptions: {
        noEmit: false,
        declaration: true,
        emitDeclarationOnly: true,
        rootDir: path.join(web, dir, 'src'),
        outDir: path.join(out, 'dist'),
        paths: declarations,
      },
      include: [path.join(web, dir, 'src')],
      exclude: [path.join(web, dir, 'src/**/*.test.ts')],
    })
  );
  await $`bunx tsc -p ${tsconfig}`.cwd(web);
  await rm(tsconfig);
  await Bun.write(
    path.join(out, 'package.json'),
    JSON.stringify(
      {
        name: manifest.name,
        version,
        license: manifest.license,
        type: 'module',
        main: './dist/index.js',
        types: './dist/index.d.ts',
        exports: { '.': { types: './dist/index.d.ts', import: './dist/index.js' } },
        files: ['dist'],
        dependencies: pinned(manifest.dependencies),
        peerDependencies: manifest.peerDependencies,
      },
      null,
      2
    )
  );
  return out;
}

/** A package of plain files, published as they are */
async function copy(dir: string, files: string[]) {
  const manifest = await read(dir);
  const out = path.join(staging, dir);
  await mkdir(out, { recursive: true });
  for (const file of files) {
    await cp(path.join(web, dir, file), path.join(out, file));
  }
  await Bun.write(
    path.join(out, 'package.json'),
    JSON.stringify(
      {
        name: manifest.name,
        version,
        license: 'Apache-2.0',
        type: 'module',
        exports: manifest.exports,
        files,
      },
      null,
      2
    )
  );
  return out;
}

const client = await build('client', 'src/index.ts', {});
const react = await build('react', 'src/index.tsx', {
  '@kermodes/client': [path.join(client, 'dist/index.d.ts')],
});
const theme = await copy('theme', ['tokens.css', 'tailwind.css']);

const host = registry.replace(/^https?:/, '');
for (const out of [client, react, theme]) {
  await Bun.write(
    path.join(out, '.npmrc'),
    `@kermodes:registry=${registry}\n${host}:_authToken=\${NPM_TOKEN}\n`
  );
  // A rerun of a release finds its version there already
  await $`bun publish --tolerate-republish ${dryRun ? ['--dry-run'] : []}`.cwd(out);
}
