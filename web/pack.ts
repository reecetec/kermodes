// Packs the packages apps build on (@kermodes/client, grid, react, theme) at a build's version:
// `bun pack.ts <version>` writes .publish/kermodes-<name>-<version>.tgz. The workspace imports
// their src/; what is packed is a copy built for apps: JavaScript with its dependencies left to
// the app's bundler, type declarations, and a manifest stamped with the version, its workspace
// dependencies pinned to it. CI packs once per build; .github/workflows/release.yml publishes
// those tarballs (a release relabels them first, ci/relabel.py).
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

const [version] = Bun.argv.slice(2);
// SemVer, with a candidate's pre-release label (ci/version)
if (version == null || !/^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)) {
  throw new Error('usage: bun pack.ts <version>');
}
const web = import.meta.dir;
const staging = path.join(web, '.publish');
await rm(staging, { recursive: true, force: true });

// npm checks a package's provenance against the repository it names
const repository = (dir: string) => ({
  type: 'git',
  url: 'git+https://github.com/reecetec/kermodes.git',
  directory: `web/${dir}`,
});
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

/**
 * A TypeScript package: each of `entries` (export path to source) bundled to dist/<name>.js,
 * sharing what they have in common through chunks (one React context, not one per entry), and
 * declared in dist/<name>.d.ts.
 */
async function build(
  dir: string,
  entries: Record<string, string>,
  declarations: Record<string, string[]>
) {
  const manifest = await read(dir);
  const out = path.join(staging, dir);
  const external = [
    ...Object.keys(manifest.dependencies ?? {}),
    ...Object.keys(manifest.peerDependencies ?? {}),
  ];
  const built = await Bun.build({
    entrypoints: Object.values(entries).map(entry => path.join(web, dir, entry)),
    outdir: path.join(out, 'dist'),
    naming: { entry: '[name].[ext]', chunk: 'chunk-[hash].[ext]' },
    splitting: true,
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
  // Each entry's file, by the export path it is published under
  const exports = Object.fromEntries(
    Object.entries(entries).map(([exported, entry]) => {
      const name = path.basename(entry).replace(/\.tsx?$/, '');
      return [exported, { types: `./dist/${name}.d.ts`, import: `./dist/${name}.js` }];
    })
  );
  await Bun.write(
    path.join(out, 'package.json'),
    JSON.stringify(
      {
        name: manifest.name,
        version,
        license: manifest.license,
        repository: repository(dir),
        type: 'module',
        main: './dist/index.js',
        types: './dist/index.d.ts',
        exports,
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

/** A package of plain files and directories, published as they are */
async function copy(dir: string, files: string[]) {
  const manifest = await read(dir);
  const out = path.join(staging, dir);
  await mkdir(out, { recursive: true });
  for (const file of files) {
    await cp(path.join(web, dir, file), path.join(out, file), { recursive: true });
  }
  await Bun.write(
    path.join(out, 'package.json'),
    JSON.stringify(
      {
        name: manifest.name,
        version,
        license: 'Apache-2.0',
        repository: repository(dir),
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

const client = await build('client', { '.': 'src/index.ts' }, {});
const clientTypes = { '@kermodes/client': [path.join(client, 'dist/index.d.ts')] };
// The platform's own table viewer, which @kermodes/react's <Table> draws with
const grid = await build('grid', { '.': 'src/index.ts' }, clientTypes);
const react = await build(
  'react',
  { '.': 'src/index.tsx', './table': 'src/table.tsx' },
  { ...clientTypes, '@kermodes/grid': [path.join(grid, 'dist/index.d.ts')] }
);
const theme = await copy('theme', ['tokens.css', 'fonts.css', 'fonts', 'layout.css', 'tailwind.css']);

for (const out of [client, grid, react, theme]) {
  await $`bun pm pack --destination ${staging} --quiet`.cwd(out);
}
