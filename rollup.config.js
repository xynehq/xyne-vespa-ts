import { nodeResolve } from '@rollup/plugin-node-resolve';

const external = ['p-limit', 'zod'];

const createConfig = (input, outputFile) => ({
  input,
  output: [
    {
      file: outputFile.replace('.js', '.esm.js'),
      format: 'es'
    },
    {
      file: outputFile.replace('.js', '.cjs'),
      format: 'cjs'
    }
  ],
  plugins: [
    nodeResolve({
      preferBuiltins: false
    })
  ],
  external,
  watch: {
    include: 'dist/**',
    exclude: 'node_modules/**'
  }
});

export default [
  createConfig('dist/index.js', 'dist/index.js'),
  createConfig('dist/src/types.js', 'dist/types.js'),
  createConfig('dist/src/mappers/index.js', 'dist/mappers/index.js'),
  createConfig('dist/src/utils/index.js', 'dist/utils/index.js'),
  createConfig('dist/src/errors/index.js', 'dist/errors/index.js'),
  createConfig('dist/src/client/index.js', 'dist/client/index.js')
];
