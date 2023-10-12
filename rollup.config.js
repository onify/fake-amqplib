import commonjs from '@rollup/plugin-commonjs';

export default {
  input: './index.js',
  plugins: [
    commonjs({ sourceMap: false }),
  ],
  external: [
    'smqp',
    'events',
    'url',
  ],
  output: [
    {
      exports: 'named',
      file: 'main.cjs',
      format: 'cjs',
    },
  ],
};
