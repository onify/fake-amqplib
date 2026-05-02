/* eslint-disable no-console */
import Mocha from 'mocha';

const mocha = new Mocha({ ui: 'bdd', reporter: 'spec' });
mocha.suite.emit('pre-require', globalThis, 'texample-readme', mocha);

let started = false;

function startMocha() {
  if (started) return Promise.resolve();
  started = true;
  return new Promise((resolve, reject) => {
    mocha.run((failures) => (failures ? reject(new Error(`${failures} mocha test(s) failed`)) : resolve()));
  });
}

globalThis.runMocha = startMocha;

process.once('beforeExit', () => {
  if (started) return;
  if (!mocha.suite.suites.length && !mocha.suite.tests.length) return;
  startMocha().catch((err) => {
    console.error(err.message);
    process.exitCode = 1;
  });
});
