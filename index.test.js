const assert = require('assert');
const libEvents = require('./index.js');

it('should have dispatch fun', function() {
  expect(libEvents.dispatch).toBeDefined()
});

it('should have queue poll fun', function() {
  expect(libEvents.poll).toBeDefined()
});
