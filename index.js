coffee = require('coffee-script');
var index = null;
if (process.env.PLAYLYFE_TEST) {
  try {
    index = require('./src-cov/src/oceanus');
  } catch(e) {
    index = require('./src/oceanus');
  }
} else {
  index = require('./src/oceanus');
}
module.exports = index;
