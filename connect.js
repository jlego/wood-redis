/**
 * Wood Plugin Module.
 * redis连接
 * by jlego on 2018-11-22
 */
const Redis = require('./src/redis');

module.exports = async (app = {}, config = {}) => {
  for (let key in config) {
    await Redis.connect(config[key], key);
  }
  return app;
}
