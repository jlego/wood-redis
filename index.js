/**
 * Wood Plugin Module.
 * redis
 * by jlego on 2018-11-18
 */
const Redis = require('./src/redis');

module.exports = (app, config = {}) => {
  if(app){
    app.Redis = Redis;
    for (let key in config) {
      Redis.connect(config[key]);
    }
  }
  return Redis;
}
