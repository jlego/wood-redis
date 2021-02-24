// redis操作方法类
// by YuRonghui 2018-11-22
const redis = require("ioredis");
const RedLock = require('redlock-node');
const calculateSlot = require('cluster-key-slot');
// const { Util } = require('wood-util')();
let dbs = {}, redlock = null;

class Redis {
  constructor(tableName, dbName = 'master') {
    this.tableName = tableName;
    this.dbName = dbName;
  }
  
  getKey(key) {
    return `${this.dbName}:${this.tableName}:${key}`;
  }
  // 新行id
  rowid() {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].incr(this.getKey('rowid'), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 设单值
  setValue(key, value, ttl) {
    return new Promise((resolve, reject) => {
      let _key = this.getKey(key),
        client = dbs[this.dbName];
      if (ttl) {
        client.set(_key, value, 'EX', ttl, (err, res) => {
          if (err) reject(err);
          resolve(res);
        });
      } else {
        client.set(_key, value, (err, res) => {
          if (err) reject(err);
          resolve(res);
        });
      }
    });
  }
  // 取单值
  getValue(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].get(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 设值
  setHashValue(key, field, value, ttl) {
    return new Promise((resolve, reject) => {
      let _key = this.getKey(key),
        client = dbs[this.dbName];
      client.hset(_key, field, value, (err, res) => {
        if (err) reject(err);
        if (ttl) client.expire(_key, ttl);
        resolve(res);
      });
    });
  }
  // 取值
  getHashValue(key, field) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].hget(this.getKey(key), field, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 删除值
  removeHashValue(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].hdel(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 是否有值
  isHashExist(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].hexists(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  // 设置多个field 
  setMultiHashValue(key, obj, ttl) {
    return new Promise((resolve, reject) => {
      let _key = this.getKey(key),
        client = dbs[this.dbName];
      client.hmset(_key, obj, (err, res) => {
        if (err) reject(err);
        if (ttl) client.expire(_key, ttl);
        resolve(res);
      });
    });
  }

  // 获取多个field 
  getMultiHashValue(key, fields) {
    return new Promise((resolve, reject) => {
      let _key = this.getKey(key),
        client = dbs[this.dbName];
      client.hmget(_key, fields, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  getAllHashValues(key) {
    return new Promise((resolve, reject) => {
      let _key = this.getKey(key),
        client = dbs[this.dbName];
      client.hgetall(_key, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  // 表锁
  lock(timeout = 1) {
    let that = this;
    return new Promise(async (resolve, reject) => {
      let hasLock = await WOOD.catchErr(that.hasLock());
      if (hasLock.err) {
        reject(hasLock.err);
      } else {
        if (!hasLock.data) {
          redlock.lock(this.getKey('lock'), timeout, (err, lockInstance) => {
            if (lockInstance === null) {
              setTimeout(async () => {
                let result = await WOOD.catchErr(that.lock(timeout));
                if (result.err) {
                  reject(result.err);
                } else {
                  resolve(result.data);
                }
              }, 20);
            } else {
              resolve(lockInstance);
            }
          });
        } else {
          await this.lock(timeout);
        }
      }
    });
  }
  // 解锁
  unlock(lockInstance) {
    return new Promise((resolve, reject) => {
      redlock.unlock(lockInstance, (err, lockInstance) => {
        if (err) reject(err);
        resolve(!!lockInstance);
      });
    });
  }
  // 是否有锁
  hasLock() {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].get(this.getKey('lock'), (err, res) => {
        if (err) reject(err);
        resolve(!!res);
      });
    });
  }
  // key是否存在
  existKey(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].exists(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 删除key
  delKey(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].del(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // key过期
  setKeyTimeout(key, timeout) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].expire(this.getKey(key), timeout, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  brpop(key, times = 0) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].brpop(this.getKey(key), times, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表记录总数
  listCount(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].llen(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表添加记录
  listPush(key, values) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].rpush(this.getKey(key), values, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表截取记录
  listSlice(key, indexstart, indexend) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].lrange(this.getKey(key), indexstart, indexend, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 删除列表某记录
  listRemove(key, count, value) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].lrem(this.getKey(key), count, value, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  sadd(key, data) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].sadd(this.getKey(key), data, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  sismember(key, data) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].sismember(this.getKey(key), data, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  srem(key, data) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].srem(this.getKey(key), data, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  scard(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].scard(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  smembers(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].smembers(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  hmset(key, value) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].hmset(this.getKey(key), value, (err, res) => {
        if (err) reject(err);
        resolve(res);
      })
    })
  }

  // 列表清除记录
  listClear(key) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].ltrim(this.getKey(key), 1, 0, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // zadd，暂时只支持单条添加
  zadd(score, value) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].zadd(this.getKey(), score, value, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  // multi
  multi() {
    return dbs[this.dbName].multi();
  }

  // exec
  exec() {
    return dbs[this.dbName].exec();
  }

  scan(cursor, match, count) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].scan(cursor, 'Match', match, 'Count', count, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }

  pipeline() {
    return dbs[this.dbName].pipeline();
  }

  calcKeysSlot(keys) {
    let group = {};
    if (!Array.isArray(keys)) return group;
    for (let i of keys) {
      const slot = calculateSlot(i);
      if (!group[slot]) group[slot] = [];
      group[slot].push(i);
    }
    return group;
  }

  zrem(value) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].zrem(this.getKey(), value, (err, res) => {
        if (err) reject(err);
        resolve(res);
      })
    })
  }

  zrange(start, stop) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].zrange(this.getKey(), start, stop, (err, res) => {
        if (err) reject(err);
        resolve(res);
      })
    })
  }

  zrangeByScore(min, max) {
    return new Promise((resolve, reject) => {
      dbs[this.dbName].zrangebyscore(this.getKey(), min, max, (err, res) => {
        if (err) reject(err);
        resolve(res);
      })
    })
  }

  static connect(opts = {}, dbName = 'master', callback) {
    if (Array.isArray(opts)) {
      dbs[dbName] = new redis.Cluster(opts, {
        scaleReads: 'slave'
      });
    } else {
      // 'redis://:authpassword@127.0.0.1:6380/4'
      dbs[dbName] = new redis(opts);
    }
    dbs[dbName].on('connect', () => {
      console.log(`Redis [${dbName}] connected Successfull`);
      redlock = new RedLock(dbs[dbName]);
      if (callback) callback(dbs[dbName]);
    });
    dbs[dbName].on('error', (error) => {
      console.log(`Redis [${dbName}] proxy error: ${error}`);
      if (callback) callback(error, dbs[dbName]);
    });
  }
  static close(dbName = 'master') {
    if (dbName) dbs[dbName].quit();
  }

  static getConnect(dbName = 'master') {
    return dbs[dbName];
  }
}

module.exports = Redis;
