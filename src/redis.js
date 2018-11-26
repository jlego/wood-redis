// redis操作方法类
// by YuRonghui 2018-11-22
const redis = require("redis");
const RedLock = require('redlock-node');
const { Util } = require('wood-util')();
let dbs = {}, redlock = null;

class Redis {
  constructor(tbname, db = 'master', ctx) {
    this.tbname = tbname;
    this.db = dbs[db];
    this.ctx = ctx;
    if(!this.db) throw this.ctx.error('redis failed: db=null');
  }
  getKey(key){
    let str = `${this.ctx.config.projectName}:${this.tbname}`;
    return key ? `${str}:${key}` : str;
  }
  // 新行id
  rowid() {
    return new Promise((resolve, reject) => {
      this.db.incr(this.getKey('rowid'), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 设单值
  setValue(key, value) {
    return new Promise((resolve, reject) => {
      this.db.set(this.getKey(key), value, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 取单值
  getValue(key) {
    return new Promise((resolve, reject) => {
      this.db.Hget(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 设值
  setHaseValue(key, value) {
    return new Promise((resolve, reject) => {
      this.db.Hset(this.getKey(key), value, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 取值
  getHaseValue(key) {
    return new Promise((resolve, reject) => {
      this.db.get(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 是否有值
  isHaseExist(key) {
    return new Promise((resolve, reject) => {
      this.db.Hexists(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 表锁
  lock(timeout = 1) {
    let that = this;
    return new Promise(async (resolve, reject) => {
      let hasLock = await this.ctx.catchErr(that.hasLock());
      if(hasLock.err){
        reject(hasLock.err);
      }else{
        if(!hasLock.data){
          redlock.lock(this.getKey('lock'), timeout, (err, lockInstance) => {
            if (lockInstance === null) {
              setTimeout(async () => {
                let result = await this.ctx.catchErr(that.lock(timeout));
                if(result.err){
                  reject(result.err);
                }else{
                  resolve(result.data);
                }
              }, 20);
            } else {
              resolve(lockInstance);
            }
          });
        }else{
          resolve(lockInstance);
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
      this.db.get(this.getKey('lock'), (err, res) => {
        if (err) reject(err);
        resolve(!!res);
      });
    });
  }
  // key是否存在
  existKey(key) {
    return new Promise((resolve, reject) => {
      this.db.exists(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 删除key
  delKey(key) {
    return new Promise((resolve, reject) => {
      this.db.del(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // key过期
  setKeyTimeout(key, timeout) {
    return new Promise((resolve, reject) => {
      this.db.expire(this.getKey(key), timeout, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  brpop(key, times = 0) {
    return new Promise((resolve, reject) => {
      this.db.brpop(this.getKey(key), times, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表记录总数
  listCount(key) {
    return new Promise((resolve, reject) => {
      this.db.llen(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表添加记录
  listPush(key, values) {
    return new Promise((resolve, reject) => {
      this.db.rpush(this.getKey(key), values, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表截取记录
  listSlice(key, indexstart, indexend) {
    return new Promise((resolve, reject) => {
      this.db.lrange(this.getKey(key), indexstart, indexend, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表清除记录
  listClear(key) {
    return new Promise((resolve, reject) => {
      this.db.ltrim(this.getKey(key), -1, 0, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  static connect(opts = {}, name = 'master', callback) {
    if(!opts.db) opts.db = opts.dbnum;
    dbs[name] = redis.createClient(opts.port, opts.host);
    dbs[name].select(opts.db, function () {
      console.log('Redis select', opts.db, 'db');
    });
    dbs[name].on('connect', () => {
      console.log('Redis connected Successfull');
      redlock = new RedLock(dbs[name]);
      if(callback) callback(dbs[name]);
    });
    dbs[name].on('error', (error) => {
      console.log('Redis proxy error:' + error);
      if(callback) callback(error, dbs[name]);
    });
  }
  static close(name) {
    if(name) dbs[name].quit();
  }
}

module.exports = Redis;
