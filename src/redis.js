// redis操作方法类
// by YuRonghui 2018-2-1
const redis = require("redis");
const RedLock = require('redlock-node');
const { Util } = require('wood-util')();
let db = null, redlock = null;

class Redis {
  constructor(tbname) {
    this.tbname = tbname;
    if(!db) throw Util.error('redis failed: db=null');
  }
  getKey(key){
    let str = `${WOOD.config.projectName}:${this.tbname}`;
    return key ? `${str}:${key}` : str;
  }
  // 新行id
  rowid() {
    return new Promise((resolve, reject) => {
      db.incr(this.getKey('rowid'), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 设单值
  setValue(key, value) {
    return new Promise((resolve, reject) => {
      db.set(this.getKey(key), value, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 取单值
  getValue(key) {
    return new Promise((resolve, reject) => {
      db.get(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  GetValue(key, callback) {
    db.get(key, function (err, res) {
      if (callback) {
        callback(err, key, res);
      }
    });
  }
  // 表锁
  lock(timeout = 1) {
    let that = this;
    return new Promise(async (resolve, reject) => {
      let hasLock = await Util.catchErr(that.hasLock());
      if(hasLock.err){
        reject(hasLock.err);
      }else{
        if(!hasLock.data){
          redlock.lock(this.getKey('lock'), timeout, (err, lockInstance) => {
            if (lockInstance === null) {
              setTimeout(async () => {
                let result = await Util.catchErr(that.lock(timeout));
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
      db.get(this.getKey('lock'), (err, res) => {
        if (err) reject(err);
        resolve(!!res);
      });
    });
  }
  // key是否存在
  existKey(key) {
    return new Promise((resolve, reject) => {
      db.exists(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 删除key
  delKey(key) {
    return new Promise((resolve, reject) => {
      db.del(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  DelKey(key, callback) {
    db.del(key, function (err, res) {
      // 删除成功，返回1，否则返回0(对于不存在的键进行删除操作，同样返回0)
      if (callback) {
        callback(err, key, res);
      }
    });
  }
  // key过期
  setKeyTimeout(key, timeout) {
    return new Promise((resolve, reject) => {
      db.expire(this.getKey(key), timeout, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  SetValue_Timeout(key, timeout, callback) {
    db.expire(key, timeout, function (err, res) {
      // 成功，返回1，否则返回0
      if (callback) {
        callback(err, key, res);
      }
    });
  }
  // 列表记录总数
  listCount(key) {
    return new Promise((resolve, reject) => {
      db.llen(this.getKey(key), (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表添加记录
  listPush(key, values) {
    return new Promise((resolve, reject) => {
      db.rpush(this.getKey(key), values, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表截取记录
  listSlice(key, indexstart, indexend) {
    return new Promise((resolve, reject) => {
      db.lrange(this.getKey(key), indexstart, indexend, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  // 列表清除记录
  listClear(key) {
    return new Promise((resolve, reject) => {
      db.ltrim(this.getKey(key), -1, 0, (err, res) => {
        if (err) reject(err);
        resolve(res);
      });
    });
  }
  static connect(opts = {}, onConnect, onError) {
    if(!opts.db) opts.db = opts.dbnum;
    Redis.db = db = redis.createClient(opts.port, opts.host);
    db.select(opts.db, function () {
      console.log('Redis select', opts.db, 'db');
    });
    db.on('connect', () => {
      console.log('Redis connected Successfull');
      redlock = new RedLock(db);
      if(onConnect) onConnect(db);
    });
    db.on('error', (error) => {
      console.log('Redis proxy error:' + error);
      if(onError) onError(error, db);
    });
  }
  static close() {
    db.quit();
  }
}

module.exports = Redis;
