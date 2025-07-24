// backend/utils/redisClient.js
const Redis = require('ioredis');

const redisMaster = new Redis({
  host: '3.36.91.119',
  port: 6379,

  connectTimeout: 5000,
  retryStrategy: times => Math.min(times * 100, 3000),
});

const redisSlaves = [
  new Redis({ host: '3.36.87.186', port: 6379, connectTimeout: 5000 }),
  new Redis({ host: '3.34.191.158', port: 6379, connectTimeout: 5000 }),
];

let currentSlave = 0;
function getReadRedis() {
  const redis = redisSlaves[currentSlave];
  currentSlave = (currentSlave + 1) % redisSlaves.length;
  return redis;
}

const pubClient = new Redis({
  host: '3.36.91.119',
  port: 6379,
  connectTimeout: 5000,
});

const subClient = new Redis({
  host: '3.36.91.119',
  port: 6379,
  lazyConnect: true,
  connectTimeout: 5000,
});

module.exports = {
  redisMaster,
  getReadRedis,
  pubClient,
  subClient,
};