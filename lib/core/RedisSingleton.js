import RedisClient from './RedisClient.js';
import config from '../config.js';
import Logger from './Logger.js';

const logger = new Logger('RedisSingleton');

class RedisSingleton {
  constructor() {
    if (RedisSingleton._instance) {
      logger.debug('RedisSingleton instance already exists. Returning existing instance.');
      return RedisSingleton._instance;
    }

    logger.info('Creating new RedisSingleton instance.');
    this.redisClient = new RedisClient(
      config.REDIS_HOST,
      config.REDIS_PORT,
      config.REDIS_USER,
      config.REDIS_PASSWORD
    );
    this._connectionPromise = null; // Manages the promise of an active connection attempt
    RedisSingleton._instance = this;
  }

  static getInstance() {
    if (!this._instance) {
      this._instance = new RedisSingleton();
    }
    return this._instance;
  }

  async connect() {
    if (this.redisClient.isConnected) {
      logger.info('Redis client is already connected via Singleton.');
      return Promise.resolve();
    }

    if (this._connectionPromise) {
      logger.info('Connection attempt already in progress by Singleton. Awaiting its completion.');
      return this._connectionPromise;
    }

    logger.info('RedisSingleton initiating connection to Redis.');
    // THIS LINE (and others below) needs to assign to this._connectionPromise
    this._connectionPromise = this.redisClient.connect()
      .then(() => {
        logger.info('RedisSingleton: Connection successful.');
        // _connectionPromise can remain holding the resolved promise or be set to null
        // depending on desired re-connect logic. For now, it has served its purpose for this attempt.
      })
      .catch((err) => {
        logger.error(`RedisSingleton: Connection failed: ${err.message}`, err);
        this._connectionPromise = null; // Allow a new attempt if this one failed
        throw err;
      });
    
    return this._connectionPromise;
  }

  async disconnect() {
    logger.info('RedisSingleton initiating disconnection from Redis.');
    await this.redisClient.disconnect();
    this._connectionPromise = null; // Clear connection promise on disconnect
    logger.info('RedisSingleton: Disconnection complete.');
  }

  async _ensureConnected() {
    if (!this.redisClient.isConnected) {
      logger.info('Singleton: Connection not active. Attempting to connect...');
      await this.connect(); 
    }
    if (!this.redisClient.isConnected) {
        logger.error('Singleton: Failed to establish Redis connection.');
        throw new Error('Redis connection could not be established by Singleton.');
    }
  }

  async set(key, value) {
    await this._ensureConnected();
    return this.redisClient.set(key, value);
  }

  async get(key) {
    await this._ensureConnected();
    return this.redisClient.get(key);
  }

  async publishToStream(streamName, jobData) {
    await this._ensureConnected();
    logger.info(`RedisSingleton: Publishing to stream '${streamName}'.`);
    return this.redisClient.publishToStream(streamName, jobData);
  }

  getRawClient() {
    if (!this.redisClient.isConnected || !this.redisClient.getClientInstance()) {
        logger.warn('Attempted to get raw client, but Redis is not connected.');
        return null;
    }
    return this.redisClient.getClientInstance();
  }
}

const redisSingletonInstance = RedisSingleton.getInstance();

export default redisSingletonInstance;