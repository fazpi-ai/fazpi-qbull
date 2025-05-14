import RedisClient from './RedisClient.js';
import config from '../config.js';
import Logger from './Logger.js';

const logger = new Logger('RedisSingleton');

class RedisSingleton {
  constructor() {
    if (RedisSingleton._instance) {
      return RedisSingleton._instance;
    }

    logger.info('Creating new RedisSingleton instance.');
    this.redisClient = new RedisClient(
      config.REDIS_HOST,
      config.REDIS_PORT,
      config.REDIS_USER,
      config.REDIS_PASSWORD,
      config.REDIS_DB
    );
    this._connectionPromise = null;
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
    this._connectionPromise = this.redisClient.connect()
      .then(() => {
        logger.info('RedisSingleton: Connection successful.');
      })
      .catch((err) => {
        logger.error(`RedisSingleton: Connection failed: ${err.message}`, err);
        this._connectionPromise = null;
        throw err;
      });

    return this._connectionPromise;
  }

  async disconnect() {
    logger.info('RedisSingleton initiating disconnection from Redis.');
    // We do not need _ensureConnected here, disconnect should work even if not connected.
    await this.redisClient.disconnect();
    this._connectionPromise = null; // Clean the connection promise in the disconnect
    logger.info('RedisSingleton: Disconnection complete.');
  }

  async _ensureConnected() {
    if (!this.redisClient.isConnected) {
      // If there is a connection promise in progress, wait for it.
      // If not, start a new connection.
      if (this._connectionPromise) {
        logger.info('Singleton: Connection in progress. Awaiting...');
        await this._connectionPromise;
      } else {
        logger.info('Singleton: Connection not active. Attempting to connect...');
        await this.connect();
      }
    }
    // After attempting to connect, check again.
    if (!this.redisClient.isConnected) {
        logger.error('Singleton: Failed to establish Redis connection after attempt.');
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
    return this.redisClient.publishToStream(streamName, jobData);
  }

  /**
   * Returns the raw ioredis client instance.
   * Useful for operations not covered by the wrapper or to pass to libraries.
   * The caller is responsible for checking if the client is connected if needed.
   * @returns {object|null} The ioredis instance or null if not initialized.
   */
  getRawClient() {
    // We do not need _ensureConnected here, we just return the client.
    // The caller is responsible for handling the connection if needed.
    if (!this.redisClient || !this.redisClient.getClientInstance()) {
        logger.warn('Attempted to get raw client, but Redis client or its instance is not initialized.');
        return null;
    }
    // We could add a check of this.redisClient.isConnected if we want to be more strict
    // if (!this.redisClient.isConnected) {
    //     logger.warn('Attempted to get raw client, but Redis is not connected.');
    // }
    return this.redisClient.getClientInstance();
  }
}

// Export the instance directly to ensure Singleton behavior
const redisSingletonInstance = RedisSingleton.getInstance();
export default redisSingletonInstance;