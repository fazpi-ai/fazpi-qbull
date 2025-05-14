import RedisClient from './RedisClient.js';
// Renamed import to avoid collision with the 'customRedisConfig' parameter
import configModule from '../config.js';
import Logger from './Logger.js';

const logger = new Logger('RedisSingleton');

class RedisSingleton {
  constructor() {
    if (RedisSingleton._instance) {
      return RedisSingleton._instance;
    }
    // logger.info('RedisSingleton (structure) created.'); // Constructor log is optional
    // Do not instantiate RedisClient here directly
    this.redisClientInstance = null; // Will hold our RedisClient wrapper instance
    this._connectionPromise = null;
    this.currentConfig = null; // Will store the configuration used for the current connection
    RedisSingleton._instance = this;
  }

  static getInstance() {
    if (!this._instance) {
      // The constructor will be called here if _instance does not exist
      new RedisSingleton();
    }
    return this._instance;
  }

  /**
   * Superficially compares two Redis configuration objects.
   * @param {object|null} configA
   * @param {object|null} configB
   * @returns {boolean}
   */
  _areConfigsEqual(configA, configB) {
    if (configA === configB) return true; // Same instance or both null/undefined
    if (!configA || !configB) return false; // One is null/undefined and the other is not

    return configA.host === configB.host &&
           configA.port === configB.port &&
           (configA.user || undefined) === (configB.user || undefined) && // Normalize undefined/null
           (configA.password || undefined) === (configB.password || undefined) && // Normalize undefined/null
           configA.db === configB.db;
  }

  /**
   * Connects to Redis.
   * @param {object} [customRedisConfig=null] - Optional Redis configuration: { host, port, user, password, db }.
   * If null, environment variables will be used (via configModule.js).
   */
  async connect(customRedisConfig = null) {
    const newConfigToUse = customRedisConfig || {
      host: configModule.REDIS_HOST,
      port: configModule.REDIS_PORT,
      user: configModule.REDIS_USER,
      password: configModule.REDIS_PASSWORD,
      db: configModule.REDIS_DB,
    };

    // If already connected with the same configuration, do nothing.
    if (this.redisClientInstance && this.redisClientInstance.isConnected && this._areConfigsEqual(this.currentConfig, newConfigToUse)) {
      logger.info('RedisSingleton: Already connected to Redis with the same configuration.');
      return this._connectionPromise; // Return the existing connection promise (already resolved)
    }

    // If a connection promise is in progress with the same configuration, and the client is connecting, return it.
    if (this._connectionPromise && this.redisClientInstance && this.redisClientInstance.isConnecting && this._areConfigsEqual(this.currentConfig, newConfigToUse)) {
      logger.info('RedisSingleton: Connection with the same configuration already in progress. Awaiting its completion.');
      return this._connectionPromise;
    }

    // If an existing client is present (possibly with different config or disconnected), disconnect it.
    if (this.redisClientInstance) {
      logger.info('RedisSingleton: Different configuration or reconnection requested. Disconnecting existing client...');
      await this.redisClientInstance.disconnect();
      // No need to nullify this.redisClientInstance or this.currentConfig here yet,
      // they will be overwritten or handled in case of error in the new connection.
      this._connectionPromise = null; // Clearing the previous promise is important
    }

    // Create and connect the new client
    this.currentConfig = { ...newConfigToUse }; // Save a copy of the current configuration
    logger.info(`RedisSingleton: Initiating new connection to Redis. Host: ${this.currentConfig.host}, Port: ${this.currentConfig.port}, DB: ${this.currentConfig.db}`);

    this.redisClientInstance = new RedisClient(
      this.currentConfig.host,
      this.currentConfig.port,
      this.currentConfig.user,
      this.currentConfig.password,
      this.currentConfig.db
    );

    this._connectionPromise = this.redisClientInstance.connect()
      .then(() => {
        logger.info('RedisSingleton: Connection to Redis successful.');
        // The promise resolves, _connectionPromise will keep it resolved.
      })
      .catch((err) => {
        logger.error(`RedisSingleton: Failed to connect to Redis: ${err.message}`, { err });
        // this.redisClientInstance = null; // Optional: nullify if connection fails catastrophically
        // this.currentConfig = null; // Optional: nullify if connection fails
        // Important: _connectionPromise is cleared here to allow a new attempt
        this._connectionPromise = null;
        throw err; // Re-throw so the caller is aware
      });

    return this._connectionPromise;
  }

  async disconnect() {
    if (this.redisClientInstance) {
      logger.info('RedisSingleton: Initiating disconnection from Redis.');
      await this.redisClientInstance.disconnect();
      logger.info('RedisSingleton: Disconnection complete.');
    } else {
      logger.info('RedisSingleton: No active Redis client to disconnect.');
    }
    // Reset the singleton's state regarding the connection
    this.redisClientInstance = null;
    this.currentConfig = null;
    this._connectionPromise = null;
  }

  async _ensureConnected() {
    // If no client instance OR the instance is not connected AND there's no active connection promise
    if (!this.redisClientInstance || (!this.redisClientInstance.isConnected && !this._connectionPromise)) {
      logger.error('RedisSingleton: Not connected to Redis. You must call redisSingleton.connect(config?) first.');
      throw new Error('RedisSingleton: Not connected to Redis. You must call redisSingleton.connect(config?) first.');
    }
    // If there's a connection promise (it might be connecting or already resolved), await it.
    if (this._connectionPromise) {
        await this._connectionPromise; // Wait for the current connection attempt to complete or fail
    }
    // Check again after awaiting the promise, in case it failed
    if (!this.redisClientInstance || !this.redisClientInstance.isConnected) {
        logger.error('RedisSingleton: Redis connection is not active after awaiting the connection promise.');
        throw new Error('RedisSingleton: Redis connection is not active.');
    }
  }

  async set(key, value) {
    await this._ensureConnected();
    return this.redisClientInstance.set(key, value);
  }

  async get(key) {
    await this._ensureConnected();
    return this.redisClientInstance.get(key);
  }

  async publishToStream(streamName, jobData) {
    await this._ensureConnected();
    return this.redisClientInstance.publishToStream(streamName, jobData);
  }

  getRawClient() {
    if (!this.redisClientInstance) {
        logger.warn('RedisSingleton: Attempted to get raw client, but RedisClient instance has not been created (call connect() first).');
        return null;
    }
    // if (!this.redisClientInstance.isConnected) {
    //     logger.warn('RedisSingleton: Attempted to get raw client, but it is not connected. Current status: ' + (this.redisClientInstance.getClientInstance()?.status || 'unknown'));
    // }
    return this.redisClientInstance.getClientInstance();
  }
}

const redisSingletonInstance = RedisSingleton.getInstance();
export default redisSingletonInstance;