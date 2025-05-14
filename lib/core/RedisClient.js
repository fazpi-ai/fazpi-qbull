import Redis from 'ioredis';
import Logger from './Logger.js';

const logger = new Logger('RedisClient');

class RedisClient {
  constructor(host, port, user, password, db) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.db = db;
    this.client = null;
    this.isConnected = false;
    this.isConnecting = false;
  }

  connect() {
    if (this.isConnected) {
      logger.debug('Already connected and ready for Redis.');
      return Promise.resolve();
    }

    if (this.isConnecting) {
      logger.debug('Connection attempt already in progress. Waiting for it to complete.');
      return new Promise((resolve, reject) => {
        const intervalId = setInterval(() => {
          if (this.isConnected) {
            clearInterval(intervalId);
            resolve();
          } else if (!this.isConnecting) {
            clearInterval(intervalId);
            reject(new Error('Previous connection attempt failed.'));
          }
        }, 100);
      });
    }

    this.isConnecting = true;
    logger.info(`Attempting to connect to Redis at ${this.host}:${this.port}, DB: ${this.db}`);

    if (this.client) {
      logger.debug('Cleaning up old Redis client instance before new connection attempt.');
      try {
        this.client.removeAllListeners();
        this.client.disconnect();
      } catch (e) {
        logger.warn(`Error during cleanup of old client instance: ${e.message}`);
      }
      this.client = null;
    }

    const options = {
      host: this.host,
      port: this.port,
      username: this.user, // username for Redis 6+ ACL
      password: this.password,
      db: this.db, // db passed to ioredis
      enableOfflineQueue: false, // Recommended to handle connection errors explicitly
      connectTimeout: 10000,    // Initial connection timeout
      maxRetriesPerRequest: 0,  // No retry individual commands if they fail (reconnection is separate)
      // retryStrategy(times) { // Custom retry strategy example if needed
      //   const delay = Math.min(times * 50, 2000);
      //   logger.info(`Redis: Retrying connection (attempt ${times}) in ${delay}ms`);
      //   return delay;
      // }
    };

    this.client = new Redis(options);

    return new Promise((resolve, reject) => {
      const onClientReady = () => {
        logger.info(`Redis client is ready (host: ${this.host}:${this.port}, DB: ${this.db}).`);
        this.isConnected = true;
        this.isConnecting = false;
        this.client.removeListener('error', onErrorDuringConnect);
        this.client.removeListener('connect', onTcpConnect);
        resolve();
      };

      const onErrorDuringConnect = (err) => {
        // Only handle the error if this connection promise is the active one
        if (this.isConnecting && this.client && this.client.listeners('ready').includes(onClientReady)) {
          logger.error(`Redis connection/ready attempt failed for ${this.host}:${this.port}, DB: ${this.db}: ${err.message}`);
          this.isConnecting = false;
          this.isConnected = false;
          this.client.removeListener('ready', onClientReady);
          this.client.removeListener('connect', onTcpConnect);
          if (this.client) {
            try { this.client.disconnect(); } catch (disconnectErr) { logger.warn(`Error disconnecting failed client: ${disconnectErr.message}`); }
            // Do not put this.client = null here, it's done if you retry connect()
          }
          reject(new Error(`Failed to connect or become ready for Redis: ${err.message}`));
        }
      };

      const onTcpConnect = () => {
        logger.info(`TCP connection established to Redis at ${this.host}:${this.port}, DB: ${this.db}. Waiting for client to be 'ready'...`);
      };

      this.client.once('connect', onTcpConnect);
      this.client.once('ready', onClientReady);
      this.client.once('error', onErrorDuringConnect); // This 'once' is for the initial connection error

      // General listeners that persist while the client exists
      this.client.on('close', () => {
        logger.info(`Redis connection closed for ${this.host}:${this.port}, DB: ${this.db}.`);
        this.isConnected = false;
        // this.isConnecting could become true if ioredis tries to reconnect automatically
      });

      this.client.on('error', (err) => { // General listener for runtime errors
        if (!onErrorDuringConnect || !this.client.listeners('error').includes(onErrorDuringConnect)) {
          // If the initial connection error listener is no longer present, it's a runtime error
          logger.error(`General Redis client runtime error for ${this.host}:${this.port}, DB: ${this.db}: ${err.message}`);
          // this.isConnected = false; // Could be too aggressive, depends on the nature of the error
        }
      });

    }).catch(err => { // Capture synchronous errors from `new Redis()` or promise configuration
      logger.error(`Synchronous error during Redis client setup for ${this.host}:${this.port}, DB: ${this.db}: ${err.message}`, err);
      this.isConnecting = false;
      this.isConnected = false;
      if (this.client) {
        try { this.client.disconnect(); } catch (e) { /* ignore */ }
        // this.client = null; // Do not null here, it's handled in the next connect() attempt
      }
      throw err;
    });
  }

  async publishToStream(streamName, jobData) {
    if (!this.client || !this.isConnected) {
      logger.error('Cannot publish to stream: No active and ready Redis connection.');
      throw new Error('No active and ready Redis connection.');
    }

    try {
      const messageId = '*'; // Redis will generate the ID
      const args = Object.entries(jobData).flat(); // Convert {k1:v1, k2:v2} to [k1, v1, k2, v2]
      logger.debug(`Publishing to stream '${streamName}' with data: ${JSON.stringify(jobData)}`);
      const result = await this.client.xadd(streamName, messageId, ...args);
      logger.info(`Job published to stream '${streamName}'. Message ID: ${result}`);
      return result; // Message ID
    } catch (err) {
      logger.error(`Error publishing to Redis stream '${streamName}': ${err.message}`, err);
      throw err;
    }
  }

  async disconnect() {
    if (this.client) {
      logger.info(`Attempting to disconnect from Redis (host: ${this.host}:${this.port}, DB: ${this.db})...`);
      // Remove all listeners to avoid problems in reconnections or multiple closures
      this.client.removeAllListeners();
      if (this.isConnected) { // Only attempt 'quit' if formally connected and ready
        try {
          await this.client.quit();
          logger.info(`Successfully disconnected (quit) from Redis.`);
        } catch (err) {
          logger.error(`Error during Redis graceful disconnect (quit): ${err.message}. Forcing disconnect.`, err);
          this.client.disconnect(); // Force disconnect
        }
      } else {
        // If not 'isConnected' (ready), simply disconnect the TCP connection if it exists.
        this.client.disconnect();
        logger.info(`Forced disconnect for non-ready client from Redis.`);
      }
      this.client = null; // Important to allow a new instance in connect()
    } else {
      logger.debug('No Redis client instance to disconnect.');
    }
    this.isConnected = false;
    this.isConnecting = false;
  }

  async set(key, value) {
    if (!this.client || !this.isConnected) {
      logger.error('Cannot SET: No active and ready Redis connection.');
      throw new Error('No active and ready Redis connection.');
    }
    try {
      logger.debug(`Setting key '${key}'`);
      return await this.client.set(key, value);
    } catch (err) {
      logger.error(`Error setting key '${key}': ${err.message}`, err);
      throw err;
    }
  }

  async get(key) {
    if (!this.client || !this.isConnected) {
      logger.error('Cannot GET: No active and ready Redis connection.');
      throw new Error('No active and ready Redis connection.');
    }
    try {
      logger.debug(`Getting key '${key}'`);
      const value = await this.client.get(key);
      logger.debug(`Value for key '${key}': ${value === null ? 'null' : 'retrieved'}`);
      return value;
    } catch (err) {
      logger.error(`Error getting key '${key}': ${err.message}`, err);
      throw err;
    }
  }

  getClientInstance() {
    return this.client;
  }
}

export default RedisClient;