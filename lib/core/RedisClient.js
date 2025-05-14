import Redis from 'ioredis';
import Logger from './Logger.js';

const logger = new Logger('RedisClient');

class RedisClient {
  constructor(host, port, user, password) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
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
    logger.info(`Attempting to connect to Redis at ${this.host}:${this.port}`);

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
      password: this.password,
      username: this.user,
      enableOfflineQueue: false,
      connectTimeout: 10000,
      maxRetriesPerRequest: 0,
    };

    this.client = new Redis(options);

    return new Promise((resolve, reject) => {
      const onClientReady = () => {
        logger.info(`Redis client is ready (host: ${this.host}:${this.port}).`);
        this.isConnected = true;
        this.isConnecting = false;
        // Clean up listeners specific to this connection attempt's promise
        this.client.removeListener('error', onErrorDuringConnect);
        this.client.removeListener('connect', onTcpConnect); // Also remove this one
        resolve();
      };

      const onErrorDuringConnect = (err) => {
        if (this.isConnecting) { 
          logger.error(`Redis connection/ready attempt failed for ${this.host}:${this.port}: ${err.message}`);
          this.isConnecting = false;
          this.isConnected = false;
          // Clean up listeners
          this.client.removeListener('ready', onClientReady);
          this.client.removeListener('connect', onTcpConnect);
          if (this.client) {
            try { this.client.disconnect(); } catch (disconnectErr) { logger.warn(`Error disconnecting failed client: ${disconnectErr.message}`); }
            this.client = null; 
          }
          reject(new Error(`Failed to connect or become ready for Redis: ${err.message}`));
        }
        // If !this.isConnecting, the error is not for the current promise, handled by general error listener
      };
      
      const onTcpConnect = () => {
        // This event indicates TCP connection is made, but client might not be fully ready.
        logger.info(`TCP connection established to Redis at ${this.host}:${this.port}. Waiting for client to be 'ready'...`);
        // isConnected and promise resolution will happen on 'ready'.
      };

      // Listeners for this specific connection attempt's promise
      this.client.once('connect', onTcpConnect);         // Informational logging for TCP connect
      this.client.once('ready', onClientReady);           // Resolve promise when client is 'ready'
      this.client.once('error', onErrorDuringConnect);    // Handle errors during connect/ready sequence for this promise

      // General listeners for client state changes (active as long as client instance exists)
      // These are not tied to resolving/rejecting the connect() promise directly
      this.client.on('close', () => {
        logger.info(`Redis connection closed for ${this.host}:${this.port}.`);
        this.isConnected = false;
        // If you have auto-reconnect enabled (default in ioredis unless maxRetriesPerRequest is 0 for commands),
        // you might see 'connecting' and 'reconnecting' events too.
      });

      this.client.on('error', (err) => { // General error listener for errors not caught by onErrorDuringConnect
        // This handles runtime errors after connection or errors if not in `isConnecting` state.
        if (!this.isConnecting) { 
            logger.error(`General Redis client runtime error for ${this.host}:${this.port}: ${err.message}`);
        }
        // isConnected should be false if a significant error occurs
        this.isConnected = false; 
      });

    }).catch(err => { // Catches synchronous errors from `new Redis()` or promise setup itself
        logger.error(`Synchronous error during Redis client setup for ${this.host}:${this.port}: ${err.message}`, err);
        this.isConnecting = false;
        this.isConnected = false;
        if (this.client) {
            try { this.client.disconnect(); } catch (e) { /* ignore */ }
            this.client = null;
        }
        throw err; // Re-throw for the caller of connect()
    });
  }

  // ... other methods (publishToStream, disconnect, set, get, getClientInstance) remain the same
  async publishToStream(streamName, jobData) {
    if (!this.client || !this.isConnected) {
      logger.error('Cannot publish to stream: No active and ready Redis connection.');
      throw new Error('No se ha establecido una conexión activa y lista con Redis');
    }

    try {
      const messageId = '*';
      const args = Object.entries(jobData).flat();
      logger.debug(`Publishing to stream '${streamName}' with data: ${JSON.stringify(jobData)}`);
      const result = await this.client.xadd(streamName, messageId, ...args);
      logger.info(`Job published to stream '${streamName}'. Message ID: ${result}`);
      return result;
    } catch (err) {
      logger.error(`Error publishing to Redis stream '${streamName}': ${err.message}`, err);
      throw err;
    }
  }

  async disconnect() {
    if (this.client) {
      if (this.isConnected) { // Check if it was connected and ready
        logger.info(`Attempting to gracefully disconnect from Redis (quit) for ${this.host}:${this.port}...`);
        try {
          await this.client.quit();
          logger.info(`Successfully disconnected (quit) from Redis for ${this.host}:${this.port}.`);
        } catch (err) {
          logger.error(`Error during Redis graceful disconnect (quit) for ${this.host}:${this.port}: ${err.message}. Forcing disconnect.`, err);
          try {
            this.client.disconnect(); // Force disconnect
            logger.info(`Forcefully disconnected from Redis for ${this.host}:${this.port}.`);
          } catch (forceErr) {
            logger.error(`Error during forceful Redis disconnection for ${this.host}:${this.port}: ${forceErr.message}`, forceErr);
          }
        }
      } else {
        logger.debug(`Client for ${this.host}:${this.port} exists but was not connected/ready. Issuing disconnect command for cleanup.`);
        try {
          this.client.disconnect(); 
        } catch (e) {
            logger.warn(`Error cleaning up non-connected client for ${this.host}:${this.port}: ${e.message}`);
        }
      }
      this.client.removeAllListeners(); 
      this.client = null; 
      this.isConnected = false;
      this.isConnecting = false; 
    } else {
      logger.debug('No Redis client instance to disconnect.');
    }
  }

  async set(key, value) {
    if (!this.client || !this.isConnected) {
      logger.error('Cannot SET: No active and ready Redis connection.');
      throw new Error('No se ha establecido una conexión activa y lista con Redis');
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
      throw new Error('No se ha establecido una conexión activa y lista con Redis');
    }
    try {
      logger.debug(`Getting key '${key}'`);
      const value = await this.client.get(key);
      logger.debug(`Value for key '${key}': ${value === null ? 'null' : value}`);
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