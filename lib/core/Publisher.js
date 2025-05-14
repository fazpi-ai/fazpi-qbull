import Logger from './Logger.js';

class Publisher {
  constructor(redisClient) {
    this.logger = new Logger('Publisher');

    if (!redisClient || typeof redisClient.publishToStream !== 'function') {
      this.logger.error('Initialization Error: Publisher requires a Redis client with a publishToStream method.');
      throw new Error('Publisher requires a Redis client with a publishToStream method.');
    }
    this.redisClient = redisClient;
    this.logger.info('Publisher initialized successfully.');
  }

  /**
   * Publishes a message to a Redis stream.
   * @param {string} streamName - The name of the stream (topic/queue).
   * @param {object} messageData - The data of the message to be published.
   * @param {object} [options={}] - Additional publishing options.
   * @param {string} [options.orderingKey] - Optional key to ensure processing order.
   * @returns {Promise<string|null>} - The ID of the published message, or null if it fails (depends on redisClient.publishToStream).
   */
  async publish(streamName, messageData, options = {}) {
    if (!streamName || typeof streamName !== 'string' || streamName.trim() === '') {
      this.logger.error('Post Error: Stream name must be a non-empty string.', { streamName });
      throw new Error('The stream name must be a non-empty string.');
    }
    if (typeof messageData !== 'object' || messageData === null) {
      this.logger.error('Post Error: The message data must be an object.', { messageData });
      throw new Error('The message data must be an object.');
    }

    const jobPayload = { ...messageData };
    let orderingKeyInfo = 'without orderingKey';

    if (options.orderingKey && typeof options.orderingKey === 'string' && options.orderingKey.trim() !== '') {
      jobPayload._orderingKey = options.orderingKey.trim();
      orderingKeyInfo = `with orderingKey '${jobPayload._orderingKey}'`;
    }

    this.logger.debug(`Trying to publish to stream '${streamName}' ${orderingKeyInfo}. Payload:`, jobPayload);

    try {
      // Delegate to the stream publishing method of the Redis client (RedisSingleton).
      // RedisSingleton will ensure the connection and might have its own logging.
      const messageId = await this.redisClient.publishToStream(streamName, jobPayload);
      this.logger.info(`Message published successfully to stream '${streamName}' ${orderingKeyInfo}. Message ID: ${messageId}`);
      return messageId;
    } catch (error) {
      this.logger.error(`Error publishing to stream '${streamName}': ${error.message}`, { streamName, payload: jobPayload, error });
      throw error; // Re-throw the error to be handled by the caller
    }
  }
}

export default Publisher;