class Publisher {
  constructor(redisClient) {
    if (!redisClient || typeof redisClient.publishToStream !== 'function') {
      throw new Error('Publisher requiere un cliente de Redis con un método publishToStream.');
    }
    this.redisClient = redisClient;
  }

  /**
   * Publica un mensaje en un stream de Redis.
   * @param {string} streamName - El nombre del stream (tópico/cola).
   * @param {object} messageData - Los datos del mensaje a publicar.
   * @param {object} [options={}] - Opciones adicionales para la publicación.
   * @param {string} [options.orderingKey] - Clave opcional para garantizar el orden de procesamiento.
   * @returns {Promise<string|null>} - El ID del mensaje publicado, o null si falla la publicación (depende de redisClient.publishToStream).
   */
  async publish(streamName, messageData, options = {}) {
    if (!streamName || typeof streamName !== 'string' || streamName.trim() === '') {
      throw new Error('El nombre del stream debe ser una cadena de texto no vacía.');
    }
    if (typeof messageData !== 'object' || messageData === null) {
      throw new Error('Los datos del mensaje deben ser un objeto.');
    }

    const jobPayload = { ...messageData };

    if (options.orderingKey && typeof options.orderingKey === 'string' && options.orderingKey.trim() !== '') {
      // Añadir la orderingKey como un campo especial al payload del trabajo.
      // El prefijo _ es una convención para campos "internos" o de metadatos.
      jobPayload._orderingKey = options.orderingKey.trim();
      // this.logger.debug(`Publicando en stream '${streamName}' con orderingKey '${jobPayload._orderingKey}'. Datos:`, messageData);
    } else {
      // this.logger.debug(`Publicando en stream '${streamName}' (sin orderingKey). Datos:`, messageData);
    }

    // Delegar al método de publicación en stream del cliente Redis.
    // Asumimos que publishToStream ahora puede devolver el ID del mensaje.
    const messageId = await this.redisClient.publishToStream(streamName, jobPayload);
    return messageId;
  }
}

export default Publisher;
