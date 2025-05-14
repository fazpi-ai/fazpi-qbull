// lib/core/QueueWorker.js
import Logger from './Logger.js';

/**
 * Convierte un array plano de Redis (ej: ['key1', 'value1', 'key2', 'value2']) en un objeto.
 * @param {string[]} arr - El array de Redis.
 * @returns {object} - El objeto resultante.
 */
function arrayToObject(arr) {
  const obj = {};
  for (let i = 0; i < arr.length; i += 2) {
    obj[arr[i]] = arr[i + 1];
  }
  return obj;
}

class QueueWorker {
  /**
   * Crea una instancia de QueueWorker.
   * @param {object} redisClient - Cliente de Redis (compatible con ioredis) con métodos de streams.
   * @param {string} queueName - Nombre del stream de Redis (tópico/cola) a escuchar.
   * @param {function} jobHandler - Función asíncrona para procesar cada trabajo. Debe aceptar (jobData, jobId).
   * @param {object} [options={}] - Opciones de configuración.
   * @param {number} [options.concurrency=1] - Número de trabajos a procesar en paralelo.
   * @param {boolean} [options.processOrderedByKey=false] - Habilitar procesamiento ordenado por _orderingKey.
   * @param {string} [options.groupName] - Nombre del grupo de consumidores. Por defecto `group:${queueName}`.
   * @param {string} [options.consumerName] - Nombre único para este consumidor. Por defecto se autogenera.
   * @param {number} [options.blockTimeMs=5000] - Tiempo en ms para bloquear la lectura del stream.
   */
  constructor(redisClient, queueName, jobHandler, options = {}) {
    // Validaciones completas
    if (!redisClient ||
        typeof redisClient.xgroup !== 'function' ||
        typeof redisClient.xreadgroup !== 'function' ||
        typeof redisClient.xack !== 'function') {
      throw new Error('QueueWorker requiere un cliente de Redis compatible con comandos de streams (xgroup, xreadgroup, xack).');
    }
    if (!queueName || typeof queueName !== 'string' || queueName.trim() === '') {
      throw new Error('queueName (string no vacío) es requerido.');
    }
    if (typeof jobHandler !== 'function') {
      throw new Error('jobHandler (function) es requerido.');
    }

    this.redisClient = redisClient;
    this.queueName = queueName.trim();
    this.jobHandler = jobHandler;

    this.options = {
      concurrency: 1,
      processOrderedByKey: false,
      blockTimeMs: 5000,
      groupName: `group:${this.queueName}`,
      consumerName: `consumer:${this.queueName}-${process.pid}-${Date.now()}`,
      ...options,
    };

    // Validar concurrencia
    if (typeof this.options.concurrency !== 'number' || this.options.concurrency < 1) {
        this.logger.warn(`Valor de concurrencia inválido (${this.options.concurrency}). Usando 1 por defecto.`);
        this.options.concurrency = 1;
    }


    this.logger = new Logger(`QueueWorker:${this.options.consumerName}`);
    this.isStopping = false;
    this.activeJobs = 0;
    this._pollTimeoutId = null;

    if (this.options.processOrderedByKey) {
      this.orderingKeyQueues = new Map();
      this.processingKeys = new Set();
      this.logger.info(`Procesamiento ordenado por clave habilitado para '${this.queueName}'.`);
    }

    this.logger.info(`QueueWorker inicializado para cola '${this.queueName}'. Concurrencia: ${this.options.concurrency}.`);
  }

  /**
   * Inicia el worker: crea el grupo de consumidores y comienza a sondear trabajos.
   */
  async start() {
    if (this.isStopping) {
        this.logger.warn('Intento de iniciar un QueueWorker que se está deteniendo o ya está detenido.');
        return;
    }
    this.logger.info(`Iniciando QueueWorker para cola '${this.queueName}', grupo '${this.options.groupName}'.`);
    this.isStopping = false;

    try {
      // Intenta crear el grupo de consumidores.
      // MKSTREAM crea el stream si no existe.
      // '$' significa que el grupo comenzará a leer mensajes nuevos desde este punto.
      await this.redisClient.xgroup('CREATE', this.queueName, this.options.groupName, '$', 'MKSTREAM');
      this.logger.info(`Grupo de consumidores '${this.options.groupName}' asegurado/creado en stream '${this.queueName}'.`);
    } catch (err) {
      // Si el grupo ya existe, Redis devuelve un error BUSYGROUP. Esto es normal.
      if (err.message && err.message.includes('BUSYGROUP')) {
        this.logger.info(`El grupo de consumidores '${this.options.groupName}' ya existe en stream '${this.queueName}'.`);
      } else {
        // Otro error al crear el grupo es problemático.
        this.logger.error(`Error al crear/asegurar el grupo de consumidores: ${err.message}`, err);
        throw err; // Propagar error crítico de inicio
      }
    }
    this._doPoll(); // Iniciar el bucle de sondeo
  }
  
  /**
   * Programa el siguiente ciclo de sondeo.
   * @param {number} delayMs - Tiempo de espera en milisegundos antes del próximo sondeo.
   */
  _scheduleNextPoll(delayMs) {
    if (this.isStopping) return;
    // Limpiar cualquier timeout anterior para evitar múltiples bucles
    if (this._pollTimeoutId) clearTimeout(this._pollTimeoutId);
    this._pollTimeoutId = setTimeout(() => this._doPoll(), delayMs);
  }

  /**
   * Bucle principal de sondeo para nuevos trabajos.
   */
  async _doPoll() {
    if (this.isStopping) {
      this.logger.info('Sondeo detenido (isStopping).');
      return;
    }

    // Calcular cuántos trabajos se pueden buscar, basado en la concurrencia disponible.
    // Si processOrderedByKey está activo, podríamos querer leer más para llenar las colas internas,
    // pero la lógica de despacho (_dispatchOrderedJobs) se encargará de no exceder la concurrencia.
    // Por ahora, limitamos la búsqueda si todos los slots están ocupados y no es procesamiento ordenado.
    const slotsAvailableForFetch = this.options.concurrency - this.activeJobs;
    if (slotsAvailableForFetch <= 0 && !this.options.processOrderedByKey) {
      this.logger.debug(`Todos los slots de concurrencia (${this.options.concurrency}) están ocupados. Reintentando sondeo en 1s.`);
      this._scheduleNextPoll(1000); // Reintentar más tarde
      return;
    }
    
    // Determinar cuántos mensajes pedir a Redis.
    // Si processOrderedByKey está activo, podríamos querer buscar más para llenar las colas internas.
    // Si no, solo buscamos hasta llenar los slots de concurrencia.
    // Por simplicidad, leeremos hasta `concurrency` mensajes si processOrderedByKey está activo,
    // o `slotsAvailableForFetch` si no. Siempre al menos 1 si hay algún slot.
    const fetchCount = this.options.processOrderedByKey ? this.options.concurrency : Math.max(1, slotsAvailableForFetch);


    try {
      this.logger.debug(`Sondeando stream '${this.queueName}' por hasta ${fetchCount} trabajos. Activos: ${this.activeJobs}.`);
      const results = await this.redisClient.xreadgroup(
        'GROUP', this.options.groupName, this.options.consumerName,
        'COUNT', fetchCount,
        'BLOCK', this.options.blockTimeMs,
        'STREAMS', this.queueName, '>' // Leer solo mensajes nuevos no entregados a otros consumidores del grupo
      );

      if (this.isStopping) { 
        this.logger.info('Sondeo detenido (isStopping post-block).'); 
        return; 
      }

      if (results && results.length > 0) {
        const messages = results[0][1]; // messages = [ [messageId, [field, value, ...]], ... ]
        this.logger.debug(`Recibidos ${messages.length} mensajes de '${this.queueName}'.`);

        for (const message of messages) {
          const jobId = message[0];
          const jobData = arrayToObject(message[1]);
          const orderingKey = jobData._orderingKey; // Asume que Publisher añade este campo

          if (this.options.processOrderedByKey && orderingKey) {
            // Encolar para procesamiento ordenado
            if (!this.orderingKeyQueues.has(orderingKey)) {
              this.orderingKeyQueues.set(orderingKey, []);
            }
            this.orderingKeyQueues.get(orderingKey).push({ jobId, jobData });
            this.logger.debug(`Trabajo ${jobId} (key: ${orderingKey}) encolado internamente.`);
          } else {
            // Procesar inmediatamente si hay concurrencia (trabajo no ordenado o modo no ordenado)
            if (this.activeJobs < this.options.concurrency) {
              this.activeJobs++;
              this._executeJob(jobId, jobData)
                .finally(() => {
                  this.activeJobs--;
                  // Si el procesamiento ordenado está activo, intentar despachar de esas colas
                  if (this.options.processOrderedByKey) {
                    this._dispatchOrderedJobs(); 
                  }
                });
            } else {
              this.logger.warn(`Trabajo ${jobId} (no ordenado) no se puede procesar inmediatamente, concurrencia llena. Será reintentado por otro consumidor o en el próximo ciclo.`);
              // El mensaje no se procesa ni se hace ACK, quedará pendiente.
              // Esto es una limitación: si la concurrencia está llena de trabajos ordenados, los no ordenados podrían esperar.
              break; // Salir del bucle for, ya que no podemos tomar más trabajos no ordenados.
            }
          }
        }
        // Después de encolar todos los mensajes recibidos (si es modo ordenado), intentar despacharlos.
        if (this.options.processOrderedByKey) {
          this._dispatchOrderedJobs();
        }
      } else {
        this.logger.debug(`No se recibieron mensajes de '${this.queueName}' en este sondeo.`);
      }
    } catch (err) {
      this.logger.error(`Error durante xreadgroup para stream '${this.queueName}': ${err.message}`, err);
      this._scheduleNextPoll(5000); // Esperar antes de reintentar en caso de error de red/Redis
      return; // Salir de esta ejecución de _doPoll
    }

    // Programar el siguiente sondeo
    this._scheduleNextPoll(0); // Inmediatamente para el siguiente ciclo del event loop si no hubo error
  }

  /**
   * Despacha trabajos de las colas internas ordenadas si la concurrencia lo permite.
   */
  _dispatchOrderedJobs() {
    if (!this.options.processOrderedByKey || this.isStopping) {
      return;
    }

    for (const [key, queue] of this.orderingKeyQueues) {
      // Si hay trabajos en la cola para esta clave, la clave no está siendo procesada, y hay slots de concurrencia
      if (queue.length > 0 && !this.processingKeys.has(key) && this.activeJobs < this.options.concurrency) {
        this.processingKeys.add(key); // Marcar esta clave como "en procesamiento"
        this.activeJobs++;

        const { jobId, jobData } = queue.shift(); // Tomar el primer trabajo de la cola para esta clave
        if (queue.length === 0) {
          this.orderingKeyQueues.delete(key); // Limpiar el mapa si la cola para esta clave está vacía
        }
        
        this.logger.debug(`Despachando trabajo ordenado ${jobId} para clave '${key}'. Trabajos restantes para esta clave: ${queue.length}. Activos totales: ${this.activeJobs}.`);

        this._executeJob(jobId, jobData, key) // Pasar la clave para el finally y logging
          .finally(() => {
            this.activeJobs--;
            this.processingKeys.delete(key); // Liberar la clave
            this._dispatchOrderedJobs(); // Intentar despachar más (para esta u otras claves)
          });
      }
      // Si ya estamos en la concurrencia máxima, no tomar más trabajos aunque haya en las colas.
      if (this.activeJobs >= this.options.concurrency) break;
    }
  }

  /**
   * Ejecuta el jobHandler para un trabajo específico y maneja el ACK.
   * @param {string} jobId - El ID del trabajo.
   * @param {object} jobData - Los datos del trabajo.
   * @param {string|null} [orderingKey=null] - La clave de ordenamiento, si aplica (para logging).
   */
  async _executeJob(jobId, jobData, orderingKey = null) {
    const logPrefix = orderingKey ? `(Key: ${orderingKey}) ` : '';
    this.logger.info(`${logPrefix}Procesando trabajo ${jobId} de la cola '${this.queueName}'.`);
    // this.logger.debug(`${logPrefix}Datos para ${jobId}:`, jobData); // Puede ser muy verboso

    try {
      const result = await this.jobHandler(jobData, jobId);
      this.logger.info(`${logPrefix}Trabajo ${jobId} completado. Resultado: ${result !== undefined ? JSON.stringify(result) : '[sin resultado]'}`);
      
      try {
        await this.redisClient.xack(this.queueName, this.options.groupName, jobId);
        this.logger.info(`${logPrefix}Trabajo ${jobId} confirmado (ACK).`);
      } catch (ackError) {
        this.logger.error(`${logPrefix}Error al confirmar (ACK) trabajo ${jobId} después de procesamiento exitoso: ${ackError.message}`, ackError);
        // El trabajo se procesó, pero el ACK falló. Podría ser reprocesado por otro consumidor.
        // Esto es un estado problemático que requiere monitoreo.
      }
    } catch (error) {
      this.logger.error(`${logPrefix}Error en jobHandler para trabajo ${jobId}: ${error.message}`, {stack: error.stack, name: error.name});
      // No se hace XACK si el handler falla. El trabajo quedará pendiente en el stream
      // y podría ser recogido de nuevo por este u otro consumidor después del min-idle-time,
      // o requerir manejo de mensajes pendientes (XPENDING, XCLAIM, dead-letter queue).
    }
  }

  /**
   * Indica al worker que debe detenerse. Intentará completar los trabajos activos.
   */
  async stop() {
    this.logger.info(`Intentando detener QueueWorker para cola '${this.queueName}'. ${this.activeJobs} trabajos activos.`);
    this.isStopping = true;
    if (this._pollTimeoutId) {
        clearTimeout(this._pollTimeoutId); // Cancelar el próximo sondeo programado
    }

    // Esperar a que los trabajos activos terminen (con un timeout)
    const stopTime = Date.now();
    const maxWaitMs = this.options.gracefulShutdownTimeoutMs || 30000; // Esperar máximo 30 segundos por defecto
    
    while (this.activeJobs > 0 && (Date.now() - stopTime) < maxWaitMs) {
      this.logger.debug(`Esperando ${this.activeJobs} trabajos activos para que terminen... (Colas ordenadas internas: ${this.options.processOrderedByKey ? this.orderingKeyQueues.size : 0})`);
      await new Promise(resolve => setTimeout(resolve, 250)); // Pequeña pausa
    }

    if (this.activeJobs > 0) {
      this.logger.warn(`QueueWorker detenido, pero ${this.activeJobs} trabajos aún estaban activos después del período de espera de ${maxWaitMs}ms.`);
    }
    
    // También verificar si quedaron trabajos en las colas internas ordenadas que no llegaron a procesarse
    if (this.options.processOrderedByKey) {
        let pendingInOrderedQueues = 0;
        if (this.orderingKeyQueues) { // Asegurarse que existe
            this.orderingKeyQueues.forEach(q => pendingInOrderedQueues += q.length);
        }
        if (pendingInOrderedQueues > 0) {
            this.logger.warn(`${pendingInOrderedQueues} trabajos quedaron pendientes en colas internas ordenadas al detener el QueueWorker.`);
        }
    }
    this.logger.info('QueueWorker detenido completamente.');
  }
}

export default QueueWorker;
