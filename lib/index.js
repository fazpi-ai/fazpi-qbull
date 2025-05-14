import redisSingleton from './core/RedisSingleton.js';
import Logger from './core/Logger.js';
import Publisher from './core/Publisher.js'; // Asegúrate que es la versión con options.orderingKey
import QueueWorker from './core/QueueWorker.js'; // Asegúrate que es la versión con processOrderedByKey

const appLogger = new Logger('Application');

// --- Definición de los Manejadores de Trabajos (Job Handlers) ---

async function handleEmailJob(jobData, jobId) {
  const emailJobLogger = new Logger(`EmailJob:${jobId}`);
  const orderingKeyInfo = jobData._orderingKey ? `(Ordenado por: ${jobData._orderingKey})` : '';
  emailJobLogger.info(`Procesando trabajo de email ${orderingKeyInfo}. Para: ${jobData.email}, Asunto: "${jobData.subject}"`);

  if (!jobData.email || !jobData.subject || !jobData.body) {
    const errorMessage = `Datos inválidos para el trabajo de email ${jobId}. Faltan email, subject, o body.`;
    emailJobLogger.error(errorMessage, jobData);
    throw new Error(errorMessage);
  }

  const delay = jobData.delay || (1500 + Math.random() * 1000);
  emailJobLogger.info(`Simulando envío de email (duración: ${Math.round(delay)}ms)...`);
  await new Promise(resolve => setTimeout(resolve, delay));

  emailJobLogger.info(`Email "${jobData.subject}" "enviado" a ${jobData.email} para el trabajo ${jobId}.`);
  return { status: 'sent', deliveryTime: new Date().toISOString(), jobId };
}

async function handleWhatsAppJob(jobData, jobId) {
  const whatsappJobLogger = new Logger(`WhatsAppJob:${jobId}`);
  whatsappJobLogger.info(`Procesando trabajo de WhatsApp. Para: ${jobData.phoneNumber}, Mensaje: "${jobData.message}"`);

  if (!jobData.phoneNumber || !jobData.message) {
    const errorMessage = `Datos inválidos para el trabajo de WhatsApp ${jobId}. Faltan phoneNumber o message.`;
    whatsappJobLogger.error(errorMessage, jobData);
    throw new Error(errorMessage);
  }

  const delay = jobData.delay || (800 + Math.random() * 500);
  whatsappJobLogger.info(`Simulando envío de WhatsApp (duración: ${Math.round(delay)}ms)...`);
  await new Promise(resolve => setTimeout(resolve, delay));

  whatsappJobLogger.info(`Mensaje de WhatsApp "enviado" a ${jobData.phoneNumber} para el trabajo ${jobId}.`);
  return { status: 'delivered', messageId: `whatsapp-${jobId}` };
}

async function handleUserActivityJob(jobData, jobId) {
  const activityLogger = new Logger(`UserActivityJob:${jobId}`);
  const orderingKey = jobData._orderingKey; // La orderingKey será el userId
  activityLogger.info(`Procesando actividad para usuario ${orderingKey}. Actividad: ${jobData.activityType}, Detalles: ${jobData.details}`);

  // Simular procesamiento de actividad
  const delay = jobData.delay || (500 + Math.random() * 500);
  activityLogger.info(`Simulando procesamiento de actividad (duración: ${Math.round(delay)}ms)...`);
  await new Promise(resolve => setTimeout(resolve, delay));

  activityLogger.info(`Actividad '${jobData.activityType}' para usuario ${orderingKey} procesada (trabajo ${jobId}).`);
  return { status: 'processed', activity: jobData.activityType, userId: orderingKey };
}

// --- Nombres de las Colas/Tópicos ---
const EMAIL_QUEUE = 'EMAIL_JOBS_Q'; // Renombrado para evitar colisiones con ejecuciones anteriores
const WHATSAPP_QUEUE = 'WHATSAPP_JOBS_Q';
const ORDERED_USER_ACTIVITY_QUEUE = 'USER_ACTIVITY_Q';

// --- Instancias de los Workers de Cola ---
let emailQueueWorker;
let whatsappQueueWorker;
let userActivityWorker;

async function main() {
  try {
    appLogger.info('Application starting...');
    await redisSingleton.connect();
    appLogger.info('Successfully connected to Redis via Singleton.');

    const rawRedisClient = redisSingleton.getRawClient();
    if (!rawRedisClient) {
      throw new Error("No se pudo obtener el cliente crudo de Redis desde RedisSingleton. ¿Está conectado?");
    }

    // --- Configurar y arrancar los QueueWorkers ---

    // Worker para Emails (procesamiento paralelo simple)
    emailQueueWorker = new QueueWorker(
      rawRedisClient,
      EMAIL_QUEUE,
      handleEmailJob,
      {
        concurrency: 2,
        groupName: 'email-service-group-main',
      }
    );
    appLogger.info(`Iniciando Email QueueWorker (concurrencia: 2) para la cola '${EMAIL_QUEUE}'...`);
    await emailQueueWorker.start();

    // Worker para WhatsApp (procesamiento paralelo simple)
    whatsappQueueWorker = new QueueWorker(
      rawRedisClient,
      WHATSAPP_QUEUE,
      handleWhatsAppJob,
      {
        concurrency: 3,
        groupName: 'whatsapp-service-group-main',
      }
    );
    appLogger.info(`Iniciando WhatsApp QueueWorker (concurrencia: 3) para la cola '${WHATSAPP_QUEUE}'...`);
    await whatsappQueueWorker.start();

    // Worker para Actividad de Usuario (procesamiento ORDENADO por userId)
    userActivityWorker = new QueueWorker(
      rawRedisClient,
      ORDERED_USER_ACTIVITY_QUEUE,
      handleUserActivityJob,
      {
        concurrency: 4, // Puede procesar hasta 4 usuarios DIFERENTES en paralelo
        processOrderedByKey: true, // Habilitar la lógica de ordenamiento
        groupName: 'user-activity-ordered-group',
      }
    );
    appLogger.info(`Iniciando UserActivity QueueWorker (concurrencia: 4, ordenado por clave) para la cola '${ORDERED_USER_ACTIVITY_QUEUE}'...`);
    await userActivityWorker.start();


    // --- Publicar algunos trabajos de ejemplo ---
    const jobPublisher = new Publisher(redisSingleton);
    appLogger.info('Publisher instance created.');
    appLogger.info(`Publicando trabajos de ejemplo...`);

    // Publicar trabajos de email y WhatsApp (no ordenados)
    for (let i = 1; i <= 3; i++) {
      await jobPublisher.publish(EMAIL_QUEUE, {
        email: `user${i}@example.com`, subject: `Email de prueba #${i}`, body: `Cuerpo email ${i}`,
      });
      await jobPublisher.publish(WHATSAPP_QUEUE, {
        phoneNumber: `+100000000${i}`, message: `Mensaje WhatsApp #${i}`,
      });
    }

    // Publicar trabajos de actividad de usuario (ordenados por userId)
    const userId1 = 'user_alpha';
    const userId2 = 'user_beta';

    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'login', details: 'Login desde web' }, { orderingKey: userId1 });
    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'view_profile', details: 'Vio su perfil' }, { orderingKey: userId1 });

    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'login', details: 'Login desde app móvil' }, { orderingKey: userId2 });

    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'update_settings', details: 'Cambió preferencia de email' }, { orderingKey: userId1 });

    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'post_comment', details: 'Comentó en el artículo XYZ' }, { orderingKey: userId2 });

    // Un trabajo sin orderingKey en la cola ordenada (se procesará sin garantía de orden respecto a otros sin clave)
    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'system_health_check', details: 'Chequeo general' });

    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'logout', details: 'Logout desde web' }, { orderingKey: userId1 });
    await jobPublisher.publish(ORDERED_USER_ACTIVITY_QUEUE, { activityType: 'view_dashboard', details: 'Accedió al dashboard' }, { orderingKey: userId2 });


    appLogger.info('Trabajos de ejemplo publicados.');
    appLogger.info('Aplicación configurada. QueueWorkers corriendo. Presiona Ctrl+C para salir.');

  } catch (err) {
    appLogger.error(`Error en la configuración principal de la aplicación: ${err.message}`, err);
    await gracefulShutdown('ERROR_SETUP');
  }
}

async function gracefulShutdown(signal) {
  appLogger.info(`Recibido ${signal}. Iniciando cierre elegante...`);
  const shutdowns = [];

  if (emailQueueWorker) {
    appLogger.info(`Deteniendo Email QueueWorker para la cola '${EMAIL_QUEUE}'...`);
    shutdowns.push(emailQueueWorker.stop());
  }
  if (whatsappQueueWorker) {
    appLogger.info(`Deteniendo WhatsApp QueueWorker para la cola '${WHATSAPP_QUEUE}'...`);
    shutdowns.push(whatsappQueueWorker.stop());
  }
  if (userActivityWorker) {
    appLogger.info(`Deteniendo UserActivity QueueWorker para la cola '${ORDERED_USER_ACTIVITY_QUEUE}'...`);
    shutdowns.push(userActivityWorker.stop());
  }

  if (shutdowns.length > 0) {
    try {
      await Promise.all(shutdowns);
      appLogger.info('Todos los QueueWorkers han sido señalados para detenerse.');
    } catch (err) {
      appLogger.error(`Error durante la espera de la detención de QueueWorkers: ${err.message}`, err);
    }
  } else {
    appLogger.info('No hay QueueWorkers activos para detener.');
  }

  const rawClient = redisSingleton?.getRawClient ? redisSingleton.getRawClient() : null;
  if (redisSingleton && typeof redisSingleton.disconnect === 'function' &&
    (rawClient && (rawClient.status === 'ready' || rawClient.status === 'connecting' || rawClient.status === 'reconnecting'))) {
    appLogger.info('Desconectando de Redis...');
    try {
      await redisSingleton.disconnect();
      appLogger.info('Desconectado de Redis.');
    } catch (disconnectError) {
      appLogger.error(`Error al desconectar de Redis: ${disconnectError.message}`, disconnectError);
    }
  } else {
    appLogger.warn('RedisSingleton o su cliente crudo no está en estado para desconectar, o el método disconnect no está disponible, o ya está desconectado.');
  }
  appLogger.info('Cierre elegante completado. Saliendo.');
  process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

main().catch(async (err) => {
  appLogger.error(`Error no manejado en la cadena de ejecución principal: ${err.message}`, err);
  await gracefulShutdown('ERROR_UNHANDLED_MAIN');
});
