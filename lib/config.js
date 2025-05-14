import dotenv from 'dotenv';

const env = process.env.NODE_ENV || 'development';

if (env === 'production') {
    dotenv.config({ path: '.env.production' });
} else {
    dotenv.config({ path: '.env.development' });
}

export default {
    REDIS_HOST: process.env.REDIS_HOST || '127.0.0.1',
    REDIS_PORT: process.env.REDIS_PORT || 6379,
    REDIS_DB: process.env.REDIS_DB || 0,
    REDIS_USER: process.env.REDIS_USER || 'default',
    REDIS_PASSWORD: process.env.REDIS_PASSWORD || 'password',

    LOG_FILE: process.env.LOG_FILE || 'app.log',
    LOG_LEVEL_CONSOLE: process.env.LOG_LEVEL_CONSOLE || 'debug',
};