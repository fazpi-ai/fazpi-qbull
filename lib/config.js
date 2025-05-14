import dotenv from 'dotenv';

import fs from 'fs';

const env = process.env.NODE_ENV || 'development';

if (env === 'production') {
    dotenv.config({ path: '.env.production' });
} else {
    // For development, you can have a .env.development or just .env
    // Here we assume .env.development, but you can adjust it to just .env if that's your case
    dotenv.config({ path: '.env.development' });
    if (!fs.existsSync('.env.development') && fs.existsSync('.env')) {
        console.log("'.env.development' not found, falling back to '.env'");
        dotenv.config({ path: '.env', override: true }); // Override if .env.development does not exist but .env does
    }
}

export default {
    REDIS_HOST: process.env.REDIS_HOST || '127.0.0.1',
    REDIS_PORT: process.env.REDIS_PORT || 6379,
    REDIS_DB: parseInt(process.env.REDIS_DB || '0', 10), // Ensure it's a number
    REDIS_USER: process.env.REDIS_USER || undefined, // Use undefined if no user for ioredis
    REDIS_PASSWORD: process.env.REDIS_PASSWORD || undefined, // Use undefined if no password

    LOG_FILE: process.env.LOG_FILE || 'app.log',
    LOG_LEVEL: process.env.LOG_LEVEL || 'debug', // General level for pino
    LOG_LEVEL_CONSOLE: process.env.LOG_LEVEL_CONSOLE || 'debug', // Level for console transport
    LOG_LEVEL_FILE: process.env.LOG_LEVEL_FILE || 'info',       // Level for file transport
};