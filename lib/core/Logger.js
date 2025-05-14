import pino from 'pino';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import config from '../config.js';

// Helper to get __dirname in ES modules
const getDirname = (importMetaUrl) => {
    return path.dirname(fileURLToPath(importMetaUrl));
};

const getProjectRoot = () => {
    const currentDir = getDirname(import.meta.url); // No Spanish here
    return path.resolve(currentDir, '..', '..'); // No Spanish here
};

class Logger {
    constructor(moduleName) {
        const projectRoot = getProjectRoot(); // No Spanish here
        const logsDir = path.join(projectRoot, 'logs'); // No Spanish here

        if (!fs.existsSync(logsDir)) {
            fs.mkdirSync(logsDir, { recursive: true }); // No Spanish here
        }

        this.logFile = path.join(logsDir, config.LOG_FILE || 'app.log'); // No Spanish here
        this.moduleName = moduleName; // No Spanish here

        const transportTargets = []; // No Spanish here

        transportTargets.push({ // No Spanish here
            target: 'pino-pretty', // No Spanish here
            options: { // No Spanish here
                colorize: true, // No Spanish here
                translateTime: 'SYS:standard', // No Spanish here
                ignore: 'pid,hostname', // No Spanish here
                messageFormat: '[{moduleName}] {msg}' // Language-neutral
            },
            level: config.LOG_LEVEL_CONSOLE || 'debug', // No Spanish here
        });

        // File transport
        transportTargets.push({
            target: 'pino/file', // No Spanish here
            options: { // No Spanish here
                destination: this.logFile, // No Spanish here
                mkdir: true, // No Spanish here
            },
            level: config.LOG_LEVEL_FILE || 'info', // No Spanish here
        });

        this.logger = pino({ // No Spanish here
            level: config.LOG_LEVEL || 'debug', // No Spanish here
            base: { // No Spanish here
                moduleName: this.moduleName, // No Spanish here
            },
            timestamp: pino.stdTimeFunctions.isoTime, // No Spanish here
            transport: { // No Spanish here
                targets: transportTargets, // No Spanish here
            },
        });
    }

    setLevel(level) { // No Spanish here
        this.logger.level = level; // No Spanish here
    }

    // Methods debug, info, warn, error are standard and their logic is language-neutral
    // No Spanish comments or strings within these methods.
    debug(objOrMsg, ...args) {
        if (objOrMsg instanceof Error) {
            const msg = (typeof args[0] === 'string') ? args.shift() : objOrMsg.message;
            this.logger.debug({ err: objOrMsg }, msg, ...args);
        } else {
            this.logger.debug(objOrMsg, ...args);
        }
    }

    info(objOrMsg, ...args) {
        if (objOrMsg instanceof Error) {
            const msg = (typeof args[0] === 'string') ? args.shift() : objOrMsg.message;
            this.logger.info({ err: objOrMsg }, msg, ...args);
        } else {
            this.logger.info(objOrMsg, ...args);
        }
    }

    warn(objOrMsg, ...args) {
        if (objOrMsg instanceof Error) {
            const msg = (typeof args[0] === 'string') ? args.shift() : objOrMsg.message;
            this.logger.warn({ err: objOrMsg }, msg, ...args);
        } else {
            this.logger.warn(objOrMsg, ...args);
        }
    }

    error(objOrMsg, ...args) {
        if (objOrMsg instanceof Error) {
            const msg = (typeof args[0] === 'string') ? args.shift() : objOrMsg.message;
            this.logger.error({ err: objOrMsg }, msg, ...args);
        } else {
            this.logger.error(objOrMsg, ...args);
        }
    }
}

export default Logger;