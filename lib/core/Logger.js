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
    const currentDir = getDirname(import.meta.url);
    return path.resolve(currentDir, '..', '..');
};

class Logger {
    constructor(moduleName) {
        const projectRoot = getProjectRoot();
        const logsDir = path.join(projectRoot, 'logs');

        if (!fs.existsSync(logsDir)) {
            fs.mkdirSync(logsDir, { recursive: true });
        }

        this.logFile = path.join(logsDir, config.LOG_FILE || 'app.log');
        this.moduleName = moduleName;

        const transportTargets = [];

        transportTargets.push({
            target: 'pino-pretty',
            options: {
                colorize: true,
                translateTime: 'SYS:standard',
                ignore: 'pid,hostname',
                messageFormat: '[{moduleName}] {msg}'
            },
            level: config.LOG_LEVEL_CONSOLE || 'debug',
        });

        // File transport
        transportTargets.push({
            target: 'pino/file',
            options: {
                destination: this.logFile,
                mkdir: true,
            },
            level: config.LOG_LEVEL_FILE || 'info',
        });

        this.logger = pino({
            level: config.LOG_LEVEL || 'debug',
            base: {
                moduleName: this.moduleName,
            },
            timestamp: pino.stdTimeFunctions.isoTime,
            transport: {
                targets: transportTargets,
            },
        });
    }

    setLevel(level) {
        this.logger.level = level;
    }

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