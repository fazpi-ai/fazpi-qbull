
import pino from 'pino';
import fs from 'fs';
import path from 'path';
import config from '../config.js'; // Assuming config.js is in lib/

const getDirname = (importMetaUrl) => {
    const url = new URL(importMetaUrl);
    return path.dirname(url.pathname);
};

const getProjectRoot = () => {
    const currentDir = getDirname(import.meta.url); // lib/core
    return path.resolve(currentDir, '../..'); // project-root
};

class Logger {
    constructor(moduleName) {
        const projectRoot = getProjectRoot();
        const logsDir = path.join(projectRoot, 'logs');

        if (!fs.existsSync(logsDir)) {
            fs.mkdirSync(logsDir, { recursive: true });
        }

        this.logFile = path.join(logsDir, config.LOG_FILE || 'app.log');
        this.moduleName = moduleName; // Used in `base` option for pino

        const transportTargets = [];

        // Console transport (pino-pretty)
        transportTargets.push({
            target: 'pino-pretty',
            options: {
                colorize: true,
                translateTime: 'SYS:standard', // Formats the 'time' field based on system timezone
                ignore: 'pid,hostname',       // Properties to ignore in the output
                // {moduleName} will be available if set in `base` or via `logger.child()`
                // {msg} is the main log message.
                messageFormat: '[{moduleName}] {msg}'
            },
            level: config.LOG_LEVEL_CONSOLE || 'debug',
        });

        // File transport
        transportTargets.push({
            target: 'pino/file', // Uses a worker thread
            options: { // These options must be serializable
                destination: this.logFile, // string, serializable
                mkdir: true,               // boolean, serializable
            },
            level: config.LOG_LEVEL_FILE || 'info',
        });

        this.logger = pino({
            level: config.LOG_LEVEL || 'debug', // Overall minimum log level
            // Add moduleName to all log entries automatically via the 'base' option
            base: {
                moduleName: this.moduleName,
            },
            timestamp: pino.stdTimeFunctions.isoTime, // Use ISO time format for timestamps
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
        if (objOrMsg instanceof Error) { // Though less common to log Errors at INFO
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
            // If the first argument is an error, pass it as 'err' object to pino
            // Use the first string in 'args' as message, or default to error's message
            const msg = (typeof args[0] === 'string') ? args.shift() : objOrMsg.message;
            this.logger.error({ err: objOrMsg }, msg, ...args);
        } else {
            // Standard logging: first arg is message string or an object of fields
            this.logger.error(objOrMsg, ...args);
        }
    }

    // writeToFile method is generally not needed when using pino's file transport
    // as it handles file writing efficiently.
    // writeToFile(message) { ... }
}

export default Logger;