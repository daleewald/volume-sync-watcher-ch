const InventoryQueue = require('./inventory-queue');
const logger = require('./logger');
const redis = require('redis');

logger.info('Watcher process starting');

// create redis client and define channel IDs
const rclient = redis.createClient({ host: 'redis' });
const CHNL_CONTAINER_LOG_LEVEL = 'CONTAINER-LOG-LEVEL';

rclient.on("message", ( channel, message ) => {
    switch (channel) {
        case CHNL_CONTAINER_LOG_LEVEL:
            // should have been validated before being queued
            logger.level = message;
            break;
        default:
    }
});
rclient.subscribe(CHNL_CONTAINER_LOG_LEVEL);

let queueConfigs = [
    {
        queueName:'AWS-INVENTORY-EVENTS',
        bucketName: process.env.S3_BUCKET_NAME,
        baseDir: process.env.BASE_DIR,
        excludeFilePattern: process.env.EXCLUDE_FILE_PATTERN,
        includeFilePattern: process.env.INCLUDE_FILE_PATTERN,
    },
    {
        queueName: 'GCP-INVENTORY-EVENTS',
        bucketName: process.env.GCP_BUCKET_NAME,
        baseDir: process.env.BASE_DIR,
        excludeFilePattern: process.env.EXCLUDE_FILE_PATTERN,
        includeFilePattern: process.env.INCLUDE_FILE_PATTERN,
    }
];

let queues = [];

queueConfigs.forEach( (queueConfig) => {
    const queue = new InventoryQueue( queueConfig );
    queue.setupQueue();
    queues.push(queue);
});
