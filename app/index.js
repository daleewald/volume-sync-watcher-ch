const InventoryQueue = require('./inventory-queue');
const logger = require('./logger');
const redis = require('redis');

logger.info('Watcher process starting');

// create redis client and define channel IDs
const rclient = redis.createClient({ host: 'redis' });
const CHNL_CONTAINER_LOG_LEVEL = 'CONTAINER-LOG-LEVEL';
const CHNL_QUEUE_CONFIG = 'QUEUE_CONFIG';

rclient.on("message", ( channel, message ) => {
    switch (channel) {
        case CHNL_CONTAINER_LOG_LEVEL:
            // Receive new log level. Value should be reliable, validated before being queued
            logger.level = message;
            break;
        case CHNL_QUEUE_CONFIG:
            // Receive full queue config map

        default:
    }
});
rclient.subscribe(CHNL_CONTAINER_LOG_LEVEL);
rclient.subscribe(CHNL_QUEUE_CONFIG);

const qTypeNames = {
    aws: 'AWS-INVENTORY-EVENTS',
    gcp: 'GCP-INVENTORY-EVENTS'
};

let queueConfigs = [
    {
        queueName: qTypeNames.aws,
        bucketName: process.env.S3_BUCKET_NAME,
        baseDir: process.env.BASE_DIR + 'syncdir/',
        excludeFilePattern: process.env.EXCLUDE_FILE_PATTERN,
        includeFilePattern: process.env.INCLUDE_FILE_PATTERN,
    },
    {
        queueName: qTypeNames.gcp,
        bucketName: process.env.GCP_BUCKET_NAME,
        baseDir: process.env.BASE_DIR + 'syncdir/',
        excludeFilePattern: process.env.EXCLUDE_FILE_PATTERN,
        includeFilePattern: process.env.INCLUDE_FILE_PATTERN,
    }
];

let queues = [];

function manageQueues( fromConfig ) {
    if (typeof fromConfig === 'string') {
        // json array of maps
        const configQCs = JSON.parse( fromConfig );
        
        // find existing queues to disable by reduction of queueConfigs
        let qsToClose = [];
        queueConfigs = queueConfigs.reduce( ( acc, config, idx ) => {
            // search newConfigs to see if this config is still active
            const exConfig = configQCs.find( ( qc ) => {
                return qc.baseDir === config.baseDir;
            });
            if (exConfig === undefined) {
                // when not found collect matching queue for closure
                qsToClose.push(queues[idx]);
            } else {
                acc.push(config);
            }
            return acc;
        }, []);

    }
}
queueConfigs.forEach( (queueConfig) => {
    const queue = new InventoryQueue( queueConfig );
    queue.setupQueue();
    queueConfig['queue'] = queue;
    //queues.push(queue);
});
