const InventoryQueue = require('./inventory-queue');
const logger = require('./logger');
const redis = require('redis');

logger.info('Watcher process starting');

// create redis client and define channel IDs
const rclient = redis.createClient({ host: 'redis' });
const CHNL_CONTAINER_LOG_LEVEL = 'CONTAINER-LOG-LEVEL';
const CHNL_QUEUE_CONFIG = 'INVENTORY-QUEUE-CONFIG';

rclient.on("message", ( channel, message ) => {
    switch (channel) {
        case CHNL_CONTAINER_LOG_LEVEL:
            // Receive new log level. Value should be reliable, validated before being queued
            logger.level = message;
            break;
        case CHNL_QUEUE_CONFIG:
            // Receive full queue config map
            manageQueues( message );
        default:
    }
});
rclient.subscribe(CHNL_CONTAINER_LOG_LEVEL);
rclient.subscribe(CHNL_QUEUE_CONFIG);


let queueConfigs = [];

function manageQueues( fromConfig ) {
    if (typeof fromConfig === 'string') {
        try {
            // json array of maps
            const configQCs = JSON.parse( fromConfig );
        } catch ( err ) {
            logger.error('ERROR from manageQueues');
            logger.error( err );
            throw err;
        }

        // find existing queueConfigs and compare settings, disable queues that changed and reduce list
        queueConfigs = queueConfigs.reduce( ( currentConfigs, config ) => {
            // search newConfigs to see if this config is unchanged
            const matchedQC = configQCs.find( ( qc ) => {
                return isQueueConfigMatch( qc, config );
            });
            if (matchedQC === undefined) {
                config.queue.stopQueue();
                delete config.queue;
            } else {
                currentConfigs.push(config);
            }
            return currentConfigs;
        }, []);

        // Next reduce configQCs and init the new ones, accumulate in current queueConfigs
        queueConfigs = configQCs.reduce( ( _configs, config ) => {
            // search the accumulator to see if this config is net new
            const matchedQC = _configs.find( ( qc ) => {
                return isQueueConfigMatch( qc, config );
            });
            if (matchedQC === undefined) {
                const queue = new InventoryQueue( config );
                queue.setupQueue();
                config['queue'] = queue;
                _configs.push(config);
            }
            return _configs;
        }, queueConfigs);
    }
}

function isQueueConfigMatch( configA, configB ) {
    return (                
        configA.syncDir === configB.syncDir &&
        configA.includeFilePattern === configB.includeFilePattern &&
        configA.excludeFilePattern === configB.excludeFilePattern
    );
}