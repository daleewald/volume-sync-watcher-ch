const InventoryQueue = require('./inventory-queue');
const logger = require('./logger');
const redis = require('redis');

logger.info('Watcher process starting');

const MOUNT_PATH = process.env.BASE_DIR;

const redisClient = redis.createClient({ host: 'redis' });
const KEY_INVENTORY_QUEUE_CONFIG = 'INVENTORY-QUEUE-CONFIG';

let configLastModified = '';
let queueConfigs = [];

function getInventoryQueueConfig() {
    redisClient.get(KEY_INVENTORY_QUEUE_CONFIG, ( err, config ) => {
        if ( err ) {
            logger.error( err );
        } else {
            logger.info('Cache Key ', KEY_INVENTORY_QUEUE_CONFIG, 'Value:', config);
            if (config !== null && config !== '') {
                const configObj = JSON.parse(config);
                configLastModified = configObj.modified;
                manageQueues( configObj.body );
            }
        }
    });
}

function manageQueues( newConfigs ) {

    // find existing queueConfigs and compare settings, disable queues that changed and reduce list
    queueConfigs = queueConfigs.reduce( ( currentConfigs, config ) => {
        // search newConfigs to see if this config is unchanged
        const matchedQC = newConfigs.find( ( qc ) => {
            return isQueueConfigMatch( qc, config );
        });
        if (matchedQC === undefined) {
            logger.info('Stopping queue', config);
            config.queue.stopQueue();
            delete config.queue;
        } else {
            currentConfigs.push(config);
        }
        return currentConfigs;
    }, []);

    // Next reduce newConfigs and init the new ones, accumulate in current queueConfigs
    queueConfigs = newConfigs.reduce( ( _configs, config ) => {
        // search the accumulator to see if this config is net new
        const matchedQC = _configs.find( ( qc ) => {
            return isQueueConfigMatch( qc, config );
        });
        if (matchedQC === undefined) {
            const queue = new InventoryQueue( config, MOUNT_PATH );
            logger.info('Setting up queue', config);
            queue.setupQueue();
            config['queue'] = queue;
            _configs.push(config);
        }
        return _configs;
    }, queueConfigs);
}

function isQueueConfigMatch( configA, configB ) {
    logger.info('*** isQueueConfigMatch ',
        '( ' +
            configA.type + ' === ' + configB.type + ' && ' +
            configA.bucketName + ' === ' + configB.bucketName + ' && ' +
            configA.syncDir + ' === ' + configB.syncDir + ' && ' +
            configA.includeFilePattern + ' === ' + configB.includeFilePattern + ' && ' +
            configA.excludeFilePattern + ' === ' + configB.excludeFilePattern + ' && ' +
            (configA.ignoreLocalDeletes || false + ' === ' + configB.ignoreLocalDeletes || false) + ' && ' +
            (configA.suppressInventoryScan || false + ' === ' + configB.ignoreLocalDeletes || false)
        + ' )'
    );
    return (
        configA.type === configB.type &&
        configA.bucketName === configB.bucketName &&
        configA.syncDir === configB.syncDir &&
        configA.includeFilePattern === configB.includeFilePattern &&
        configA.excludeFilePattern === configB.excludeFilePattern &&
        (configA.ignoreLocalDeletes || false === configB.ignoreLocalDeletes || false) &&
        (configA.suppressInventoryScan || false === configB.ignoreLocalDeletes || false)
    );
}

/* INITIALIZATION
 * 1) Call getInventoryQueueConfig() to check cache for a config definition
 *  -- the cache may not have been populated, depending on load timing, continue:
 * 2) Setup Admin subscriber and message listener
 *  -- As Admin service gathers config definition or definition is updated,
 *     subscriber calls getInventoryQueueConfig()
 */
getInventoryQueueConfig();

// create redis client to subscribe to admin channels; define channel IDs
const adminSubscriber = redisClient.duplicate();
const CHNL_CONTAINER_LOG_LEVEL = 'CONTAINER-LOG-LEVEL';
const CHNL_INVENTORY_QUEUE_CONFIG_MOD = KEY_INVENTORY_QUEUE_CONFIG;

adminSubscriber.on("message", ( channel, message ) => {
    switch (channel) {
        case CHNL_CONTAINER_LOG_LEVEL:
            // Receive new log level. Value should be reliable, validated before being queued
            logger.level = message;
            break;
        case CHNL_INVENTORY_QUEUE_CONFIG_MOD:
            // Received a modified date of the queueConfigs.json; compare for update.
            const configModified = message;
            if (configModified !== configLastModified) {
                getInventoryQueueConfig();
            }
            break;
        default:
    }
});
adminSubscriber.subscribe(CHNL_CONTAINER_LOG_LEVEL);
adminSubscriber.subscribe(CHNL_INVENTORY_QUEUE_CONFIG_MOD);