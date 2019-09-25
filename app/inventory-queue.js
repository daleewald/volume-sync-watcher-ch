const redis = require('redis');
const Queue = require('bee-queue');
const chalky = require('chokidar');
const localInventory = require('./local-inventory');
const logger = require('./logger');

const qTypeNames = {
    aws: 'AWS-INVENTORY-EVENTS',
    gcp: 'GCP-INVENTORY-EVENTS'
};

class InventoryQueue {
    queueConfig;
    queueName;
    bucketName;
    mountPath;
    syncDir;
    excludeFilePattern;
    includeFilePattern;
    suppressInventoryScan; // {boolean} - If true, local and remote inventory is not compared at setup.
    ignoreLocalDeletes; // {boolean} - If true, deletions from the monitored syncDir structure are not deleted from remote.
    eq;
    watcher;
    rclient;

    /** 
     * @param {Object} queueConfig object consisting of: 
     * - {string} type - The type of cloud vendor : 'aws' or 'gcp'
     * - {string} bucketName - Target remote bucket name
     * - {string} syncDir - Source base directory to monitor
     * - {string} excludeFilePattern - Anymatch file pattern to exclude
     * - {string} includeFilePattern - Anymatch file pattern to include
     * 
     * @param {string} mountPath - the base path of the volume mount - syncDir is relative to this path
     **/
    constructor( queueConfig, mountPath ) {
        const requiredProps = ['type','bucketName', 'syncDir','excludeFilePattern','includeFilePattern'];
        requiredProps.forEach( (prop) => {
            if (queueConfig[prop] === undefined || queueConfig[prop] === '') {
                throw new Error(prop + ' is a required property in queueConfig parameter object');
            }
        });
        if ( mountPath === undefined) {
            throw new Error('mountPath is required parameter.');
        }

        logger.debug('Starting InventoryQueue with config:', JSON.stringify( queueConfig ) );

        this.queueConfig = queueConfig;
        this.queueName = qTypeNames[queueConfig.type];
        this.bucketName = queueConfig.bucketName;
        this.syncDir = mountPath + queueConfig.syncDir;
        this.excludeFilePattern = queueConfig.excludeFilePattern;
        this.includeFilePattern = queueConfig.includeFilePattern;
        this.ignoreLocalDeletes = queueConfig.ignoreLocalDeletes || false;
        this.suppressInventoryScan = queueConfig.suppressInventoryScan || false;

        this.rclient = redis.createClient( { host: queueConfig.queueHost || 'redis' });
    }

    setupQueue( suppressInventoryScan, ignoreLocalDeletes ) {
        this.eq = new Queue(this.queueName, {
            redis: {
                host: 'redis'
            },
            isWorker: false
        });
        
        this.eq.on('ready', () => {
            logger.info(this.queueName + ' queue is ready.');
        });
        
        this.eq.on('error', (err) => {
            logger.error('Queue error: ', err.message);
        });
        
        this.eq.on('succeeded', (job, result) => {
            logger.info(this.queueName, '#', job.id, 'OK');
        });
        
        const watchContext = ((this.syncDir.endsWith('/') || this.syncDir.endsWith('\\'))
         ? this.syncDir : this.syncDir + '/') + (this.includeFilePattern || '');

        
        this.watcher = chalky.watch(watchContext, { 
            ignored: this.excludeFilePattern,
            persistent: true,
            depth: 99,
            usePolling: true,
            awaitWriteFinish: true
         });
        
         this.watcher
         .on('error', err => logger.error(err))
         .on('ready', path => {
             logger.info('Watcher ready:',watchContext);

             if (suppressInventoryScan || this.suppressInventoryScan) {
                 logger.info(this.queueName,'Inventory scan and comparison disabled.');
             } else {
                const collection = this.watcher.getWatched();
                this.inventoryCheckup( collection, this.syncDir, this.bucketName );
             }
        
             this.watcher.on('all', (evt, sourcePath) => {
                const targetPath = sourcePath.replace(this.syncDir, '');
                logger.debug('Watch Event: ', evt, sourcePath);
                let jobdata = {
                    sourceFileName: sourcePath,
                    targetFileName: targetPath,
                    targetBucket: this.bucketName
                }
                if (this.queueConfig.type === 'aws') {
                    if (this.queueConfig.region !== undefined) {
                        jobdata['region'] = this.queueConfig.region;
                    }
                }
                let retries = 0;
        
                 if (evt === 'addDir') {
                    jobdata['event'] = 'addDir';
                 } else
                 if (evt === 'unlinkDir') {
                    jobdata['event'] = 'removeDir';
                 } else
                 if (evt === 'add' || evt === 'change') {
                     retries = 2;
                    jobdata['event'] = 'update';
                 } else
                 if (evt === 'unlink') {
                     if (ignoreLocalDeletes || this.ignoreLocalDeletes) {
                        logger.debug('Local delete ignored.');
                     } else {
                        jobdata['event'] = 'remove';
                        retries = 2;
                    }
                 }
        
                 if (jobdata.event !== undefined) {
                    this.createFileJob( jobdata, retries );
                 }
             });
         });
    }

    stopQueue() {
        this.watcher.close();
    }
    
    createFileJob( jobdata, retries ) {
        this.eq.createJob(jobdata)
        .retries(retries)
        .save()
        .then((job) =>  {
            logger.info(this.queueName, '#', job.id, job.data.event, job.data.targetFileName || '');
            job.on('succeeded', ( result ) => {
                logger.info(this.queueName,job.id,'OK');
            });
            job.on('failed', ( err ) => {
                logger.error(this.queueName, '#', job.id,'failed. Error:', err.message);
            });
            job.on('retrying', ( err ) => {
                logger.info(this.queueName, '#', job.id,'retrying. Error:', err.message);
            });
        });
     }
     
    inventoryCheckup( collection ) {
        logger.info('Queue a worker request to fetch remote inventory, then check local');
        let jobdata = {
            targetBucket: this.bucketName,
            event: 'inventory',
            projection: ['name','updated']
        }
        if (this.queueConfig.type === 'aws') {
            if (this.queueConfig.region !== undefined) {
                jobdata['region'] = this.queueConfig.region;
            }
        }
        this.eq.createJob(jobdata)
        .save()
        .then(( job ) => {
            logger.info(this.queueName, '#', job.id, job.data.event);
            job.on('succeeded', ( cachedResultKey ) => {
                logger.info('Fetch inventory cache key', cachedResultKey);
                this.rclient.get( cachedResultKey, ( err, result ) => {
                    if (err) {
                        logger.error(err);
                    } else {
                        const bucket_inv = JSON.parse(result);
                        logger.info('Number of items in Bucket inventory:', bucket_inv.length);
        
                        const inv = localInventory.list(collection, this.syncDir);
                        logger.info('Number of items in Local inventory:', inv.length);
        
                        // Test local inventory for new or modified vs. bucket version.
                        inv.forEach( ( localfile ) => {
                            if (localfile.path !== undefined) {
                                const localtime = new Date(localfile.mtime).getTime();
                                const remotefile = bucket_inv.find( ( bucketfile ) => {
                                    const localpath = ((localfile.path.startsWith('/')) ? localfile.path.substr(1) : localfile.path);
                                    logger.debug('bucketfile.name[',bucketfile.name,'] === localpath[', localpath,']');
                                    return bucketfile.name === localpath;
                                });
                                let jobdata = {
                                    targetBucket: this.bucketName,
                                    sourceFileName: localfile.fullpath
                                }
                                if (this.queueConfig.type === 'aws') {
                                    if (this.queueConfig.region !== undefined) {
                                        jobdata['region'] = this.queueConfig.region;
                                    }
                                }
                                if (remotefile === undefined) {
                                    logger.info('Queue new file:',localfile.path);
                                    jobdata['targetFileName'] = localfile.path;
                                    jobdata['event'] = 'update';
                                } else {
                                    const remotetime = new Date(remotefile.updated).getTime();
                                    const timediff = localtime - remotetime;
                                    if (timediff > 0) {
                                        logger.info('Queue modified file:',localfile.path);
                                        jobdata['targetFileName'] = remotefile.name;
                                        jobdata['event'] = 'update';
                                    }
                                }
                                if (jobdata.event !== undefined) {
                                    this.createFileJob( jobdata );
                                }
                            }
                        });
                    }
                } );
            });
            job.on('failed', ( err ) => {
                logger.error(this.queueName, 'INVENTORY #',job.id,'failed. Error:', err.message);
            });
            job.on('retrying', ( err ) => {
                logger.warn(this.queueName, 'INVENTORY #',job.id,'retrying. Error:', err.message);
            });
        });
     }
}

module.exports = InventoryQueue;