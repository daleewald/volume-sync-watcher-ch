const redis = require('redis');
const Queue = require('bee-queue');
const chalky = require('chokidar');
const localInventory = require('./local-inventory');

class InventoryQueue {
    queueConfig;
    queueName;
    bucketName;
    baseDir;
    excludeFilePattern;
    includeFilePattern;
    eq;
    watcher;
    rclient;

    /** 
     * @param {Object} queueConfig object consisting of: 
     * - {string} queueHost - (optional) Service name of the queue host - default 'redis'
     * - {string} queueName - Name of the queue to publish to
     * - {string} bucketName - Target remote bucket name
     * - {string} baseDir - Source base directory to monitor
     * - {string} excludeFilePattern - Anymatch file pattern to exclude
     * - {string} includeFilePattern - Anymatch file pattern to include
     **/
    constructor( queueConfig ) {
        const props = ['queueName','bucketName','baseDir','excludeFilePattern','includeFilePattern'];
        props.forEach( (prop) => {
            if (queueConfig[prop] === undefined || queueConfig[prop] === '') {
                throw new Error(prop + ' is required in queueConfig');
            }
        });

        this.queueConfig = queueConfig;
        this.queueName = queueConfig.queueName;
        this.bucketName = queueConfig.bucketName;
        this.baseDir = queueConfig.baseDir;
        this.excludeFilePattern = queueConfig.excludeFilePattern;
        this.includeFilePattern = queueConfig.includeFilePattern;

        this.rclient = redis.createClient( { host: queueConfig.queueHost || 'redis' });
        
    }

    /**
     * 
     * @param {boolean} suppressInventoryScan - If true, local and remote inventory is not compared at setup.
     * @param {*} ignoreLocalDeletes - If true, deletions from the monitored baseDir structure are not deleted from remote.
     */
    setupQueue( suppressInventoryScan, ignoreLocalDeletes ) {
        this.eq = new Queue(this.queueName, {
            redis: {
                host: 'redis'
            },
            isWorker: false
        });
        
        this.eq.on('ready', () => {
            console.log(this.queueName + ' queue is ready.');
        });
        
        this.eq.on('error', (err) => {
            console.log('Queue error: ', err.message);
        });
        
        this.eq.on('succeeded', (job, result) => {
            console.log('Job', job.id, 'succeeded. Result:', result);
        });
        
        const watchContext = ((this.baseDir.endsWith('/') || this.baseDir.endsWith('\\'))
         ? this.baseDir : this.baseDir + '/') + (this.includeFilePattern || '');

        
        this.watcher = chalky.watch(watchContext, { 
            ignored: this.excludeFilePattern,
            persistent: true,
            depth: 99,
            usePolling: true,
            awaitWriteFinish: true
         });
        
         const logger = console.log.bind(console);
        
         this.watcher
         .on('error', err => logger('error:', err))
         .on('ready', path => {
             logger('Watcher ready:',watchContext);

             if (!suppressInventoryScan) {
                const collection = this.watcher.getWatched();
                this.inventoryCheckup( collection, this.baseDir, this.bucketName );
             }
        
             this.watcher.on('all', (evt, sourcePath) => {
                const targetPath = sourcePath.replace(this.baseDir, '');
                console.log('Watch Event: ', evt, sourcePath);
                let jobdata = {
                    sourceFileName: sourcePath,
                    targetFileName: targetPath,
                    targetBucket: this.bucketName
                }
                let retries = 0;
        
                 if (evt === 'addDir') {
                    jobdata['event'] = 'addDir';
                 } else
                 if (evt === 'unlinkDir') {
                    jobdata['event'] = 'removeDir';
                 } else
                 if (evt === 'add' || evt === 'change') {
                    jobdata['event'] = 'update';
                 } else
                 if (evt === 'unlink') {
                     if (!ignoreLocalDeletes) {
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
    
    createFileJob( jobdata, retries ) {
        this.eq.createJob(jobdata)
        .retries(retries)
        .save()
        .then((job) =>  {
            console.log('Job', job.id, 'created for ', job.data);
            job.on('succeeded', ( result ) => {
                console.log('Job',job.id,'succeeded', result);
            });
            job.on('failed', ( err ) => {
                console.log('Job',job.id,'failed. Error:', err.message);
            });
            job.on('retrying', ( err ) => {
                console.log('Job',job.id,'retrying. Error:', err.message);
            });
        });
     }
     
    inventoryCheckup( collection ) {
        console.log('Queue a worker request to fetch remote inventory, then check local');
        this.eq.createJob({targetBucket: this.bucketName, event: 'inventory', projection: ['name','updated']})
        .save()
        .then(( job ) => {
            console.log('Job', job.id, 'created for', job.data);
            job.on('succeeded', ( cachedResultKey ) => {
                this.rclient.get( cachedResultKey, ( err, result ) => {
                    if (err) {
                        console.log('ERROR', err);
                    } else {
                        const bucket_inv = JSON.parse(result);
                        console.log('Number of items in Bucket inventory:', bucket_inv.length);
        
                        const inv = localInventory.list(collection, this.baseDir);
                        console.log('Number of items in Local inventory:', inv.length);
        
                        // Test local inventory for new or modified vs. bucket version.
                        inv.forEach( ( localfile ) => {
                            if (localfile.path !== undefined) {
                                const localtime = new Date(localfile.mtime).getTime();
                                const remotefile = bucket_inv.find( ( bucketfile ) => {
                                    return bucketfile.name === localfile.path;
                                });
                                if (remotefile === undefined) {
                                    console.log('Queue new file:',localfile.path);
                                    this.createFileJob({sourceFileName: localfile.fullpath, targetBucket: this.bucketName, targetFileName: localfile.path, event: 'update'});
                                } else {
                                    const remotetime = new Date(remotefile.updated).getTime();
                                    const timediff = localtime - remotetime;
                                    if (timediff > 0) {
                                        console.log('Queue modified file:',localfile.path);
                                        this.createFileJob({sourceFileName: localfile.fullpath, targetBucket: this.bucketName, targetFileName: remotefile.name, event: 'update'});
                                    }
                                }
                            }
                        });
                    }
                } );
            });
            job.on('failed', ( err ) => {
                console.log('REMOTE INVENTORY Job',job.id,'failed. Error:', err.message);
            });
            job.on('retrying', ( err ) => {
                console.log('REMOTE INVENTORY Job',job.id,'retrying. Error:', err.message);
            });
        });
     }
}

module.exports = InventoryQueue;