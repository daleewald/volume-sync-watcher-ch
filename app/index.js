const redis = require('redis');
const Queue = require('bee-queue');
const chalky = require('chokidar');
const inventory = require('./inventory');

const BASE_DIR = process.env.BASE_DIR;
const watchContext = ((BASE_DIR.endsWith('/') || BASE_DIR.endsWith('\\')) ? BASE_DIR : BASE_DIR + '/') + (process.env.INCLUDE_FILE_PATTERN || '');

console.log('create queue for watcher (Q)');

const eq = new Queue('watchEvents', {
    redis: {
        host: 'redis'
    },
    isWorker: false
});

const rclient = redis.createClient( { host: 'redis' });

eq.on('ready', () => {
    console.log('watchEvents queue is ready.');
});

eq.on('error', (err) => {
    console.log('Queue error: ', err.message);
});

eq.on('succeeded', (job, result) => {
    console.log('Job', job.id, 'succeeded. Result:', result);
});

const BUCKET_NAME = process.env.GCP_BUCKET_NAME;

const watcher = chalky.watch(watchContext, { 
    ignored: process.env.EXCLUDE_FILE_PATTERN,
    persistent: true,
    depth: 99,
    usePolling: true,
    awaitWriteFinish: true
 });

 const logger = console.log.bind(console);

 watcher
 .on('error', err => logger('error:', err))
 .on('ready', path => {
     logger('Watcher ready:',watchContext);
     const collection = watcher.getWatched();

     inventoryCheckup( collection );

     watcher.on('all', (evt, sourcePath) => {
        const targetPath = sourcePath.replace(BASE_DIR, '');
        console.log('Watch Event: ', evt, sourcePath);
        let jobdata = {
            sourceFileName: sourcePath,
            targetFileName: targetPath,
            targetBucket: BUCKET_NAME
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
            jobdata['event'] = 'remove';
            retries = 2;
         }

         if (jobdata.event !== undefined) {
            createFileJob( jobdata, retries );
         }
     });
 });

 function createFileJob( jobdata, retries ) {
    eq.createJob(jobdata)
    .retries(retries)
    .save()
    .then((job) =>  {
        console.log('Job', job.id, 'created for ', job.data);
        job.on('succeeded', ( result ) => {
            console.log('Job',job.id,'succeeded', result.result, result.file, result.generation);
        });
        job.on('failed', ( err ) => {
            console.log('Job',job.id,'failed. Error:', err.message);
        });
        job.on('retrying', ( err ) => {
            console.log('Job',job.id,'retrying. Error:', err.message);
        });
    });
 }
 
 function inventoryCheckup( collection ) {
    logger('Queue a worker request to fetch remote inventory, then check local');
    eq.createJob({targetBucket: BUCKET_NAME, event: 'inventory', projection: ['name','updated']})
    .save()
    .then(( job ) => {
        console.log('Job', job.id, 'created for ', job.data);
        job.on('succeeded', ( cachedResultKey ) => {
            rclient.get( cachedResultKey, ( err, result ) => {
                if (err) {
                    console.log('ERROR', err);
                } else {
                    const bucket_inv = JSON.parse(result);
                    logger('Number of items in Bucket inventory:', bucket_inv.length);
    
                    const inv = inventory.getInventory(collection, BASE_DIR);
                    logger('Number of items in Local inventory:', inv.length);
    
                    // Test local inventory for new or modified vs. bucket version.
                    inv.forEach( ( localfile ) => {
                        if (localfile.path !== undefined) {
                            const localtime = new Date(localfile.mtime).getTime();
                            const remotefile = bucket_inv.find( ( bucketfile ) => {
                                return bucketfile.name === localfile.path;
                            });
                            if (remotefile === undefined) {
                                console.log('Queue new file:',localfile.path);
                                createFileJob({sourceFileName: localfile.fullpath, targetBucket: BUCKET_NAME, targetFileName: localfile.path, event: 'update'});
                            } else {
                                const remotetime = new Date(remotefile.updated).getTime();
                                const timediff = localtime - remotetime;
                                if (timediff > 0) {
                                    console.log('Queue modified file:',localfile.path);
                                    createFileJob({sourceFileName: localfile.fullpath, targetBucket: BUCKET_NAME, targetFileName: remotefile.name, event: 'update'});
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

