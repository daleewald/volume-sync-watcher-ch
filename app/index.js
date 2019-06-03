const Queue = require('bee-queue');
const chalky = require('chokidar');

const BASE_DIR = process.env.BASE_DIR;
const watchContext = ((BASE_DIR.endsWith('/') || BASE_DIR.endsWith('\\')) ? BASE_DIR : BASE_DIR + '/') + (process.env.INCLUDE_FILE_PATTERN || '');

console.log('create queue for watcher');
const eq = new Queue('watchEvents', {
    redis: {
        host: 'redis'
    },
    isWorker: false
});

eq.on('ready', () => {
    console.log('watchEvents queue is ready.');
});

eq.on('error', (err) => {
    console.log('Queue error: ', err.message);
});

eq.on('succeeded', (job, result) => {
    console.log('Job', job.id, 'succeeded. Result:', result);
});

// *.swx, *.swp
// /(^|\.\w*.)|\w*~(?!\S)/
// ignored will only match during watch init
const watcher = chalky.watch(watchContext, { 
    ignored: /(^|[\\/\\])\..|(\w*~(?!\S))/,
    persistent: true,
    depth: 99,
    useFsEvents: true,
    awaitWriteFinish: true
 });

 const logger = console.log.bind(console);

 watcher
 .on('error', err => logger('error:', err))
 .on('ready', path => {
     logger('Watcher ready:',BASE_DIR);
     watcher.on('all', (evt, sourcePath) => {
        const targetPath = sourcePath.replace(BASE_DIR, '');
        console.log('Watch Event: ', evt, sourcePath);
         if (evt === 'addDir') {
            eq.createJob({sourceFileName: sourcePath, targetFileName: targetPath, event: 'addDir'})
            .save()
            .then((job) => {
                console.log('Job', job.id, 'created for ', job.data);
                job.on('succeeded', ( result ) => {
                    console.log('Job',job.id,'succeeded. Result:', result);
                });
                job.on('failed', ( err ) => {
                    console.log('Job',job.id,'failed. Error:', err.message);
                });
                job.on('retrying', ( err ) => {
                    console.log('Job',job.id,'retrying. Error:', err.message);
                });
            });
         } else
         if (evt === 'unlinkDir') {
            eq.createJob({sourceFileName: sourcePath, targetFileName: targetPath, event: 'removeDir'})
            .save()
            .then((job) => {
                console.log('Job', job.id, 'created for ', job.data);
                job.on('succeeded', ( result ) => {
                    console.log('Job',job.id,'succeeded. Result:', result);
                });
                job.on('failed', ( err ) => {
                    console.log('Job',job.id,'failed. Error:', err.message);
                });
                job.on('retrying', ( err ) => {
                    console.log('Job',job.id,'retrying. Error:', err.message);
                });
            });
         } else
         if (evt === 'add' || evt === 'change') {
            eq.createJob({sourceFileName: sourcePath, targetFileName: targetPath, event: 'update'})
            .save()
            .then((job) => {
                console.log('Job', job.id, 'created for ', job.data);
                job.on('succeeded', ( result ) => {
                    console.log('Job',job.id,'succeeded. Result:', result);
                });
                job.on('failed', ( err ) => {
                    console.log('Job',job.id,'failed. Error:', err.message);
                });
                job.on('retrying', ( err ) => {
                    console.log('Job',job.id,'retrying. Error:', err.message);
                });
            });
         } else
         if (evt === 'unlink') {
            eq.createJob({sourceFileName: sourcePath, targetFileName: targetPath, event: 'remove'})
            .retries(2)
            .save()
            .then((job) => {
                console.log('Job', job.id, 'created for ', job.data);
                job.on('succeeded', ( result ) => {
                    console.log('Job',job.id,'succeeded. Result:', result);
                });
                job.on('failed', ( err ) => {
                    console.log('Job',job.id,'failed. Error:', err.message);
                });
                job.on('retrying', ( err ) => {
                    console.log('Job',job.id,'retrying. Error:', err.message);
                });
            });
         }
     });
 });