const InventoryQueue = require('./inventory-queue');

//const logger = console.log.bind(console);

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
