const pino = require('pino');

//const dest = pino.destination([process.env.LOG_DIR,'watcher-',process.pid,'.log'].join(''));

module.exports = pino({
    prettyPrint: {translateTime: true, ignore: 'hostname'},
    useLevelLabels: true
}); //, dest);