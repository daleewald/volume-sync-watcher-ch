const pino = require('pino');

module.exports = pino({
    //prettyPrint: {translateTime: true, ignore: 'hostname'},
    useLevelLabels: true
}); //, dest);