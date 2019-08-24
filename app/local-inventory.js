const fs = require('fs');

exports.list = function( collection , baseDirFilter ) {
    const paths = Object.keys(collection).filter( ( path ) => {
        const fpath = ((path.endsWith('/')) ? path : path + '/');
        return (fpath.indexOf(baseDirFilter) > -1);
    });

    let inventory = [];
    for (i in paths) {
        const basepath = ((paths[i] + '/' === baseDirFilter) ? '' : [paths[i].split(baseDirFilter)[1],'/'].join(''));
        inventory = inventory.concat(collection[paths[i]].map( path => { 
            let file = {};
            const localpath = [paths[i],'/',path].join('');
            const stat = fs.statSync(localpath);
            if (stat.isFile()) {
                file['path'] = [basepath,path].join('');
                file['fullpath'] = localpath;
                Object.assign(file, stat);
            }
            return file;
        } ));
    }
    return inventory;
}