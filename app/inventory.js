const fs = require('fs');

exports.getInventory = function( collection , BASE_DIR_filter ) {
    const paths = Object.keys(collection).filter( ( path ) => {
        const fpath = ((path.endsWith('/')) ? path : path + '/');
        return (fpath.indexOf(BASE_DIR_filter) > -1);
    });

    let inventory = [];
    for (i in paths) {
        const basepath = ((paths[i] + '/' === BASE_DIR_filter) ? '' : [paths[i].split(BASE_DIR_filter)[1],'/'].join(''));
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