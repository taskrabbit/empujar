var FTP = require('ftp');

var connection = function(name, type, options, book){
  this.name       = name;
  this.type       = type;
  this.options    = options;
  this.book       = book;
  this.connection = null;
};

connection.prototype.connect = function(callback){
  var self = this;
  self.connection = new FTP();
  
  self.connection.on('greeting', function(msg){ self.book.logger.log(msg, 'debug'); });
  self.connection.on('error',    function(error){ callback(error); });

  self.connection.on('ready', function(){
    callback();
  });

  self.connection.connect(self.options);
};

connection.prototype.get = function(file, callback){
  var self = this;
  self.connection.get(file, callback);
};

connection.prototype.listFiles = function(dir, callback){
  var self = this;
  var folders = [];
  var files   = [];

  folders.push(dir);

  // it is importnat to only do one check at a time because the CWD pointer of the connection matters!
  var checkFolders = function(){
    if(folders.length === 0){ callback(null, files); }
    else{
      var folder = folders.pop();
      self.book.log('checking ' + folder + ' for files');
      self.connection.cwd(folder, function(error){
        if(error){ callback(error); }
        else{
          self.connection.list(function(error, list){
            if(error){ callback(error); }
            else{
              var filesFound = 0;
              list.forEach(function(e){
                if(e.type === 'd'){
                  folders.push(folder + e.name);
                }else{
                  filesFound++;
                  var fullFileName = folder + '/' + e.name;
                  files.push(fullFileName);
                }
              });
              self.book.log('    found ' + filesFound + ' files');

              checkFolders();
            }
          });
        }
      });
    }
  };

  checkFolders();

};

exports.connection = connection;
