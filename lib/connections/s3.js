var AWS = require('aws-sdk');

var connection = function(name, type, options, book){
  this.name       = name;
  this.type       = type;
  this.options    = options;
  this.book       = book;
  this.connection = null;
  this.s3Stream   = null;
};

connection.prototype.connect = function(callback){
  var self = this;

  AWS.config.update({
    accessKeyId:     self.options.key, 
    secretAccessKey: self.options.token,
    region:          self.options.region,
  });
  self.connection = new AWS.S3();
  self.s3Stream   = require('s3-upload-stream')(self.connection);

  callback();
};

connection.prototype.cleanKey = function(key){
  if(key.charAt(0) === '/'){ key = key.substr(1); }
  return key;
};

connection.prototype.listFolders = function(prefix, callback){
  var self = this;
  var folders = [];

  self.listObjects(prefix, function(error, objects){
    if(error){ callback(error); }
    else{
      objects.forEach(function(object){
        if( object.indexOf(prefix) === 0 ){
          var parts = object.split('/');
          parts.pop();
          folders.push( parts.join() );
        }
      });

      folders.sort();
      callback(null, folders);
    }
  });
};

connection.prototype.listObjects = function(prefix, callback, marker, lastData){
  var self = this;
  var allObjects = [];
  if(lastData){ allObjects = lastData; }
  if(prefix === null || prefix === undefined){ prefix = ''; }

  self.connection.listObjects({
    Bucket: self.options.bucket,
    Marker: marker,
    Prefix: prefix,
  }, function(error, data){
    if(error){
      callback(error, allObjects);
    }else{
      data.Contents.forEach(function(object){
        allObjects.push(object.Key);
      });

      if(data.IsTruncated){
        marker = data.Contents.slice(-1)[0].Key;
        self.listObjects(callback, marker, allObjects);
      }else{
        callback(error, allObjects);
      }
    }
  });
};

connection.prototype.deleteFolder = function(prefix, callback){
  var self = this;
  self.listObjects(prefix, function(error, objects){
    var started = 0;
    objects.forEach(function(object){
      started++;
      self.delete(object, function(error){
        if(error){ callback(error); }
        else{
          started--;
          if(started === 0){
            callback();
          }
        }
      });
    });
  });
};

connection.prototype.objectExists = function(filename, callback){
  var self = this;
  var key = self.cleanKey(filename);

  self.connection.headObject({
    Key: key,
    Bucket: self.options.bucket,
  }, function(error, metadata){
    if(error && error.code === 'Not Found'){
      callback(null, false);
    }else if(error){
      callback(error, false);
    }else{
      callback(null, true);
    }
  }); 
};

connection.prototype.delete = function(filename, callback){
  var self = this;
  var key = self.cleanKey(filename);

  self.connection.deleteObject({
    Key: key,
    Bucket: self.options.bucket,
  }, callback);
};

connection.prototype.streamingUpload = function(inputStream, filename, callback){
  var self = this;
  var key = self.cleanKey(filename);

  var upload = self.s3Stream.upload({
    Bucket: self.options.bucket,
    Key: key,
  });

  upload.on('error', callback);
  upload.on('uploaded', function(details){
    callback();
  });

  inputStream.pipe(upload);
};

exports.connection = connection;
