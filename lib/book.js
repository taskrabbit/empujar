var logger       = require(__dirname + '/logger.js').logger;
var chapter      = require(__dirname + '/chapter.js').chapter;
var utils        = require(__dirname + '/utils.js');
var EventEmitter = require('events').EventEmitter;
var util         = require("util");
var isrunning    = require('is-running');
var glob         = require('glob');
var fs           = require('fs');
var mkdirp       = require('mkdirp');
var async        = require('async');
var path         = require('path');

var book = function(options){  
  this.options = utils.hashMerge(options, this.defaults());

  if(typeof this.options.chapters === 'string'){ 
    this.options.chapters = this.options.chapters.split(',');
  }
  if(typeof this.options.chapters === 'number'){ 
    this.options.chapters = [this.options.chapters];
  }
  if(typeof this.options.chapters === 'boolean'){ 
    this.options.chapters = [];
  }
  for(var i in this.options.chapters){
    if(typeof this.options.chapters[i] === 'string' && this.options.chapters[i].indexOf('-') > 0){
      var parts = this.options.chapters[i].split('-');
      var j   = parseInt(parts[0]);
      this.options.chapters.push(j);
      var end = parseInt(parts[1]);
      while(j <= end){
        this.options.chapters.push(j);
        j++;
      }
    }else{
      this.options.chapters[i] = parseInt(this.options.chapters[i]);
    }
  }

  this.data           = {}; // location for users to store and sync data
  this.chapters       = [];
  this.connections    = {};
  this.logger         = new logger(
    this.options.logStdout, 
    this.options.logPath + '/' + this.options.logFile, 
    this.options.logLevel
  );

  mkdirp.sync(this.options.configPath);
  mkdirp.sync(this.options.pidsPath);
  mkdirp.sync(this.options.logPath);
  mkdirp.sync(this.options.tmpPath);

  this.ensurePid();

  utils.cleanDir(this.options.tmpPath);
  mkdirp.sync(this.options.tmpPath);
};

util.inherits(book, EventEmitter);

book.prototype.defaults = function(){
  var parts = process.cwd().split('/');

  return {
    name:         parts[(parts.length - 1)],
    chapterFiles: path.normalize( process.cwd() + '/chapters/**/*.js' ),
    configPath:   path.normalize( process.cwd() + '/config' ),
    logPath:      path.normalize( process.cwd() + '/log' ),
    pidsPath:     path.normalize( process.cwd() + '/pids' ),
    logFile:      'empujar.log',
    tmpPath:      path.normalize( process.cwd() + '/tmp' ),
    logStdout:    true,
    logLevel:     'info',
    chapters:     [],
    getAllLimit:  Infinity,
  };
};

book.prototype.ensurePid = function(){
  var self    = this;
  var pidFile = self.options.pidsPath + '/' + 'pidfile';
  var ok = true;

  if(fs.existsSync(pidFile)){
    var oldpid = fs.readFileSync(pidFile);
    if( isrunning(parseInt(oldpid)) ){
      ok = false;
      var message = 'empujar already running (pid ' + String(oldpid) + ')';
      self.log(message, 'alert' );
      throw new Error(message);
    }
  }

  if(ok){
    fs.writeFileSync(pidFile, process.pid);
  }
};

book.prototype.removePid = function(){
  var self    = this;
  var pidFile = self.options.pidsPath + '/' + 'pidfile';

  fs.unlinkSync(pidFile);
};

book.prototype.log = function(msg, severity, data){
  var self = this;
  self.logger.log(msg, severity, data);
};

book.prototype.connect = function(callback){
  var self = this;

  var files = glob.sync(self.options.configPath + '/connections/*.js');
  files.forEach(function(file){
    if(file.indexOf('.example.') < 0){
      var paths = file.split('/');
      var parts = paths[(paths.length - 1)].split('.');
      var name = parts[(parts.length - 2)];
      var options = require(file);

      self.connections[name] = {
        name:       name,
        options:    options,
        type:       options.type,
        connection: self.buildConnection(name, options.type, options.options),
      };
    }
  });

  var jobs = []; 
  for(var name in self.connections){
    (function(name){
      jobs.push(function(next){
        var connection = self.connections[name];
        if(connection.connection){
          self.log('Connecting to: `' + name + '` (' + connection.options.type + ')');
          connection.connection.connect(function(error){
            if(error){ 
              var e = new Error('Cannot connect to: `' + connection.name + '` (' + connection.options.type + '): ' + String(error));
              self.logger.log('quitting Empujar due to error', 'emerg');
              self.logger.log(e, 'emerg');
              self.emit('error', e, String(error));
              next(e);
            }
            else{ 
              var successMessage = '    `' + name + '` OK';
              if(connection.connection.tables){
                successMessage += ' (' + connection.connection.tables.length + ' tables)';
              }
              self.log(successMessage);
              next();
            }
          });
        }else{
          next();
        }
      });
    })(name);
  }

  async.series(jobs, function(error){
    if(!error){ callback(); }
  });
};

book.prototype.buildConnection = function(name, type, options){
  var self = this;
  try{
    var localPath = process.cwd() + '/connections/' + type + '.js';
    var libPath   = __dirname + '/connections/' + type + '.js';
    var constructor;

    if( fs.existsSync(localPath) ){
      constructor = require(localPath).connection;
    }else{
      constructor = require(libPath).connection;
    }
     
    var connection = new constructor(name, type, options, self);
    return connection;
  }catch(e){
    self.log('unknown connection type: ' + type + '; skipping connection: ' + name, 'error');
    // throw e;
  }
};

book.prototype.loadChapters = function(){
  var self = this;
  var files = glob.sync(self.options.chapterFiles);
  files.forEach(function(file){
    var fileObject = require(file);
    for(var key in fileObject){
      var loader = fileObject[key];
      loader(self);
    }
  });
};

book.prototype.addChapter = function(priority, name, options){
  var self = this;
  var ch = new chapter(priority, name, options, self);
  self.chapters.push(ch);
  self.sortChapters();
  return ch;
};

book.prototype.sortChapters = function(){
  var self = this;
  self.chapters.sort(function(a,b){
    if(a.priority > b.priority){
      return 1;
    }else if(a.priority < b.priority){
      return -1;
    }else{
      return 0;
    }
  });
};

book.prototype.run = function(callback){
  var self      = this;
  var jobs      = [];
  var bookStart = new Date().getTime();

  self.logger.emphatically('Starting Empujar', 'alert');

  self.emit('state', {
    state: 'book:start',
    time: new Date(),
    name: self.options.name,
    data: JSON.stringify({
      cwd: process.cwd(),
    })
  });

  self.chapters.forEach(function(ch){
    jobs.push(function(next){
      if(
          self.options.chapters.length === 0 ||
          self.options.chapters.indexOf(ch.priority) >= 0
      ){
        var start = new Date().getTime();
        self.logger.emphatically('Starting Chapter [' + ch.priority + ']: ' + ch.name + ' (' + ch.options.threads + ' threads)');
        
        self.emit('state', {
          state: 'chapter:start',
          time: new Date(),
          name: ch.name,
          data: JSON.stringify({
            priority: ch.priority,
            threads: ch.options.threads,
          })
        });

        ch.run(function(error, failedChapter){
          var delta = Math.round(new Date().getTime() - start) / 1000;
          if(!error){
            self.logger.emphatically('Chapter Complete [' + ch.priority + ']: ' + ch.name + ' (' + delta + 's)');
            
            self.emit('state', {
              state: 'chapter:end',
              time: new Date(),
              name: ch.name,
              duration: delta,
              data: JSON.stringify({
                priority: ch.priority,
                threads: ch.options.threads,
              })
            });

          }
          if(error){ 
            next(error, {
              chapter: {
                name: ch.name,
                options: ch.options,
                priority: ch.priority,
              }, 
              page: failedChapter.name
            }); 
          }
          else{ next(); }
        });
      }else{
        next();
      }
    });
  });

  async.series(jobs, function(error, data){
    if(!error){
      var bookDelta = Math.round(new Date().getTime() - bookStart) / 1000;
      self.logger.emphatically('Empujar Complete (' + bookDelta + 's)', 'alert');

      self.emit('state', {
        state: 'book:end',
        time:  new Date(),
        name:  self.options.name,
        duration: bookDelta,
        data: JSON.stringify({
          cwd: process.cwd(),
        })
      });

      self.removePid();
      callback();
    }else{
      data = utils.extractFromArray(data);
      self.logger.log('quitting Empujar due to error', 'emerg');
      self.logger.log('context:', 'emerg', utils.objectFlatten(data));
      self.logger.log(error, 'emerg');

      self.emit('state', {
        state: 'book:error',
        time: new Date(),
        name: String(error),
        data: JSON.stringify({
          error: String(error),
          context: utils.objectFlatten(data),
        })
      });

      self.emit('error', error, data);
    }
  });
};

exports.book = book;
