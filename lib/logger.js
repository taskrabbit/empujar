var winston = require('winston');

var logger = function(stdout, file, level){
  var transports = [];
  if(!level){ level = 'info'; }
  
  if(stdout){
    transports.push( new (winston.transports.Console)({
      colorize:  true,
      timestamp: true,
      level: level,
    }) );
  }
  if(file){
    transports.push( new (winston.transports.File)({ 
      filename: file,
      level: level,
      json: true,
    }));
  }

  this.logger = new (winston.Logger)({
    transports: transports,
    levels: {
      emerg: 8,
      alert: 7,
      crit: 6,
      error: 5,
      warning: 4,
      notice: 3,
      info: 2,
      debug: 1,
      trace: 0
    },
    colors: {
      trace: 'magenta',
      input: 'grey',
      verbose: 'cyan',
      prompt: 'grey',
      debug: 'blue',
      info: 'green',
      data: 'grey',
      help: 'cyan',
      warn: 'yellow',
      error: 'red'
    },
  });
};

logger.prototype.log = function(message, severity, data){
  if(!severity){ severity = 'info'; }
  if(data !== null && data !== undefined){
    this.logger.log(severity, message, data);
  }else{
    this.logger.log(severity, message);
  }
};

logger.prototype.emphatically = function(message, severity){
  var stars = '';
  var i = 0;
  while(i < ( message.length + 6)){
    stars += '*';
    i++;
  }

  this.log('', severity);
  this.log(stars, severity);
  this.log('** ' + message + ' **', severity);
  this.log(stars, severity);
  this.log('', severity);
};

exports.logger = logger;