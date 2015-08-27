var MixpanelExport = require('mixpanel-data-export');

var connection = function(name, type, options, book){
  this.name       = name;
  this.type       = type;
  this.options    = options;
  this.book       = book;
  this.connection = null;
};

connection.prototype.connect = function(callback){
  var self = this;
  self.connection = new MixpanelExport({
    api_key: self.options.apiKey,
    api_secret: self.options.apiSecret
  });
  callback();
};

connection.prototype.getAll = function(method, where, dataCallback, doneCallback, sessionId, page, rowsFound){
  var self = this;
  if(page === undefined){ page = 0; }
  if(!rowsFound){ rowsFound = 0; }

  var payload = {};
  if(where){     payload.where = where;          }
  if(page){      payload.page = page;            }
  if(sessionId){ payload.session_id = sessionId; }

  self.book.logger.log('   ' + self.name + '/' + self.type + ' >> [' + method + ']' + JSON.stringify(payload), 'debug');

  self.connection[method](payload, function(error, data){

    // who changes the order of response params?!
    if(data === undefined && error.status === 'ok'){
      data = error; error = null;
    }

    if(error){
      doneCallback(error);
    }else{
      sessionId = data.session_id;
      var results = [];
      data.results.forEach(function(elem){
        results.push( elem.$properties );
      });

      if(results.length > 0){
        rowsFound = rowsFound + results.length;
        
        dataCallback(null, results, function(){
          if(self.book.options.getAllLimit > rowsFound){
            self.getAll(method, where, dataCallback, doneCallback, sessionId, (page + 1), rowsFound);
          }else{
            doneCallback(null, rowsFound);
          }
        });
      }else{
        doneCallback(null, rowsFound);
      }
    }

  });
};

exports.connection = connection;