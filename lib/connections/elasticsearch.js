var dateformat    = require('dateformat');
var async         = require('async');
var elasticsearch = require('elasticsearch');

var connection = function(name, type, options, book){
  this.name       = name;
  this.type       = type;
  this.options    = options;
  this.book       = book;
  this.connection = null;

  if(!this.options.writeLimit){
    this.options.writeLimit = 20;
  }
};

connection.prototype.connect = function(callback){
  var self = this;
  self.connection = new elasticsearch.Client(self.options);
  self.connection.ping({hello: "elasticsearch!"}, function(error){
    callback(error);
  });
};

connection.prototype.insertData = function(index, data, callback, writeMethod){
  var self = this;
  var jobs = [];
  if(!writeMethod){ writeMethod = 'update'; }
  if(!data || data.length === 0){ return callback(); }

  data.forEach(function(d){
    jobs.push(function(done){
      self.connection[writeMethod]({
        index: index,
        type: index,
        body: { doc: d }
      }, done);
    });
  });

  async.parallelLimit(jobs, self.options.writeLimit, callback);
};

connection.prototype.getAll = function(index, query, fields, chunkSize, dataCallback, doneCallback, rowsFound){
  var self = this;
  var scrollTime = '5m';
  if(!rowsFound){ rowsFound = 0; }
  
  var payload = {
    index: index,
    scroll: scrollTime,
    size: chunkSize,
    body: query
  };

  if(fields !== '*' && fields.length > 0){
    payload.fields = fields;
  }

  self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + JSON.stringify(payload), 'debug');

  self.connection.search(payload, function getMoreUntilDone(error, response) {
    if(error){ 
      return doneCallback(error, rowsFound);
    }else{
      rowsFound += response.hits.hits.length;

      var simpleData = [];
      response.hits.hits.forEach(function(hit){ 
        var row = {};

        if(hit.fields){
          for(var key in hit.fields){
            row[key] = hit.fields[key][0];
          }
          fields.forEach(function(f){
            if(row[f] === undefined){ row[f] = null; }
          });
        }else{
          row = hit._source;
        }

        simpleData.push( row ); 
      });

      dataCallback(null, simpleData, function(){
        if(rowsFound < response.hits.total && self.book.options.getAllLimit > rowsFound){
          self.connection.scroll({
            scrollId: response._scroll_id,
            scroll: scrollTime
          }, getMoreUntilDone);
        }else{
          doneCallback(null, rowsFound);
        }
      });
    }
  });
};

exports.connection = connection;