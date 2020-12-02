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
  self.connection.ping({requestTimeout: 30000}, function(error){
    callback(error);
  });
};

connection.prototype.showIndices = function(callback){
  var self = this;
  self.connection.indices.get({index: '*'}, callback);
};

connection.prototype.insertData = function(index, data, callback){
  var self = this;
  var jobs = [];
  var writeMethod = 'update';
  if(!data || data.length === 0){ return callback(); }

  data.forEach(function(d){
    jobs.push(function(done){
      self.connection[writeMethod]({
        index: index,
        type: index,
        id: d.id,
        body: { doc: d }
      }, function(error){
        if(error){
          if(error.message.match(/DocumentMissingException/) || error.message.match(/document_missing_exception/)){
            writeMethod = 'create';

            self.connection[writeMethod]({
              index: index,
              type: index,
              id: d.id,
              body: d
            }, done);

          }else{
            done(error);
          }
        }else{
          done();
        }
      });
    });
  });

  async.parallelLimit(jobs, self.options.writeLimit, callback);
};

connection.prototype.getAll = function(index, query, fields, chunkSize, dataCallback, doneCallback, rowsFound){
  var self = this;
  var scrollTime = '8m';
  if(!rowsFound){ rowsFound = 0; }

  var payload = {
    index: index,
    scroll: scrollTime,
    size: chunkSize,
    body: query
  };

  if(fields !== '*' && fields.length > 0){
    payload._source = fields;
  }

  self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + JSON.stringify(payload), 'debug');

  self.connection.search(payload, function getMoreUntilDone(error, response){
    if(error){
      return doneCallback(error, rowsFound);
    }else if(response.hits.hits.length === 0){
      doneCallback(null, rowsFound);
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

        for(var k in row){
          // '2015-11-26T01:33:05.951Z'
          if(typeof row[k] === 'string' && row[k].match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d*Z$/)){
            row[k] = new Date(row[k]);
          }
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
