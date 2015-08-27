var dateformat = require('dateformat');

exports.chapterLoader = function(book){

  // define
  var chapter = book.addChapter(1, 'EXTRACT & LOAD', {threads: 5});

  // helpers
  var source       = book.connections.source.connection;
  var destination  = book.connections.destination.connection;
  var queryLimit   = 1000;
  var tableMaxes   = {};

  var extractTable = function(table, callback){
    destination.getMax(table, 'updatedAt', function(error, max){
      if(error){ return callback(error); }
      
      var query = 'SELECT * FROM `' + table + '` ';
      if(max){
        query += ' WHERE `updatedAt` >= "' + dateformat(max, 'yyyy-mm-dd HH:MM:ss') + '"';
      }

      source.getAll(query, queryLimit, function(error, rows, done){
        destination.insertData(table, rows, function(error){
          if(error){ return next(error); }
          done();
        });
      }, callback);
    });
  };

  chapter.addLoader('determine extract queries', function(done){
    source.tables.forEach(function(table){
      chapter.addPage('extract table: ' + table, function(next){
        extractTable(table, next);
      });
    });
    done();
  });

};
