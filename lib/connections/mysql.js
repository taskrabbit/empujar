var fs       = require('fs');
var mysql    = require('mysql');
var async    = require('async');
var filesize = require('filesize');
var exec     = require('child_process').exec;
var utils    = require(__dirname + '/../utils.js');

var connection = function(name, type, options, book){
  this.name         = name;
  this.type         = type;
  this.options      = options;
  this.book         = book;
  this.connection   = null;
  this.tables       = [];
  this.settings     = {
    max_allowed_packet: undefined,
  };

  if(!this.options.timezone){
    this.options.timezone = 'utc';
  }

  if(!this.options.varCharLength){
    this.options.varCharLength = 255;
  }

  if(!this.options.primaryKey){
    this.options.primaryKey = 'id';
  }

  if(!this.options.dumpLogTimer){
    this.options.dumpLogTimer = 1000 * 30;
  }

  if(!this.options.unknownType){
    // our proxy for an unknown type
    this.options.unknownType = 'varchar(0)';
  }
};

connection.prototype.connect = function(callback){
  var self = this;
  self.connection = mysql.createPool(self.options);

  self.showTables(function(error, tables){
    if(error){ return callback(error); }
    
    var jobs = [];
    Object.keys(self.settings).forEach(function(setting){
      jobs.push(function(next){
        self.getVariable(setting, function(error, value){
          if(!error){
            self.settings[setting] = value;
            self.book.logger.log('   ' + self.name + '/' + self.type + ' >> setting loaded: ' + setting + ' => ' + value, 'debug');
            next();
          }else{
            next(error);
          }
        });
      });
    });

    async.parallel(jobs, callback);
  });
};

connection.prototype.showTables = function(callback){
  var self = this;
  var rows = [];
  self.query('SHOW TABLES', function(error, dbRows){
    if(error){ callback(error); }
    else{
      dbRows.forEach(function(row){
        for(var key in row){
          rows.push(row[key]);
        }
      });
      rows.sort();
      self.tables = rows;
      callback(null, rows);
    }
  });
};

connection.prototype.showColumns = function(table, callback){
  var self = this;
  var rowObj = {};
  var query = 'SHOW COLUMNS FROM `' + table + '`';

  self.query(query, function(error, rows){
    if(!error && rows && rows.length > 0){
      rows.forEach(function(row){
        rowObj[row.Field] = { 'type'   : row.Type.split('(')[0],
                              'null'   : row.Null,
                              'key'    : row.Key,
                              'default': row.Default,
                              'extra'  : row.Extra,
                            };

        var lengthMatch = row.Type.match(/[(](\d+)(?:,(\d+))?[)]/);
        if(lengthMatch){
          rowObj[row.Field].charLength = parseInt(lengthMatch[1]);
          rowObj[row.Field].precision  = parseInt(lengthMatch[1]);
          rowObj[row.Field].scale      = parseInt(lengthMatch[2]);
        }
      });
    }

    callback(error, rowObj);
  });
};

connection.prototype.query = function(query, data, callback, skipTransactions){
  var self = this;
  if(!skipTransactions){ skipTransactions = false; }

  if(typeof data === 'function' && !callback){
    callback = data; data = null;
  }

  if(query instanceof Array){
    var steps = [];

    if(skipTransactions === false){
      steps.push(function(next){
        var runner = self.connection.query(' START TRANSACTION ', next);
        self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.sql, 'debug');
      });
    }

    query.forEach(function(q){
      steps.push(function(next){
        var runner = self.connection.query(q, next);
        self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.sql, 'debug');
      });
    });

    if(skipTransactions === false){
      steps.push(function(next){
        var runner = self.connection.query(' COMMIT ', next);
        self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.sql, 'debug');
      });
    }

    async.series(steps, function(error){
      if(error && skipTransactions === false){
        var runner = self.connection.query(' ROLLBACK ', function(){
          callback(error);
        });
        self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.sql, 'debug');
      }else if(error){
        callback(error);
      }else{
        callback();
      }
    });

  }else{

    var runner = self.connection.query({
      sql: query,
      typeCast: self.typeCast,
      values: data,
    }, function(error, rows, fields){
      if(error){ callback(error); }
      else{ 
        if(rows instanceof Array){
          callback(null, rows, fields); 
        }else{
          // when not doing a SELECT the resulting data is a medatadata hash... 
          // we don't care about most of the data when reporting
          callback(null, {rows: rows.affectedRows}, fields, rows); 
        }
      }
    });

    self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.sql, 'debug');
  }
};

connection.prototype.getVariable = function(variable, callback){
  var self = this;

  self.query('SHOW VARIABLES LIKE ?', variable, function(error, rows){
    if(error){ callback(error); }
    else if(!rows || rows.length === 0){ callback(new Error('Variable not found')); }
    else{
      callback(null, rows[0].Value);
    }
  });
};

connection.prototype.getMax = function(table, column, callback){
  var self = this;
  self.showTables(function(error){
    if(!error && self.tables.indexOf(table) >= 0){
      self.showColumns(table, function(error, rowObj){
        if(rowObj[column]){
          self.query('SELECT MAX(' + column + ') as "max" from `' + table + '`', function(error, rows){
            if(!error && rows && rows[0].max){
              callback(null, rows[0].max);
            }else{
              callback(error);
            }
          });
        }else{
          callback(error);
        }
      });
    }else if(!error){
      callback();
    }else{
      callback(error);
    }
  });
};

connection.prototype.typeCast = function(field, next){
  if (field.type == 'TINY' && field.length == 1) {
    return (field.string() == '1'); // 1 = true, 0 = false
  }
  return next();
};

connection.prototype.queryStream = function(query, callback){
  var self = this;

  self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + query, 'debug');
  return self.connection.query(query);
};

connection.prototype.getAll = function(queryBase, chunkSize, dataCallback, doneCallback, rowsFound){
  var self = this;
  chunkSize = parseInt(chunkSize);
  if(!rowsFound){ rowsFound = 0; }

  var query = '' + queryBase;
  query += " LIMIT " + rowsFound + "," + chunkSize;

  self.query(query, function(error, rows){
    if(error){ 
      return doneCallback(error, rowsFound);
    }else if(rows.length === 0){
      return doneCallback(null, rowsFound);
    }else{
      rowsFound = rowsFound + rows.length;
      dataCallback(null, rows, function(){
        if(self.book.options.getAllLimit > rowsFound){
          self.getAll(queryBase, chunkSize, dataCallback, doneCallback, rowsFound);
        }else{
          doneCallback(null, rowsFound);
        }
      });
    }
  });
};

connection.prototype.insertData = function(table, data, callback, mergeOnDuplicates){
  var self = this;
  var query = '';
  var max_insert_length = (self.settings.max_allowed_packet) * (3/4);
  var thisData = [];
  var keys = [];
  var insertArray = [];
  var row;
  var needsComma = false;
  var updatableColumns = [];
  var insertLength = 0;

  if(!callback){ callback = function(){}; }
  if(mergeOnDuplicates == null){ mergeOnDuplicates = true; }
  if(data.length === 0){ return callback(); }

  self.showTables(function(error, tables){
    self.showColumns(table, function(error, columnData){

      for(var col in columnData){
        if(columnData[col].type === self.options.unknownType){ updatableColumns.push(col); }
      }

      if(tables.indexOf(table) < 0){
        
        self.buildTableFromData(table, data, function(error){
          if(error){ callback(error); }
          else{ self.insertData(table, data, callback); }
        });

      }else{

        while(insertLength < max_insert_length && data.length > 0){
          row = data.shift();
          Object.keys(row).forEach(function(key){
            if(keys.indexOf(key) < 0){ keys.push(key); }
            insertLength += String( row[key] ).length; // TODO: UTF8 safe the string lengths
          });
          thisData.push( row );
        }

        var missingKey = false;
        keys.forEach(function(key){
          if(!columnData[key]){
            missingKey = key;
          }
        });

        if(missingKey){

          var rowData = [];
          data = thisData.concat(data);
          data.forEach(function(row){
            rowData.push( row[missingKey] );
          });
          self.addColumn(table, missingKey, rowData, function(error){
            if(error){ callback(error); }
            else{ self.insertData(table, data, callback); }
          });

        }else{
          var jobs = [];
          var updatableColumnData = {};
          var textColumnsToExpand = [];

          updatableColumns.forEach(function(key){ updatableColumnData[key] = []; });

          thisData.forEach(function(row){
            var thisData = [];
            keys.forEach(function(key){
              thisData.push( row[key] );
              
              if(row[key] !== null && row[key] !== undefined){
                if(updatableColumns.indexOf(key) >= 0 && row[key] !== undefined){
                  updatableColumnData[key].push(row[key]);
                }

                // TODO: UTF8 safe the string lengths
                if(columnData[key].type.match(/varchar/) && columnData[key].charLength && row[key].length > columnData[key].charLength){
                  if(textColumnsToExpand.indexOf(key) < 0){ textColumnsToExpand.push(key); }
                }
              }
            });
            insertArray.push( thisData );
          });

          if(textColumnsToExpand.length > 0){
            textColumnsToExpand.forEach(function(column){
              jobs.push(function(next){
                self.alterColumn(table, column, 'TEXT', next);
              });
            });
          }

          updatableColumns.forEach(function(key){
            if(updatableColumnData[key].length > 0){
              jobs.push(function(next){
                self.addColumn(table, key, updatableColumnData[key], next);
              });
            }
          });

          jobs.push(function(next){
            query += ' INSERT INTO `' + table + '` ( ';
            keys.forEach(function(key){
              if(needsComma){ query += ','; }
              query += ' `' + key + '` ';
              needsComma = true;
            });
            query += ' ) VALUES ? ';

            if(mergeOnDuplicates === true){
              query += ' ON DUPLICATE KEY UPDATE ';
              needsComma = false;
              keys.forEach(function(key){
                if(needsComma){ query += ' , '; }
                query += ' `' + key + '`=VALUES(`' + key + '`) ';
                needsComma = true;
              });
            }

            self.book.log('    Loading ' +  insertArray.length + ' rows into `' +  table + '`', 'debug');

            self.query(query, [insertArray], next);
          });

          async.series(jobs, function(error){
            if(error){ callback(error); }
            else if(data.length > 0){ self.insertData(table, data, callback); }
            else{ callback(); }
          });
        }
      }
    });
  });
};

connection.prototype.buildTableFromData = function(table, data, callback){
  var self       = this;
  var query      = '';
  var types      = {};
  var needsComma = false;

  if(!data || data.length === 0){
    return callback(new Error('No data provided to create table `' + table + '`'));
  }

  data.forEach(function(row){
    for(var key in row){
      if( row[key] !== null && row[key] !== undefined ){
        
        if(types[key] === 'varchar(' + self.options.varCharLength + ')' && row[key].length > self.options.varCharLength){
          types[key] = 'text';
        }
        else if(types[key] === 'bigint(20)' && utils.isFloat(row[key]) ){
          types[key] = 'float';
        }

        else if(types[key] === self.options.unknownType || !types[key] ){
          if(typeof row[key] === 'boolean'){
            types[key] = 'tinyint(1)';
          }else if(typeof row[key] === 'number'){
            if( utils.isInt(row[key]) ){
              types[key] = 'bigint(20)';
            }else{
              types[key] = 'float';
            }
          }else if(row[key] instanceof Date === true){
            types[key] = 'datetime';
          }else{
            if(row[key].length <= self.options.varCharLength){
              types[key] = 'varchar(' + self.options.varCharLength + ')';
            }else{
              types[key] = 'text';
            }
          }
        }

        if(key === self.options.primaryKey && types[key] != self.options.unknownType && types[key].indexOf('PRIMARY') < 0){
          if(typeof row[key] === 'number'){
            types[key] += ' PRIMARY KEY NOT NULL AUTO_INCREMENT';
          }else{
            types[key] += ' PRIMARY KEY';
          }
        }
      }else{
        types[key] = self.options.unknownType; 
      }
    }
  });

  var sortedKeys = Object.keys(types);
  [ self.options.primaryKey ].reverse().forEach(function(e){
    if(sortedKeys.indexOf(e) >= 0){
      sortedKeys.splice(sortedKeys.indexOf(e), 1);
      sortedKeys.unshift(e);
    }
  });

  query += ' CREATE TABLE `' + table + '` ( \n';

  sortedKeys.forEach(function(key){
    if(needsComma) query += ', ';
    query += '  `' + key + '` ' + types[key];
    query += ' \n';
    needsComma = true;
  });

  query += ' ) \n';

  self.query(query, callback);
};

connection.prototype.addColumn = function(table, column, rowData, callback){
  var self = this;
  var type = self.options.unknownType;

  if(!rowData || rowData.length === 0){
    return callback(new Error('No rowData provided to alter table `' + table + '`.`' + column + '`'));
  }

  rowData.forEach(function(item){
    if(item !== undefined && item !== null){

      if(type === 'varchar(' + self.options.varCharLength + ')' && item.length > self.options.varCharLength){
        type = 'text';
      }

      if(type === 'bigint(20)' && utils.isFloat(item) ){
        type = 'float';
      }

      if(type === self.options.unknownType ){
        if(typeof item === 'boolean'){
          type = 'tinyint(1)';
        }else if(typeof item === 'number'){
          if( utils.isInt(item) ){
            type = 'bigint(20)';
          }else{
            type = 'float';
          }
        }else if(item instanceof Date === true){
          type = 'datetime';
        }else{
          if(item.length <= self.options.varCharLength){
            type = 'varchar(' + self.options.varCharLength + ')';
          }else{
            type = 'text';
          }
        }
      }
    }
  });

  if(column === self.options.primaryKey){
    if(type === 'bigint(20)'){
      type += ' PRIMARY KEY NOT NULL AUTO_INCREMENT';
    }else{
      type += ' PRIMARY KEY';
    }
  }

  self.showColumns(table, function(error, columnData){
    var query;
    if(error){ return callback(error); }
    else if(columnData[column]){
      query = 'ALTER TABLE `' + table + '` CHANGE `' + column + '` `' + column + '` ' + type;
    }else{
      query = 'ALTER TABLE `' + table + '` ADD `' + column + '` ' + type;
    }
    self.query(query, callback);
  });
};

connection.prototype.alterColumn = function(table, column, definition, callback){
  var self = this;
  
  var queries = [];
  var new_column = 'tmp_column';
  
  queries.push(' ALTER TABLE ' + table + ' ADD ' + new_column + ' ' + definition + '  NULL ');
  queries.push(' UPDATE ' + table + ' SET ' + new_column + ' = ' + column + ' ');
  queries.push(' ALTER TABLE ' + table + ' DROP COLUMN ' + column + ' ');
  queries.push(' ALTER TABLE ' + table + ' CHANGE ' + new_column + ' ' + column + ' ' + definition + '; ');

  self.book.log('Alter Table ' + table + ', ' + column + ', ' + definition);
  self.query(queries, null, callback, true);
};

connection.prototype.mergeTables = function(sourceTable, destinationTable, callback){
  var self = this;
  var queries = [];
  var fullMerge = false;
  var needsComma = false;

  self.showTables(function(error, tables){
    self.showColumns(sourceTable, function(error, sourceColumns){
      self.showColumns(destinationTable, function(error, destinationColumns){
        
        var sourceKeys      = Object.keys(sourceColumns);
        var destinationKeys = Object.keys(destinationColumns);

        if(tables.indexOf(sourceTable) < 0){ 
          return callback(new Error('sourceTable does not exist: ' + sourceTable)); 
        }else{
          if(tables.indexOf(destinationTable) < 0){ fullMerge = true; }
          if(sourceKeys.length > destinationKeys.length){ fullMerge = true; }
          if(destinationKeys.indexOf(self.options.primaryKey) < 0){ fullMerge = true; }

          if(fullMerge){
            queries.push( 'DROP TABLE IF EXISTS `' + destinationTable + '`');
            queries.push( 'CREATE TABLE `' + destinationTable + '` LIKE `' + sourceTable + '`' );
            queries.push( 'INSERT INTO `' + destinationTable + '` ( SELECT * FROM `' + sourceTable + '` )' );
          }else{
            var mergeQuery = '';
            mergeQuery += ' INSERT INTO `' + destinationTable + '` ( SELECT * FROM `' + sourceTable + '` ) ';
            mergeQuery += ' ON DUPLICATE KEY UPDATE ';

            for(var key in destinationColumns){
              if(needsComma){ mergeQuery += ' , '; }
              mergeQuery += ' `' + key + '`=VALUES(`' + key + '`) ';
              needsComma = true;

              if(sourceColumns[key] && !destinationColumns[key]){
                // new column added to source table
                queries.push('ALTER TABLE `' + destinationTable + '` ADD `' + key + '` ' + sourceColumns[key]);
              }
              else if( destinationColumns[key] === self.options.unknownType && sourceColumns[key] !== self.options.unknownType ){
                // source table was able to figure out data type and destination needs to be updated
                queries.push('ALTER TABLE `' + destinationTable + '` CHANGE `' + key + '` `' + key + '` ' + sourceColumns[key]);
              }
            }

            queries.push(mergeQuery);
          }
          self.book.log('  merging `' + sourceTable + '` => `' + destinationTable + '`', 'debug');
          self.query(queries, null, callback, true);
        }
      });
    });
  });
};

connection.prototype.copyTableSchema = function(sourceTable, destinationTable, callback){
  var self = this;

  self.query('CREATE TABLE `' + destinationTable + '` LIKE `' + sourceTable + '`', callback);
};

connection.prototype.dump = function(file, options, callback){
  var self = this;
  var timer;
  var command = '';

  if(!options.binary){   options.binary = 'mysqldump';    }
  if(!options.database){ options.database = self.options.database; }
  if(!options.password){ options.password = self.options.password; }
  if(!options.host){     options.host = self.options.host;         }
  if(!options.port){     options.port = self.options.port;         }
  if(!options.user){     options.user = self.options.user;         }
  if(!options.tables){   options.tables = [];                      }
  if(!options.gzip){     options.gzip = false;                     }

  command += ' ' + options.binary + ' ';
  command += ' -h ' + options.host + ' ';
  command += ' -u ' + options.user + ' ';
  if(options.password){ command += ' -p' + options.password + ' '; }
  if(options.port){ command += ' --port ' + options.port + ' '; }
  if(options.options){ command += ' ' + options.options + ' '; }
  command += '  ' + options.database + ' ';

  options.tables.forEach(function(table){
    command += ' ' + table + ' ';
  });

  if(options.gzip === true){
    command += ' | gzip > ' + file;
  }else{
    command += ' > ' + file;
  }

  self.book.logger.log(command, 'debug');

  exec(command, function(error, stdout, stderr){
    clearTimeout(timer);
    if(!error && (stderr && stderr !== '')){
      if( !stderr.match(/a password on the command line interface can be insecure/) ){
        error = new Error(stderr);
      }
    }

    callback(error, stdout);
  });

  timer = setInterval(function(){
    if(fs.existsSync(file)){
      var stats = fs.statSync(file);
      self.book.logger.log('dumped to ' + file + ': ' + filesize(stats.size), 'info');
    }else{
      self.book.logger.log('cannot find dumpfile: ' + file, 'error');
    }
  }, self.options.dumpLogTimer);
};

exports.connection = connection;
