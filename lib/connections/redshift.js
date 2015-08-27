var pg         = require('pg');
var utf8       = require('utf8');
var dateformat = require('dateformat');
var utils      = require(__dirname + '/../utils.js');

var connection = function(name, type, options, book){
  this.name         = name;
  this.type         = type;
  this.options      = options;
  this.book         = book;
  this.connection   = null;
  this.tables       = [];
};

connection.prototype.connect = function(callback){
  var self = this;

  self.connectionString = 'postgres://';
  self.connectionString += self.options.username + ':' + self.options.password;
  self.connectionString += '@' + self.options.host + ':' + self.options.port;
  self.connectionString += '/' + self.options.database;

  self.showTables(function(error, tables){
    callback(error);
  });
};

connection.prototype.showColumns = function(table, callback){
  var self = this;
  var rowObj = {};
  // "SHOW TABLES"
  var query = 'select column_name, data_type, character_maximum_length from INFORMATION_SCHEMA.COLUMNS where table_name = \'' + table + '\';';

  self.query(query, function(error, rows){
    if(!error && rows && rows.length > 0){
      rows.forEach(function(row){
        rowObj[row.column_name] = { type: row.data_type };
      });
    }

    callback(error, rowObj);
  });
};

connection.prototype.showTables = function(callback){
  var self = this;
  var rows = [];
  self.query('SELECT * FROM pg_catalog.pg_tables', function(error, dbRows){
    if(error){ callback(error); }
    else{
      dbRows.forEach(function(row){
        if(row.schemaname === self.options.schema){
          rows.push(row.tablename);
        }
      });
      rows.sort();
      if(!error){ self.tables = rows; }
      callback(null, rows);
    }
  });
};

connection.prototype.badMappings = function(){
  return {
    'authorization': 'authorization_',
    'tag':           'tag_',
    'system':        'system_',
  };
};

connection.prototype.sanitizeString = function(s){
  var self = this;
  var replaceChar = '?';

  s = utf8.encode(s);

  // redshift byte-code limits
  // http://docs.aws.amazon.com/redshift/latest/dg/multi-byte-character-load-errors.html
  var chars = s.split('');
  s = '';
  for (var i = 0; i < chars.length; i++) {
    if(chars[i].charCodeAt(0) === 254){ s += replaceChar; }
    else if(chars[i].charCodeAt(0) === 255){ s += replaceChar; }
    else if(chars[i].charCodeAt(0) >= 128 && chars[i].charCodeAt(0) <= 191 ){ s += replaceChar; }
    else{ s += chars[i]; }
  }

  s = s.replace(/\\/g, '\\\\');     // re-escape any "\" literalls within the string
  s = s.replace(/\'/g, "\\'");      // escape any single quotes
  s = s.replace(/\0/g, '');         // remove any null chars
  
  // http://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html
  var maxTextLength = 65535; 
  while( utils.utf8ByteLength(s) > maxTextLength ){
    s = s.slice(0, -1);
  }

  s = s.replace(/\\+$/, '');        // remove any "\" at the end of the string (post truncation)

  // check for any string dates that should be "null" in postgres (0000 is not a year!)
  if(s === '0000-00-00 00:00:00'){ s = 'NULL'; }

  return s;
};

connection.prototype.query = function(query, callback){
  var self = this;
  self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + query, 'debug');

  pg.connect(self.connectionString, function(error, client, done){
    if(error){ 
      done(); 
      return callback(error); 
    }else{
      client.query(query, function(error, results){
        done();
        // pg's error handling (and stack tracing) is terrible... at least we should append the query!
        if(error){ 
          error = new Error('PG Error: ' + String(error) + '; query: ' + query);
          return callback(error);
        }else{
          return callback(null, results.rows);
        }        
      });
    }
  });
};

connection.prototype.tableSize = function(table, callback){
  var self = this;
  self.query('SELECT count(1) as "total" from ' + table, function(error, rows){
    if(error){ callback(error); }
    else{ callback(null, rows[0].total); }
  });
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

connection.prototype.insertData = function(table, data, callback){
  var self = this;
  var query  = '';
  var previousQuery = '';
  var keys = [];
  var quotedKeys = [];
  var needsComma = false;
  var rowNeedsComma = false;
  var formattedPart = '';
  var rowsInserting = 0;

  if(!callback){ callback = function(){}; }
  if(data.length === 0){ return callback(); }

  // something guessing about `Maximum Statement Allowed: 16777216 bytes`
  // using the actual byte limit seems to crash the cluster, so divide by a large number for saftey
  // TODO: this can be tweaked later
  var maxInsertByteLength = (16777216 / 32); 

  self.showTables(function(error, tables){
    self.showColumns(table, function(error, columnData){

      var missingColumn = null;
      var missingColumnData = [];
      var columns = Object.keys( columnData );

      var badMappings = self.badMappings();
      var i = 0;
      while(i < data.length){
        for(var key in data[i]){
          if(badMappings[key]){
            data[i][badMappings[key]] = data[i][key];
            delete data[i][key];
            key = badMappings[key];
          }

          if( columns.indexOf( key.toLowerCase() ) < 0 ){
            missingColumn = key;
            if(data[i][key] !== null && data[i][key] !== undefined){ 
              missingColumnData.push( data[i][key] ); 
            }
          }
        }
        i++;
      }

      if(self.tables.indexOf(table) < 0){
        self.buildTableFromData(table, data, function(error){
          if(error){ callback(error); }
          else{ self.insertData(table, data, callback); }
        });

      }else if(missingColumn !== null){
        self.addColumn(table, missingColumn, missingColumnData, function(error){
          if(error){ callback(error); }
          else{ self.insertData(table, data, callback); }
        });

      }else{
        data.forEach(function(row){
          for(var key in row){
            var quotedKey = '"' + key + '"';
            if(quotedKeys.indexOf(quotedKey) < 0){ quotedKeys.push(quotedKey); }
            if(keys.indexOf(key) < 0){ keys.push(key); }
          }
        });

        query += 'INSERT INTO ' + table + ' ( ' + quotedKeys.join(', ') + ' ) ';
        query += ' VALUES \n'; 

        while( 
          ( utils.utf8ByteLength(query) < maxInsertByteLength && data.length > 0 ) || 
          ( data.length > 0 && rowsInserting === 0 )
        ){
          row = data.shift();
          rowsInserting++;
          previousQuery = query;
        
          rowNeedsComma = false;
          if(needsComma){  query += ','; }
          query += ' ( ';
          keys.forEach(function(key){
            if(rowNeedsComma){ query += ','; }
            if(row[key] === undefined || row[key] === null){
              query += 'NULL';
            }else if(row[key] instanceof Date){
              formattedPart = dateformat(row[key], 'yyyy-mm-dd HH:MM:ss');
              if(formattedPart === '0000-00-00 00:00:00'){ formattedPart = 'NULL'; }
              query += '\'' + formattedPart + '\'';
            }else if(typeof row[key] === 'string'){
              sanitizeString = self.sanitizeString(row[key]);
              if(sanitizeString === 'NULL'){
                query += 'NULL';
              }else{
                query += '\'' + sanitizeString + '\'';
              }
            }else{
              query += row[key];
            }
            rowNeedsComma = true;
          });
          query += ' ) \n';
          needsComma = true;

          // ensure that the row we just added isn't *really* long
          if( utils.utf8ByteLength(query) > maxInsertByteLength  ){
            query = previousQuery;
            data.unshift(row);
            rowsInserting--;
            break;
          }
        }

        query += ' ; \n';

        self.book.log('    Loading ' +  rowsInserting + ' rows into `' +  table + '`', 'debug');

        self.query(query, function(error){
          if(error){ callback(error); }
          else if(data.length > 0){ self.insertData(table, data, callback); }
          else{ callback(); }
        });
      }
    });
  });
};

connection.prototype.buildTableFromData = function(table, rows, callback){
  var self = this;
  // we only really care about 3 types of data: varchar(65535), datetime, and bigint
  // any custom column types we should handle in a transformation
  // there are no primary keys in redshift
  // there are no indexes in redshift

  var types = {};
  rows.forEach(function(row){
    for(var key in row){
      if( row[key] !== null && row[key] !== undefined ){
        if(types[key] === 'varchar(65535)' || types[key] === 'datetime' || types[key] === 'bigint' || types[key] === 'boolean' ){
          // noop, keep it
        }else{
          if(typeof row[key] === 'boolean'){
            types[key] = 'boolean';
          }else if(typeof row[key] === 'number'){
            types[key] = 'bigint';
          }else if(row[key] instanceof Date === true){
            types[key] = 'datetime';
          }else{
            // TODO: How to re-expand columns after a write? There's no string vs symbol in JS :(
            // if(row[key].length <= 150){
            //   types[key] = 'varchar(255)';
            // }else{
              types[key] = 'varchar(65535)';
            // }
          }
        }
      }else{
        types[key] = false;
      }
    }
  });

  var query = '';
  var needsComma = false;
  query += ' CREATE TABLE ' + table + ' ( \n';

  for(var key in types){
    if(types[key] === false){ types[key] = 'varchar(65535)'; }
    if(needsComma) query += ', ';
    query += '  "' + key + '" ' + types[key];
    query += ' encode ' + self.compressionMap()[types[key]];
    query += ' \n';
    needsComma = true;
  }

  query += ') \n';

  if(types.id){
    query += ' distkey(id) \n';
  }

  if(types.updated_at){
    query += ' sortkey(updated_at) \n';
  }else if(types.created_at){
    query += ' sortkey(created_at) \n';
  }else if(types.updated_at){
    query += ' sortkey(id) \n';
  }

  query += ';\n';

  self.query(query, function(error, rows){
    if(error){ 
       callback(error); 
    }else{
      self.showTables(function(error, tables){
        callback(error);
      });
    }
  });
};

connection.prototype.compressionMap = function(){
  return {
    'boolean':        '',
    'bigint':         'delta',
    'datetime':       'delta32k',
    'varchar(255)':   'text255',
    'varchar(65535)': 'raw',
  };
};

connection.prototype.mergeTables = function(sourceTable, destinationTable, callback){
  var self = this;
  var queries = [];
  var fullMerge = false;

  self.showTables(function(error, tables){
    self.showColumns(sourceTable, function(error, sourceColumns){
      self.showColumns(destinationTable, function(error, destinationColumns){
        
        var sourceKeys      = Object.keys(sourceColumns);
        var destinationKeys = Object.keys(destinationColumns);

        if(tables.indexOf(sourceTable) < 0){ 
          return callback(new Error('sourceTable does not exist')); 
        }else{
          if(tables.indexOf(destinationTable) < 0){ fullMerge = true; }
          if(sourceKeys.length != destinationKeys.length){ fullMerge = true; }
          if(destinationKeys.indexOf('id') < 0){ fullMerge = true; }
          for(var key in sourceColumns){
            if(destinationKeys.indexOf(key) < 0){ fullMerge = true; }
          }

          queries.push(' BEGIN ');

          // #ensure we de-dup what we just uploaded
          if(sourceKeys.indexOf('id') >= 0 && sourceKeys.indexOf('updated_at') >= 0){
            delteQuery = '';
            delteQuery += 'DELETE FROM ' + sourceTable + ' WHERE id IN (                                             ';
            delteQuery += '  SELECT id FROM (                                                                        ';
            delteQuery += '    SELECT count(id) AS "cnt", id FROM ' + sourceTable + ' GROUP BY id                    ';
            delteQuery += '  ) WHERE cnt > 1                                                                         ';
            delteQuery += ') AND (                                                                                   ';
            delteQuery += '  updated_at != (                                                                         ';
            delteQuery += '    SELECT MAX(updated_at) FROM ' + sourceTable + ' x WHERE x.id = ' + sourceTable + '.id ';
            delteQuery += '  )                                                                                       ';
            delteQuery += ')                                                                                         ';
            
            queries.push(delteQuery);
          }

          if(fullMerge){
            queries.push( 'DROP TABLE IF EXISTS ' + destinationTable );
            queries.push( 'CREATE TABLE ' + destinationTable + ' ( LIKE ' + sourceTable + ' INCLUDING DEFAULTS )' );
            queries.push( 'INSERT INTO ' + destinationTable + ' ( SELECT * FROM ' + sourceTable + ' )' );
          }else{
            queries.push( 'DELETE FROM ' + destinationTable + ' WHERE id IN ( SELECT id FROM ' + sourceTable + ')' );
            queries.push( 'INSERT INTO ' + destinationTable + ' ( SELECT * FROM ' + sourceTable + ' )' );
          }

          queries.push(' COMMIT ');

          self.book.log('  merging `' + sourceTable + '` => `' + destinationTable + '`', 'debug');

          self.query(queries.join(' ; '), callback);
        }
      });
    });
  });
};

connection.prototype.addColumn = function(table, column, rowData, callback){
  var self = this;

  if(rowData.length === 0){
    return callback( new Error('rowData is required to determine column type') );
  }

  var type = 'varchar(65535)';
  rowData.forEach(function(elem){
    if(typeof elem === 'boolean'){
      type = 'boolean';
    }else if(typeof elem === 'number'){
      type = 'bigint';
    }else if(elem instanceof Date === true){
      type = 'datetime';
    }else{
      type = 'varchar(65535)';
    }
  });

  var encoding = self.compressionMap()[type];
  var query = 'ALTER TABLE ' + table + ' ADD COLUMN "' + column + '" ' + type + ' encode ' + encoding + ';';
  
  self.book.log('Add Column ' + table + ', ' + column + ', ' + type + ', ' + encoding);
  self.query(query, callback);
};

connection.prototype.alterColumn = function(table, column, definition, callback){
  var self = this;
  // you cannot alter column in redshift... here's a hack!
  
  var queries = [];
  var new_column = 'tmp_column';
  
  queries.push(' ALTER TABLE ' + table + ' ADD COLUMN ' + new_column + ' (' + definition + ') ');
  queries.push(' UPDATE ' + table + ' SET ' + new_column + ' = ' + column + ' ');
  queries.push(' ALTER TABLE ' + table + ' DROP COLUMN ' + column + ' ');
  queries.push(' ALTER TABLE ' + table + ' RENAME COLUMN ' + new_column + ' TO ' + column + ' ');

  self.book.log('Alter Table ' + table + ', ' + column + ', ' + definition);
  self.query(queries.join(' ; '), callback);
};

connection.prototype.copyTableSchema = function(sourceTable, destinationTable, callback){
  var self = this;

  self.query('CREATE TABLE ' + destinationTable + ' ( LIKE ' + sourceTable + ' INCLUDING DEFAULTS )', callback);
};

connection.prototype.getMax = function(table, column, callback){
  var self = this;
  self.showTables(function(error){
    if(!error && self.tables.indexOf(table) >= 0){
      self.showColumns(table, function(error, rowObj){
        if(rowObj[column]){
          self.query('SELECT MAX(' + column + ') as "max" from ' + table, function(error, rows){
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

exports.connection = connection;