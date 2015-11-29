# Empujar. Empujarlo Bueno.
When you need to push data around, you push it. Push it real good.  
An ETL and Operations tool. 

[![Build Status](https://travis-ci.org/taskrabbit/empujar.svg?branch=master)](https://travis-ci.org/taskrabbit/empujar)

![https://raw.githubusercontent.com/taskrabbit/empujar/master/empujar.png](https://raw.githubusercontent.com/taskrabbit/empujar/master/empujar.png)

## What

Empujar is a tool which moves stuff around.  It's built in node.js so you can do lots of stuff async-ly.  You can move data around (a ETL tool), files (a backup tool), and more!  

Empujar's top level object is a "book", which contains "chapters" and then "pages".  Chapters are excecuted 1-by-1 in order, and then each page in a chapter can be run in parallel (up to a *threading* limit you specify).

```javascript
#!/usr/bin/env node

process.chdir(__dirname);

var Empujar    = require('empujar');
var optimist   = require('optimist'); 
var options    = optimist.argv; // get command line opts, like `--logLevel debug` or `--chapters 100`

var book = new Empujar.book(options);

// you can define custom error behavior when a page callback retruns an error
var errorHandler = function(error, context){
  console.log("OH NO! (but I handled the error) | " + error);
  setTimeout(process.exit, 5000);
};

book.on('error', errorHandler);

book.connect(function(){

  // the logger will output to the console and a log file
  book.logger.log('I am a debug message', 'debug'); // log levels can be set on log lines, and toggled with the `--logLevel` flag

  // define `book.data.stuff` to make it availalbe to all phases of the book
  book.data.stuff = 'something cool';

  var chapter1 = book.addChapter(1, 'Do the first thing in parallel', {threads: 10});
  var chapter2 = book.addChapter(2, 'Do that next thing in serial', {threads: 1});

  // chapter 1
  var i = 0;
  while(i < 100){
    chapter1.addPage('sleepy thing: ' + i, function(next){
      setTimeout(next, 100);
    });
    i++;
  }

  // chapter 2

  // chapters can also have pre-loaders which run before all pages
  chapter2.addLoader('do something before', function(next){
    book.logger.log('I am the preloader'); 
    next();
  });

  chapter2.addPage('the final step', function(next){
    next();
    // next(new Error('on no!')); // if you end a page with an error, the errorHandler will be invoked, and the book stopped
  });

  // chapters can also be loaded from /chapters/name/chapter.js in the project
  // book.loadChapters();

  // you can also configure an optional logger (perhaps to a DB) for empujar's internal status
  // book.on('state', function(data){ 
  //   databse.insertData('empujar', [data]); 
  // });

  book.run(function(){
    setTimeout(process.exit, 5000);
  });
});
```

There is also a more formal example you can explore within this project.  Check out /books/etl to learn more.

Empujar will connect to connections you define in `book/config/connections/NAME.js`, and there should be a matching transport in `/lib/connections/TYPE.js`.

When `book.run()` is complete, you probably want to `process.exit()`, or more gracefully shutdown.

You can subscribe to `book.on('error')` and `book.on('state')` events.  A cool thing to do would be to actually record these state events into your datawarehouse, if you are using empujar as an ETL tool:

```javascript
book.on('state', function(data){  datawarehouse.insertData('empujar', [data]);  });
```

## Project Layout

Create your project so that it looks like this:
```
| -\books
| ---\myBook
| -----\book.js
| -----\pids\
| -----\logs\
| -----\config\
| -----\config\connections\
| -----\config\connections\myDatabase.js
| -----\chapters\
| -----\chapters\chapte1.js
| -----\chapters\chapte2.js
```

## Launch Flags

The defaults for all launch flags are:

```javascript
{
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
}
```

**Examples:**

1. Run your book: `node yourBook.js`
2. Run your book in verbose mode: `node yourBook.js --logLevel debug`
3. Run only certain chapters in your book: `node yourBook.js --chapters 1,4` or a range: `node yourBook.js --chapters 100-300`
4. Extract only a small subset of yoru data (great in testing) `node yourBook.js --getAllLimit 1000`
  - This would make all invocations of `connection.getAll()` exit sucessfully after retrieving 1000 rows. 

## Connections

While you can create your own connections, Empujar ships with the tools to work with a number of the most common ones:

- mySQL
- Amazon Redshift
- Elasticsearch
- FTP
- S3

### MySQL

```javascript
var connection = book.connections.mysql.connection;

connection.connect = function(callback)
// Connection method; handled by book.connect();
// callback is passed (error) 

connection.showTables = function(callback)
// list tables
// callback is returned error, array of table names

connection.showColumns = function(table, callback)
// list the columns + metadata for each column
// callback is returned error, hash of columns + metadata

connection.query = function(query, data, callback)
// query the table
// data can be optional; used to fill in missing attributes/interpolate (?)
// callback is returned error, rows (array of hashes col-value)

connection.getAll = function(queryBase, chunkSize, dataCallback, doneCallback)
// fetch data from the cluster; normalized as an array of hashes.  Data is already typecast.
// queryBase -> the base mySQL query (Limit and offset will be appended automatically)
// chunkSize -> number of results to return (IE: limit)
// dataCallback -> callback called with each collection of data
//   -> (error, data, next)
//   -> data is normalized
//   -> next() must be called to continue
// doneCallback is passed (error, rowsFound) 

connection.getMax = function(table, column, callback)
// list the maximum value for a column in a table
// callback is returned error, maximum value from the table or null

connection.queryStream = function(query, callback)
// get a stream that returns results of a query
// events listed here: https://github.com/felixge/node-mysql#streaming-query-rows
// callback is returned error, stream

connection.insertData = function(table, data, callback, mergeOnDuplicates)
// add data to an table; create the index if needed.  Data should be normalized (IE results from #getAll)
// callback is passed (error) 

connection.addColumn = function(table, column, rowData, callback)
// add a column to a table. 
// RowData is an array of data to insert into the column which can be used to determine the column data type
// callback is returned error

connection.alterColumn = function(table, column, definition, callback)
// change the datatype of a column
// definition is a mySQL statment
// callback is returned error

connection.mergeTables = function(sourceTable, destinationTable, callback)
// merge the data from sourceTable into destinationTable
// destinationTable will be created if if doesn't exist
// destinationTable will be erased and recreated from sourceTable if there is no primary key present
// callback is returned error

connection.copyTableSchema = function(sourceTable, destinationTable, callback)
// create a new table (destinationTable) with the same schema as (sourceTable)
// callback is returned error

connection.dump = function(file, options, callback)
// mysqlDump the DB to file
// options: 
/*
  if(!options.binary){   options.binary = 'mysqldump';             }
  if(!options.database){ options.database = self.options.database; }
  if(!options.password){ options.password = self.options.password; }
  if(!options.host){     options.host = self.options.host;         }
  if(!options.port){     options.port = self.options.port;         }
  if(!options.user){     options.user = self.options.user;         }
  if(!options.tables){   options.tables = [];                      }
  if(!options.gzip){     options.gzip = false;                     }
*/
// callback is returned error

```

### Elasticsearch

```javascript
var connection = book.connections.mysql.connection;

connection.connect = function(callback)
// Connection method; handled by book.connect();
// callback is passed (error) 

connection.showIndices = function(callback)
// list the indices in the cluster
// callback is passed (error, indicies) 
//  -> `indicies` is a hash with index names and metadata

connection.insertData = function(index, data, callback)
// add data to an index; create the index if needed.  Data should be normalized (IE results from #getAll)
// callback is passed (error) 

connection.getAll = function(index, query, fields, chunkSize, dataCallback, doneCallback)
// fetch data from the cluster; normalized as an array of hashes.  Data is already typecast.
// index -> string name of index
// query -> the elasticsearch query (as a hash)
// fields -> array of fields you want returned; '*' can be passed as an argument to request all fields
// chunkSize -> number of results to return (from each server)
// dataCallback -> callback called with each collection of data
//   -> (error, data, next)
//   -> data is normalized
//   -> next() must be called to continue
// doneCallback is passed (error, rowsFound) 

```
### FTP

``` javascript
var connection = book.connections.mysql.connection;

connection.connect = function(callback)
// Connection method; handled by book.connect();
// callback is passed (error) 

connection.get = function(file, callback)
// donwload a file from the FTP server
// callback is passed (error, stream) 
//  -> `stream` which you can pipe to a file on disk or S3, etc

connection.listFiles = function(dir, callback)
// list files from a remote directory
// callback is passed (error, files) 
//  -> `files` is an array of remote file names

```

### Amazon Redshift

``` javascript
var connection = book.connections.mysql.connection;

connection.connect = function(callback)
// Connection method; handled by book.connect();
// callback is passed (error) 

connection.showTables = function(callback)
// list tables
// callback is returned error, array of table names

connection.showColumns = function(table, callback)
// list the columns + metadata for each column
// callback is returned error, hash of columns + metadata

connection.query = function(query, callback)
// query the table
// callback is returned error, rows (array of hashes col-value)

connection.getAll = function(queryBase, chunkSize, dataCallback, doneCallback)
// fetch data from the cluster; normalized as an array of hashes.  Data is already typecast.
// queryBase -> the base mySQL query (Limit and offset will be appended automatically)
// chunkSize -> number of results to return (IE: limit)
// dataCallback -> callback called with each collection of data
//   -> (error, data, next)
//   -> data is normalized
//   -> next() must be called to continue
// doneCallback is passed (error, rowsFound) 

connection.insertData = function(table, data, callback)
// add data to an table; create the index if needed.  Data should be normalized (IE results from #getAll)
// callback is passed (error) 

connection.mergeTables = function(sourceTable, destinationTable, callback)
// merge the data from sourceTable into destinationTable
// destinationTable will be created if if doesn't exist
// destinationTable will be erased and recreated from sourceTable if there is no primary key present
// callback is returned error

connection.addColumn = function(table, column, rowData, callback)
// add a column to a table. 
// RowData is an array of data to insert into the column which can be used to determine the column data type
// callback is returned error

connection.alterColumn = function(table, column, definition, callback)
// change the datatype of a column
// definition is a mySQL statment
// callback is returned error

connection.copyTableSchema = function(sourceTable, destinationTable, callback)
// create a new table (destinationTable) with the same schema as (sourceTable)
// callback is returned error

connection.getMax = function(table, column, callback)
// list the maximum value for a column in a table
// callback is returned error, maximum value from the table or null

```
