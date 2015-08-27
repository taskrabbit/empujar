# Empujar. Empujarlo Bueno.
When you need to push data around, you push it. Push it real good.  An ETL and Operations tool.

## What

Empujar is a tool which moves stuff around.  It's built in node.js so you can do lots of stuff async-ly.  You can move data around (a ETL tool), files (a backup tool), and more!  

Empujar's top level object is a "book", which contains "chapters" and then "pages".  Chapters are excecuted 1-by-1 in order, and then each page in a chapter can be run in parallel (up to a *threading* limit you specify).

```javascript

#!/usr/bin/env node

process.chdir(__dirname);

var Empujar    = require(__dirname + '/../../index.js');
var optimist   = require('optimist'); 
var options    = optimist.argv; // get command line opts, like `--logLevel debug` or `--chapters 100`

// you can define custom error behavior when a page callback retruns an error
var errorHandler = function(error, context){
  console.log("OH NO! (but I handled the error) | " + error);
  setTimeout(process.exit, 5000);
};

var book = new Empujar.book(options);

book.connect(function(error, errorContext){
  if(error){ return errorHandler(error, errorContext); }

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

  book.on('error', errorHandler);

  book.run(function(error, errorContext){
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

## How to Use

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

1. Run your book: `node yourBook.js`
2. Run your book in verbose mode: `node yourBook.js --logLevel debug`
3. Run only certain chapters in your book: `node yourBook.js --chapters 1,4` or a range: `node yourBook.js --chapters 100-300`

## Supported Connections:
- mysql
- redshift
- elasticsearch
- FTP
- S3
- mixpanel
- delighted
