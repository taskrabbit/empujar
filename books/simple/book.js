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

  book.run(function(){
    setTimeout(process.exit, 5000);
  });
});