var Empujar = require(__dirname + '/../../index.js');
var should  = require('should');
var fs      = require('fs');

describe('chapter', function(){

  it('will run loaders before all pages', function(done){  
    var book   = new Empujar.book({logStdout: false});
    var chapter = book.addChapter(1, 'chapter');

    chapter.addLoader('loader', function(next){
      chapter.data.stuff = 'thing';
      next();
    });

    chapter.addPage('page', function(next){
      chapter.data.stuff.should.equal('thing');

      book.removePid();
      done();
    });

    chapter.run();
  });

  it('can run things in parallel', function(done){
    var book   = new Empujar.book({logStdout: false});
    var chapter = book.addChapter(1, 'chapter', {threads: 10});
    var i = 0;

    while(i < 10){
      chapter.addPage('page', function(next){
        setTimeout(next, 1000);
      });
      i++;
    }
    
    var start = new Date().getTime();
    chapter.run(function(){
      var delta = new Date().getTime() - start;
      delta.should.be.above(1 * 1000);
      delta.should.be.below(2 * 1000);

      book.removePid();
      done();
    });
  });

  it('can run things in series', function(){
    var book   = new Empujar.book({logStdout: false});
    var chapter = book.addChapter(1, 'chapter', {threads: 5});
    var i = 0;

    while(i < 10){
      chapter.addPage('page', function(next){
        setTimeout(next, 1000);
      });
      i++;
    }
    
    var start = new Date().getTime();
    chapter.run(function(){
      var delta = new Date().getTime() - start;
      delta.should.be.above(2 * 1000);
      delta.should.be.below(3 * 1000);

      book.removePid();
      done();
    });
  });

});