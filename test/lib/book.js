var Empujar = require(__dirname + '/../../index.js');
var should  = require('should');
var fs      = require('fs');

describe('book', function(){

  afterEach(function(){
    var logFile = process.cwd() + '/log/empujar.log';
    if(fs.existsSync(logFile)){
      fs.unlinkSync(logFile);
    }
  });

  it('can run in the simplest case', function(done){
    var book = new Empujar.book({logStdout: false});
    var ch   = book.addChapter(1, 'chapter');
    ch.addPage('sleep', function(next){ setTimeout(next, 100); });

    book.on('error', function(error){ should.not.exist(error); });

    book.connect(function(error){
      should.not.exist(error);
      book.run(function(){
        done();
      });
    });
  });

  it('#ensurePid', function(done){
    var book1;
    var book2;

    (function(){
      book1 = new Empujar.book({logStdout: false});
    }).should.not.throw();

    (function(){
      book2 = new Empujar.book({logStdout: false});
    }).should.throw(/empujar already running/);
    
    book1.removePid();
    done();
  });

  it('#log + error reporting', function(done){
    var book = new Empujar.book({logStdout: false});
    var ch   = book.addChapter(1, 'chapter');
    ch.addPage('sleep', function(next){ 
      next(new Error('something is wrong')); 
    });

    book.on('error', function(error){ 
      setTimeout(function(){
        error.message.should.equal('something is wrong'); 
        var log = String(fs.readFileSync(process.cwd() + '/log/empujar.log'));
        log.should.match(/Starting Empujar/);
        log.should.match(/Starting Chapter \[1\]: chapter \(1 threads\)/);
        log.should.match(/quitting Empujar due to error/);
        log.should.match(/context: chapter.name=chapter, chapter.options.threads=1, chapter.priority=1, page=sleep/);
        log.should.match(/emerg:  Error: something is wrong/);

        book.removePid();
        done();
      }, 1000);
    });

    book.connect(function(error){
      should.not.exist(error);
      book.run();
    });
  });

  it('#connect');
  
  describe('#chapters', function(){
    var results;
    beforeEach(function(){ results = []; });

    var buildBook = function(args){
      args.logStdout = false;
      var book = new Empujar.book(args);

      var ch1 = book.addChapter(1, 'chapter 1');
      ch1.addPage('ch 1', function(next){ 
        results.push('chapter 1, page 1'); 
        next();
      });

      var ch2 = book.addChapter(2, 'chapter 2');
      ch2.addPage('ch 2', function(next){ 
        results.push('chapter 2, page 1'); 
        next();
      });

      var ch3 = book.addChapter(3, 'chapter 3');
      ch3.addPage('ch 3', function(next){ 
        results.push('chapter 3, page 1');
        next(); 
      });

      return book;
    };

    it('can run all chapters in order', function(done){
      var book = buildBook({});
      book.connect(function(){
        book.run(function(){
          results.should.deepEqual([
            'chapter 1, page 1',
            'chapter 2, page 1',
            'chapter 3, page 1',
          ]);

          done();
        });
      });
    });

    it('can run some chapters: comma seperated', function(done){
      var book = buildBook({chapters: '1,3'});
      book.connect(function(){
        book.run(function(){
          results.should.deepEqual([
            'chapter 1, page 1',
            'chapter 3, page 1',
          ]);

          done();
        });
      });
    });

    it('can run some chapters: range', function(done){
      var book = buildBook({chapters: '2-3'});
      book.connect(function(){
        book.run(function(){
          results.should.deepEqual([
            'chapter 2, page 1',
            'chapter 3, page 1',
          ]);

          done();
        });
      });
    });

  });

});