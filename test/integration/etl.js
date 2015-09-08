var should         = require('should');
var mysql          = require('mysql');
var async          = require('async');
var fs             = require('fs');
var child_process  = require('child_process');
var Empujar        = require(__dirname + '/../../index.js');
var source         = 'source';
var destination    = 'destination';
var connection     =  mysql.createConnection({ host: 'localhost', user: 'root' });

var cleanup = function(callback){
  var jobs = [];
  jobs.push(function(done){ connection.query('DROP DATABASE IF EXISTS ' + source, done); });
  jobs.push(function(done){ connection.query('DROP DATABASE IF EXISTS ' + destination, done); });

  async.series(jobs, callback);
}; 

var loadSourceDatabase = function(callback){
  var jobs = [];
  jobs.push(function(done){ connection.query('CREATE DATABASE ' + source, done); });
  jobs.push(function(done){ connection.query('CREATE DATABASE ' + destination, done); });
  jobs.push(function(done){ connection.query('USE ' + source, done); });
  
  var file = fs.readFileSync(__dirname + '/../../books/etl/source.sql').toString();
  var queries = file.split(';');
  queries.forEach(function(query){ 
    if(query.length > 2){
      jobs.push(function(done){ connection.query(query, done); });
    }
  });

  async.series(jobs, callback);
};

describe('integration: ETL', function(){
  before(cleanup);
  // after(cleanup);
  before(loadSourceDatabase);
  var stdout;
  var stderr;

  it('should have the source database loaded', function(done){
    connection.query('USE ' + source, function(){
      connection.query('SHOW TABLES', function(error, tables){
        var names = [];
        tables.forEach(function(t){
          names.push(t['Tables_in_' + source]);
        });

        names.length.should.equal(3);
        names.indexOf('users').should.be.within(0,2);
        names.indexOf('products').should.be.within(0,2);
        names.indexOf('purchases').should.be.within(0,2);
        done();
      });
    });
  });

  it('should not have the destination database loaded', function(done){
    connection.query('USE ' + destination, function(){
      connection.query('SHOW TABLES', function(error, tables){
        tables.length.should.equal(0);
        done();
      });
    });
  });

  it('should work', function(done){
    this.timeout(1000 * 30);
    console.log("********* TESTING ETL BOOK *********");
    var cmd = 'node ' + __dirname + '/../../books/etl/book.js';
    child_process.exec(cmd, function (error, out, err){
      should.not.exist(error);
      stdout = out;
      stderr = err;
      console.log(stdout);

      (stderr === '' || stderr === null).should.equal(true);
      stdout.indexOf('Empujar Complete').should.be.greaterThan(0);

      connection.query('USE ' + destination, done);
    });
  });

  it('should have moved users', function(done){
    connection.query('SELECT * FROM users ORDER BY updatedAt asc', function(error, rows){
      should.not.exist(error);
      rows.length.should.equal(5);
      
      rows[0].name.should.equal('Evan');
      rows[0].totalPurchases.should.equal(3);
      rows[0].totalSpentCents.should.equal(4500000);
      
      rows[4].name.should.equal('Mike');
      rows[4].totalPurchases.should.equal(1);
      rows[4].totalSpentCents.should.equal(25000000);
      
      done();
    });
  });

  it('should have moved producs', function(done){
    connection.query('SELECT * FROM products ORDER BY updatedAt asc', function(error, rows){
      should.not.exist(error);
      rows.length.should.equal(10);
      
      rows[0].name.should.equal('Civic');
      rows[0].totalPurchases.should.equal(4);
      rows[0].totalEarnedCents.should.equal(6000000);
      
      rows[9].name.should.equal('Aircraft Carrier');
      rows[9].totalPurchases.should.equal(1);
      rows[9].totalEarnedCents.should.equal(100000000);
      
      done();
    });
  });

  it('should have moved purchases', function(done){
    connection.query('SELECT * FROM purchases ORDER BY updatedAt asc', function(error, rows){
      should.not.exist(error);
      rows.length.should.equal(12);
      done();
    });
  });

  it('should have created the empujar table with events', function(done){
    connection.query('SELECT * FROM empujar where state = "book:end"', function(error, rows){
      should.not.exist(error);
      rows.length.should.equal(1);
      done();
    });
  });

});