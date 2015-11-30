var Empujar  = require(__dirname + '/../../index.js');
var should   = require('should');
var mysql    = require('mysql');
var async    = require('async');
var zlib     = require('zlib');
var fs       = require('fs');

var host     = 'localhost';
var username = 'root';
var password = null;
var database = 'empujar_test';

var helper = mysql.createConnection({
  host     : host,
  user     : username,
  password : password,
  // database : database, // (comment out so we can create the DB if needed)
});

var mysqlPrototype = require(__dirname + '/../../lib/connections/mysql.js').connection;

var options = {
  host:     host,
  database: database,
  user:     username,
  password: password,
  charset: 'utf8mb4',
  dateStrings: false,
  varCharLength: 191,
};

var book = { logger: { log: function(){}, }, log: function(){}, options: {getAllLimit: Infinity} };
var mysql = new mysqlPrototype('mysql', 'mysql', options, book);

var buildUserTable = function(callback){
  var jobs = [];

  jobs.push(function(done){
    helper.query('DROP TABLE IF EXISTS `users`', done);
  });
  
  jobs.push(function(done){
    helper.query('CREATE TABLE `users` (`id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,`email` varchar(191) DEFAULT NULL,`first_name` varchar(191) DEFAULT NULL,`counter` bigint(11) DEFAULT NULL, PRIMARY KEY (`id`))', done);
  });
  
  jobs.push(function(done){
    helper.query('INSERT INTO `users` (`id`, `email`, `first_name`, `counter`) VALUES (1, \'evan@taskrabbit.com\', \'evan\', 5), (2, \'pablo@taskrabbit.com\', \'pablo\', 1), (3, \'aaron@taskrabbit.com\', \'aaron\', 4)', done);
  });

  async.series(jobs, callback);
};

describe('connection: mysql', function(){

  before(function(done){ helper.connect(done); });
  before(function(done){ helper.query('drop database if exists `' + database + '`', done); });
  before(function(done){ helper.query('create database `' + database + '`', done); });
  before(function(done){ helper.query('use `' + database + '`', done); });
  before(function(done){
    mysql.connect(done);
  });

  it('should be able to connect', function(){
    should.exist(mysql.settings.max_allowed_packet);
  });

  it('should have sensible defaults', function(){
    mysql.options.primaryKey.should.equal('id');
    mysql.options.unknownType.should.equal('varchar(0)');
  });

  describe('with data', function(){
  
    before(function(done){
      buildUserTable(done);
    });

    it('showTables and showColums works', function(done){
      mysql.showTables(function(error, tables){
      mysql.showColumns('users', function(error, columns){
        tables.should.deepEqual(['users']);
        columns.id.should.deepEqual({ type: 'bigint(11) unsigned', charLength: 11 });
        columns.email.should.deepEqual({ type: 'varchar(191)', charLength: 191 });
        columns.first_name.should.deepEqual({ type: 'varchar(191)', charLength: 191 });
        columns.counter.should.deepEqual({ type: 'bigint(11)', charLength: 11 });
        done();
      });
      });
    });

    it('can getMax', function(done){
      mysql.getMax('users', 'counter', function(error, max){
        max.should.equal(5);
        done();
      });
    });

    it('can query (read)', function(done){
      mysql.query('select * from users order by id asc', function(error, rows){
        should.not.exist(error);
        rows.length.should.equal(3);
        rows[0].email.should.equal('evan@taskrabbit.com');
        done();
      });
    });

    describe('writer', function(){

      after(function(done){ helper.query('delete from users where id > 3', done); });

      it('can query (write) (raw)', function(done){
        mysql.query('INSERT INTO `users` (`id`, `email`, `first_name`, `counter`) VALUES (4, \'paul@taskrabbit.com\', \'paul\', 10)', function(error, response){
          should.not.exist(error);
          response.rows.should.equal(1);
          done();
        });
      });

      it('can query (write) (data)', function(done){
        mysql.query('INSERT INTO `users` (`id`, `email`, `first_name`, `counter`) VALUES (?, ?, ?, ?)', [5, 'saba@taskrabbit.com', 'saba', 30], function(error, response){
          should.not.exist(error);
          response.rows.should.equal(1);
          done();
        });
      });

    });

    it('can return a query stream', function(done){
      var s = mysql.queryStream('select * from users order by id asc');
      var counter = 0;

      s.on('result', function(row){
        counter++;
      });

      s.on('end', function(){
        counter.should.equal(3);
        done();
      });
    });

    describe('#getAll', function(){

      it('works in the happy case', function(done){
        var q = 'select * from users';
        var chunkSize = 2;
        var emails = [];
        var calls = 0;

        var dataCallback = function(error, rows, next){
          should.not.exist(error);
          calls++;
          rows.forEach(function(row){ emails.push(row.email); });
          next();
        };

        var doneCallback = function(error, rowsFound){
          should.not.exist(error);
          rowsFound.should.equal(3);
          calls.should.equal(2);
          emails.should.deepEqual([
            'evan@taskrabbit.com',
            'pablo@taskrabbit.com',
            'aaron@taskrabbit.com'
          ]);

          done();
        };

        mysql.getAll(q, chunkSize, dataCallback, doneCallback);
      });

      it('works with no data returned', function(done){
        var q = 'select * from users where id > 1000';
        var chunkSize = 2;

        var dataCallback = function(error, rows, next){
          throw new Error('should not get here');
        };

        var doneCallback = function(error, rowsFound){
          should.not.exist(error);
          rowsFound.should.equal(0);
          done();
        };

        mysql.getAll(q, chunkSize, dataCallback, doneCallback);
      });

      it('returns failures properly', function(done){
        var q = 'select BREAK IT';
        var chunkSize = 2;

        var dataCallback = function(error, rows, next){
          throw new Error('should not get here');
        };

        var doneCallback = function(error, rowsFound){
          should.exist(error);
          error.message.should.match(/Unknown column/);
          done();
        };

        mysql.getAll(q, chunkSize, dataCallback, doneCallback);
      });

    });

    describe('#insertData', function(){

      before(function(done){
        helper.query('drop table if exists monies', done);
      });

      it('will create a table with the proper data types', function(done){
        // null, int, float, boolean, small-text, large-text, date, primary key
        var data = [
          {id: 1, counter: 4, happy: true, money: 12.234, when: new Date(1448486552507), small_words: 'a small amount of words', large_words: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque consectetur ullamcorper sapien. Phasellus tincidunt quam eu ligula vestibulum convallis. Nulla facilisi. Nulla aliquam ac elit id venenatis. In hac habitasse platea dictumst. Vestibulum dolor arcu, egestas non lacus ac, cursus semper lacus. Nunc sed commodo quam. Vivamus vitae augue vitae leo vulputate maximus sed sagittis dolor.'}
        ];

        var table = 'monies';

        mysql.insertData(table, data, function(error){
          should.not.exists(error);
          helper.query('describe ' + table, function(error, tableCreate){
          helper.query('select * from ' + table, function(error, rows){
            tableCreate.forEach(function(col){
              if(col.Field === 'id'){          col.Type.should.equal( 'bigint(20)' ); col.Extra.should.equal('auto_increment'); }
              if(col.Field === 'counter'){     col.Type.should.equal( 'bigint(20)' ); }
              if(col.Field === 'money'){       col.Type.should.equal( 'float' ); }
              if(col.Field === 'happy'){       col.Type.should.equal( 'tinyint(1)' ); }
              if(col.Field === 'when'){        col.Type.should.equal( 'datetime' ); }
              if(col.Field === 'small_words'){ col.Type.should.equal( 'varchar(191)' ); }
              if(col.Field === 'large_words'){ col.Type.should.equal( 'text' ); }
            });
            
            rows.length.should.equal(1);
            rows[0].id.should.equal(1);
            rows[0].money.should.equal(12.234);
            rows[0].when.getTime().should.be.within( new Date(1448486552000).getTime(), new Date(1448486553000).getTime() ); //second resolution from mySQL
            rows[0].small_words.should.equal('a small amount of words');

            done();
          });
          });
        });
      });  
      
      it('will update existing data when updates are present (mergeOnDuplicates=true)', function(done){
        buildUserTable(function(){
          var data = [{id: 1, first_name: 'joe'}];
          mysql.insertData('users', data, function(error){
            should.not.exists(error);
            helper.query('select * from `users` where id = 1', function(error, rows){
              rows.length.should.equal(1);
              rows[0].email.should.equal('evan@taskrabbit.com');
              rows[0].first_name.should.equal('joe');

              done();
            });
          });
        });
      });

      it('will error when data already exists with primary key (mergeOnDuplicates=false)', function(done){
        buildUserTable(function(){
          var data = [{id: 1, first_name: 'evan2'}];
          mysql.insertData('users', data, function(error){
            should.exist(error);
            error.message.should.match(/Duplicate entry/);
            done();
          }, false);
        });
      });

      it('will create new rows when data already exists and null primary key (mergeOnDuplicates=false)', function(done){
        buildUserTable(function(){
          var data = [{first_name: 'evan2', email: 'evan@taskrabbit.com'}];
          mysql.insertData('users', data, function(error){
            should.not.exists(error);
            helper.query('select * from `users` where email = "evan@taskrabbit.com" order by id asc', function(error, rows){
              rows.length.should.equal(2);
              rows[0].first_name.should.equal('evan');
              rows[1].first_name.should.equal('evan2');

              done();
            });
          }, false);
        });
      });
      
      it('will add columns to a table', function(done){
        buildUserTable(function(){
          var data = [{first_name: 'evan3', email: 'evan@taskrabbit.com', admin: true}];
          mysql.insertData('users', data, function(error){
            should.not.exists(error);
            helper.query('select * from `users` where email = "evan@taskrabbit.com" order by id asc', function(error, rows){
              rows.length.should.equal(2);
              
              rows[0].first_name.should.equal('evan');
              should.not.exists(rows[0].admin);

              rows[1].first_name.should.equal('evan3');
              rows[1].admin.should.equal(1);

              done();
            });
          });
        });
      });
      
      it('will alter to a table when data becomes available for unknown columns', function(done){
        buildUserTable(function(){
          var data = [{first_name: 'evan4', message: null}];
          mysql.insertData('users', data, function(error){
            helper.query('describe `users`', function(error, tableCreate){
              tableCreate.forEach(function(col){
                if(col.Field === 'message'){ col.Type.should.equal( 'varchar(0)' ); }
              });

              data = [{message: 'abc123'}];
              mysql.insertData('users', data, function(error){

                helper.query('describe `users`', function(error, tableCreate){
                  tableCreate.forEach(function(col){
                    if(col.Field === 'message'){ col.Type.should.equal( 'varchar(191)' ); }
                  });

                  done();
                });
              });
            });
          });
        });
      });

      it('will alter to a table when string length excedes the existing varchar limit', function(done){
        buildUserTable(function(){
          var data = [{id: 1, first_name: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque consectetur ullamcorper sapien. Phasellus tincidunt quam eu ligula vestibulum convallis. Nulla facilisi. Nulla aliquam ac elit id venenatis. In hac habitasse platea dictumst. Vestibulum dolor arcu, egestas non lacus ac, cursus semper lacus. Nunc sed commodo quam. Vivamus vitae augue vitae leo vulputate maximus sed sagittis dolor.'}];
          mysql.insertData('users', data, function(error){
            should.not.exist(error);
            helper.query('describe `users`', function(error, tableCreate){
              tableCreate.forEach(function(col){
                if(col.Field === 'first_name'){ col.Type.should.equal( 'text' ); }
              });

              done();
            });
          });
        });
      });

    });

    describe('#mergeTables', function(){

      beforeEach(function(done){
        helper.query('drop table if exists `users2`', function(){
          helper.query('drop table if exists `users3`', done);
        });
      });

      it('works happy path (copy)', function(done){
        buildUserTable(function(){
          mysql.mergeTables('users', 'users2', function(error){
            should.not.exist(error);
            helper.query('select * from users', function(error, data1){
            helper.query('select * from users', function(error, data2){
              data1.should.deepEqual(data2);
              done();
            });
            });
          });
        });
      });
      
      it('works happy path (merge) + #copyTableSchema', function(done){
        buildUserTable(function(){
          mysql.copyTableSchema('users', 'users2', function(error){
            should.not.exist(error);

            var data = [{id: 100, email: 'brian@taskrabbit.com', first_name: 'brian'}];
            mysql.insertData('users2', data, function(error){
              mysql.mergeTables('users', 'users2', function(error){
                helper.query('select * from users2 order by id asc', function(error, rows){
                  rows.length.should.equal(4);
                  rows[0].first_name.should.equal('evan');
                  rows[1].first_name.should.equal('pablo');
                  rows[2].first_name.should.equal('aaron');
                  rows[3].first_name.should.equal('brian');

                  done();
                });
              });
            });
          });
        });
      });
      
      it('fails when a table is missing', function(done){
        buildUserTable(function(){
          mysql.mergeTables('users3', 'users2', function(error){
            error.message.should.equal('sourceTable does not exist: users3');
            done();
          });
        });
      });
      
      it('does a full merge when the primary key is not present', function(done){
        var data2 = [{email: 'brian@taskrabbit.com', first_name: 'brian'}];
        var data3 = [{email: 'evan@taskrabbit.com', first_name: 'evan'}];
        mysql.insertData('users2', data2, function(error){
        mysql.insertData('users3', data3, function(error){

          mysql.mergeTables('users2', 'users3', function(error){
            should.not.exist(error);
            helper.query('select * from users3', function(error, rows){
              rows.length.should.equal(1);
              rows[0].first_name.should.equal('brian');
              done();
            });
          });

        });
        });
      });
      
      it('does a full merge when a new column is detected', function(done){
        var data2 = [{id: 1, email: 'brian@taskrabbit.com', first_name: 'brian', counter: 1}];
        var data3 = [{id: 2, email: 'evan@taskrabbit.com', first_name: 'evan'}];
        mysql.insertData('users2', data2, function(error){
        mysql.insertData('users3', data3, function(error){

          mysql.mergeTables('users2', 'users3', function(error){
            should.not.exist(error);
            helper.query('select * from users3', function(error, rows){
              rows.length.should.equal(1);
              rows[0].first_name.should.equal('brian');
              
              helper.query('describe `users3`', function(error, tableCreate){
                tableCreate.forEach(function(col){
                  if(col.Field === 'id'){ col.Type.should.equal( 'bigint(20)' ); }
                  if(col.email === 'email'){ col.Type.should.equal( 'varchar(191)' ); }
                  if(col.email === 'first_name'){ col.Type.should.equal( 'varchar(191)' ); }
                  if(col.email === 'counter'){ col.Type.should.equal( 'bigint(20)' ); }
                });

                done();
              });
            });
          });

        });
        });
      });

    });

    describe('#dump', function(){

      it('works (txt)', function(done){
        var file = '/tmp/dump.sql';
        var options = {
          database: database,
          username: username,
          password: password,
          host: host,
          gzip: false,
          port: null,
        };

        buildUserTable(function(){
          mysql.dump(file, options, function(error){
            should.not.exist(error);
            var body = String( fs.readFileSync(file) );
            
            body.should.match(/CREATE TABLE `users`/);
            body.should.match(/`id` bigint\(11\) unsigned NOT NULL AUTO_INCREMENT/);
            body.should.match(/\(1,'evan@taskrabbit.com','evan',5\)/);

            done();
          });
        });
      });

      it('works (gzip)');

    });
  
  });

});
