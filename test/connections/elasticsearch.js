var Empujar       = require(__dirname + '/../../index.js');
var should        = require('should');
var elasticsearch = require('elasticsearch');
var async         = require('async');
var zlib          = require('zlib');
var fs            = require('fs');

var host   = 'http://localhost:9200' ;
var sleep  = 3000;
var index  = 'empujar_test';
var logger =  {
  type: 'file',
  level: 'trace',
  path: '/dev/null',
};

var helper = new elasticsearch.Client({
  host: host,
  log:  logger,
});

var elasticsearchPrototype = require(__dirname + '/../../lib/connections/elasticsearch.js').connection;

var options = {
  host: host,
  log: logger,
};

var book = { logger: { log: function(){}, }, log: function(){}, options: {getAllLimit: Infinity} };
var es = new elasticsearchPrototype('elasticsearch', 'elasticsearch', options, book);

describe('connection: elasticsearch', function(){

  before(function(done){
    es.connect(done);
  });

  it('should be able to connect', function(done){
    es.showIndices(function(error, indices){
      should.not.exist(error);
      done();
    });
  });

  describe('with data', function(){

    describe('#insertData', function(){

      beforeEach(function(done){
        helper.indices.delete({index: index}, function(error){
          setTimeout(function(){
            if(!error || error.message.match(/IndexMissingException/)){
              done();
            }else{ 
              done(error);
            }
          }, sleep);
        });
      });

      it('will create an index with the proper data types', function(done){
        this.timeout(10 * 1000);
        // null, int, float, boolean, small-text, large-text, date, primary key
        var data = [
          {
            id: 1, 
            counter: 4, 
            happy: true, 
            money: 100.012, 
            when: new Date(1448486552507), 
            small_words: 'a small amount of words', 
          }
        ];

        es.insertData(index, data, function(error){
          should.not.exists(error);

          setTimeout(function(){

            helper.search({
              index: index,
              q: 'id:1',
            }, function(error, response){
              should.not.exists(error);
              response.hits.hits.length.should.equal(1);

              response.hits.hits[0]._source.id.should.equal(1);
              response.hits.hits[0]._source.happy.should.equal(true);
              response.hits.hits[0]._source.small_words.should.equal('a small amount of words');
              done();
            });

          }, sleep);
        });
      });

      it('will update existing data when updates are present', function(done){
        this.timeout(10 * 1000);
        // null, int, float, boolean, small-text, large-text, date, primary key
        var data = [
          {
            id: 1, 
            counter: 4, 
            happy: true, 
            money: 100.012, 
            when: new Date(1448486552507), 
            small_words: 'a small amount of words', 
          }
        ];

        es.insertData(index, data, function(error){
          should.not.exists(error);
          setTimeout(function(){

            data = [
              {
                id: 1, 
                happy: false, 
              }
            ];

            es.insertData(index, data, function(error){
              should.not.exists(error);
              setTimeout(function(){

                helper.search({
                  index: index,
                  q: 'id:1',
                }, function(error, response){
                  should.not.exists(error);
                  response.hits.hits.length.should.equal(1);

                  response.hits.hits[0]._source.id.should.equal(1);
                  response.hits.hits[0]._source.happy.should.equal(false);
                  response.hits.hits[0]._source.small_words.should.equal('a small amount of words');
                  done();
                });
              }, sleep);
            });
          }, sleep);
        });
      });

    });

    describe('#getAll', function(done){

      before(function(done){
        this.timeout(10 * 1000);

        helper.indices.delete({index: index}, function(error){
          setTimeout(function(){
            if(!error || error.message.match(/IndexMissingException/)){
              
              var data = [
                {
                  id: 1, 
                  email: 'evan@taskrabbit.com', 
                  first_name: 'evan', 
                  when: new Date(1448486552507), 
                },
                {
                  id: 2, 
                  email: 'aaron@taskrabbit.com', 
                  first_name: 'aaron', 
                  when: new Date(), 
                },
                {
                  id: 3, 
                  email: 'pablo@taskrabbit.com', 
                  first_name: 'pablo', 
                  when: new Date(), 
                }
              ];

              es.insertData(index, data, function(error){
                setTimeout(function(){
                  done(error);
                }, sleep);
              });

            }else{ 
              done(error);
            }
          }, sleep);
        });
      });

      it('works in the happy case + has the proper data types', function(done){
        this.timeout(15 * 1000);

        var queryLimit = 2;
        var counter = 0;
        var data = [];
        var query = {
          "query" : {
            "filtered" : {
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "range" : {
                        "id" : {
                          "gte" : 0
                        }
                      }
                    }
                  ]
                }
              }
            }
          }
        };

        var fields = [
          'id', 'email', 'when'
        ];

        var handleData = function(error, rows, done){
          should.not.exist(error);
          counter++;
          rows.forEach(function(r){ data.push(r); });
          done();
        };


        es.getAll(index, query, fields, queryLimit, handleData, function(error){
          should.not.exist(error);
          counter.should.equal(2);
          data.length.should.equal(3);

          should.not.exist(data[0].first_name);

          (data[0].when instanceof Date).should.equal(true);
          (typeof data[0].email).should.equal('string');
          (typeof data[0].id).should.equal('number');

          done();
        });
      });

      it('works with no data returned', function(done){
        this.timeout(15 * 1000);

        var queryLimit = 2;
        var query = {
          "query" : {
            "filtered" : {
              "query" : {
                "bool" : {
                  "must" : [
                    {
                      "range" : {
                        "id" : {
                          "gte" : 100
                        }
                      }
                    }
                  ]
                }
              }
            }
          }
        };

        var fields = [
          'id', 'email', 'when'
        ];

        var handleData = function(error, rows, done){
          console.log(rows)
          throw new Error('should not get here');
        };


        es.getAll(index, query, fields, queryLimit, handleData, function(error){
          should.not.exist(error);
          done();
        });
      });

      it('returns failures properly');

    });

  });

});