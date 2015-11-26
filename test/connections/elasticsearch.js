var Empujar = require(__dirname + '/../../index.js');
var should  = require('should');

describe('connection: elasticsearch', function(){

  it('should be able to connect');
  it('should have sensible defaults');

  describe('with data', function(){

    describe('#insertData', function(){

      it('will create a table with the proper data types');  // null, int, float, small-text, large-text, date, primary key
      it('will update existing data when updates are present (writeMethod=update)');
      it('will add new rows even when data already exists (writeMethod=xxx)');

    });

    describe('#getAll', function(){

      it('works in the happy case');
      it('works with no data returned');
      it('returns failures properly');

    });

  });

});