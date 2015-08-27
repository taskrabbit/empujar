var should  = require('should');
var Empujar = require(__dirname + '/../../index.js');

describe('utils', function(){

  describe('utils.cleandir', function(){ it('should work'); });
  describe('utils.utf8ByteLength', function(){ it('should work'); });
  describe('utils.doInFork', function(){ it('should work'); });

  describe('utils.hashmerge', function(){
    it('should work', function(){
      Empujar.utils.hashMerge({a: 1}, {b: 2})
        .should.deepEqual({a: 1, b: 2});
      Empujar.utils.hashMerge({a: 1}, {a: 'x', b: 2})
        .should.deepEqual({a: 1, b: 2});
      Empujar.utils.hashMerge({a: 'x', b: 2}, {a: 1})
        .should.deepEqual({a: 'x', b: 2});
      Empujar.utils.hashMerge({a: 1}, {b: {c: 3}})
        .should.deepEqual({a: 1, b: {c: 3 }});
    });
  });

  describe('utils.isNumeric', function(){
    it('should work', function(){
      Empujar.utils.isNumeric(1).should.equal(true);
      Empujar.utils.isNumeric(-1).should.equal(true);
      Empujar.utils.isNumeric(0).should.equal(true);
      Empujar.utils.isNumeric(Infinity).should.equal(false);
      Empujar.utils.isNumeric('1').should.equal(true);
      Empujar.utils.isNumeric('one').should.equal(false);
      Empujar.utils.isNumeric(false).should.equal(false);
      Empujar.utils.isNumeric(true).should.equal(false);
    });
  });

  describe('utils.isInt', function(){
    it('should work', function(){
      Empujar.utils.isInt(1).should.equal(true);
      Empujar.utils.isInt(1.00000000001).should.equal(false);
      Empujar.utils.isInt('1').should.equal(false);
    });
  });

  describe('utils.isFloat', function(){
    it('should work', function(){
      Empujar.utils.isFloat(1).should.equal(false);
      Empujar.utils.isFloat(1.00000000001).should.equal(true);
      Empujar.utils.isFloat('1').should.equal(false);
    });
  });

  describe('utils.extractFromArray', function(){
    it('should work', function(){
      Empujar.utils.extractFromArray([1]).should.equal(1);
      Empujar.utils.extractFromArray([null, null, 1, undefined, null]).should.equal(1);
      Empujar.utils.extractFromArray([null, null, 1, undefined, 3]).should.equal(1);
    });
  });

  describe('utils.objectFlatten', function(){
    it('should work', function(){
      Empujar.utils.objectFlatten({
        stuff: {
          a: 1,
          b: 2,
        }, 
        c: 3,
        extra: {
          stuff: {
            is: {
              yay: true
            }
          }
        },
        'what about': {
          ' spaces?': 'cool too'
        },
      }).should.deepEqual({ 
        c: 3, 
        'extra.stuff.is.yay': true, 
        'stuff.a': 1, 
        'stuff.b': 2,
        'what about. spaces?': 'cool too',
      });
    });
  });

});