var fs             = require("fs");
var path           = require("path");
var util           = require("util");
var child_process  = require('child_process');

var cleanDir = function(dir) {
  var list = fs.readdirSync(dir);
  for(var i = 0; i < list.length; i++) {
    var filename = path.join(dir, list[i]);
    var stat = fs.statSync(filename);
    if(stat.isDirectory()) {
      cleanDir(filename);
    } else {
      fs.unlinkSync(filename);
    }
  }
};

var hashMerge = function(a, b){
  if(!a){ a = {}; }
  var response = {};
  var key;

  for(key in b){
    if(a[key] === null || a[key] === undefined){
      response[key] = b[key];
    }else{
      response[key] = a[key];
    }
  }

  for(key in a){
    if(
        (response[key] === null || response[key] === undefined ) && 
        a[key] !== null && 
        a[key] !== undefined
      ){
      response[key] = a[key];
    }
  }

  return response;
};

var isNumeric = function(n){
  // http://stackoverflow.com/questions/18082/validate-decimal-numbers-in-javascript-isnumeric
  return !isNaN(parseFloat(n)) && isFinite(n);
};

var isInt = function(n){
    return Number(n) === n && n % 1 === 0;
};

var isFloat = function(n){
    return n === Number(n) && n % 1 !== 0;
};

function fixedCharCodeAt(str, idx) {
    idx = idx || 0;
    var code = str.charCodeAt(idx);
    var hi, low;
    // High surrogate (could change last hex to 0xDB7F to treat high private surrogates as single characters)
    if (0xD800 <= code && code <= 0xDBFF) {
        hi = code;
        low = str.charCodeAt(idx + 1);
        if (isNaN(low)) { throw 'crazy utf8 error'; }
        return ((hi - 0xD800) * 0x400) + (low - 0xDC00) + 0x10000;
    }
    if (0xDC00 <= code && code <= 0xDFFF) { // Low surrogate
        return false;
    }
    return code;
}

var utf8ByteLength = function(str){
  // http://stackoverflow.com/questions/2848462/count-bytes-in-textarea-using-javascript/12206089#12206089
  var result = 0;
  for (var n = 0; n < str.length; n++) {
      var charCode = fixedCharCodeAt(str, n);
      if (typeof charCode === "number") {
          if (charCode < 128) {
              result = result + 1;
          } else if (charCode < 2048) {
              result = result + 2;
          } else if (charCode < 65536) {
              result = result + 3;
          } else if (charCode < 2097152) {
              result = result + 4;
          } else if (charCode < 67108864) {
              result = result + 5;
          } else {
              result = result + 6;
          }
      }
  }
  return result;
};

var extractFromArray = function(a){
  var i = 0;
  while(i < a.length){
    if(a[i] !== undefined && a[i] !== null){ 
      return a[i]; 
    }
    i++;
  }

  return null;
};

var objectFlatten = function(data) {
  // http://stackoverflow.com/questions/19098797/fastest-way-to-flatten-un-flatten-nested-json-objects
  var result = {};
  function recurse (cur, prop) {
      if (Object(cur) !== cur) {
          result[prop] = cur;
      } else if (Array.isArray(cur)) {
           for(var i=0, l=cur.length; i<l; i++)
               recurse(cur[i], prop + "[" + i + "]");
          if (l === 0)
              result[prop] = [];
      } else {
          var isEmpty = true;
          for (var p in cur) {
              isEmpty = false;
              recurse(cur[p], prop ? prop+"."+p : p);
          }
          if (isEmpty && prop)
              result[prop] = {};
      }
  }
  recurse(data, "");
  return result;
};

////////////////////////////////////////

exports.isNumeric        = isNumeric;
exports.isInt            = isInt;
exports.isFloat          = isFloat;
exports.cleanDir         = cleanDir;
exports.hashMerge        = hashMerge;
exports.utf8ByteLength   = utf8ByteLength;
exports.extractFromArray = extractFromArray;
exports.objectFlatten    = objectFlatten;
