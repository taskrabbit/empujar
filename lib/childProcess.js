var utils = require(__dirname + 'utils.js');

process.on('message', function(message){
  var args = message.args;
  var func = eval('(  ' + message.func + '  )');

  message.requires.forEach(function(r){
    this[r] = require(r);
  });

  // If your function is async, call `process.send` with your results
  var response = func(args);

  if(response !== null && response !== undefined){
    process.send(response);
  }
});