var utils = require(__dirname + '/utils.js');

var chapter = function(priority, name, options, book){
  this.options  = utils.hashMerge(options, this.defaults());
  this.priority = priority;
  this.name     = name;
  this.book     = book;
  this.data     = {};
  this.pages    = []; 
  this.loaders  = []; 
};

chapter.prototype.defaults = function(){
  return {
    threads: 1,
  };
};

chapter.prototype.addPage = function(name, run){
  var self = this;
  self.pages.push({
    name: name,
    run: run,
  });
};

chapter.prototype.addLoader = function(name, run){
  var self = this;
  self.loaders.push({
    name: name,
    run: run,
  });
};

chapter.prototype.run = function(callback){
  var self = this;
  var loaderJobs = [];
  var pageJobs   = [];

  self.loaders.forEach(function(loader){
    loaderJobs.push(function(next){
      var start = new Date().getTime();
      self.book.log('- Loader Starting: ' + loader.name, 'debug');
      
      self.book.emit('state', {
        state: 'loader:start',
        time: new Date(),
        name: loader.name,
        data: JSON.stringify({})
      });

      loader.run(function(error, closingMessage){
        var delta = Math.round(new Date().getTime() - start) / 1000;
        if(!error){
          if(!closingMessage){ closingMessage = ''; }
          if(typeof closingMessage === 'number'){ closingMessage = '[' + closingMessage + ' objects modified]'; }
          self.book.log('- Loader Complete: ' + loader.name + ' (' + delta + 's) ' + closingMessage);
          
          self.book.emit('state', {
            state: 'loader:end',
            time: new Date(),
            name: loader.name,
            duration: delta,
            data: JSON.stringify({
              closingMessage: closingMessage
            })
          });

        }
        if(error){ next(error, {name: loader.name}); }
        else{ next(); }
      });
    });
  });

  async.parallel(loaderJobs, function(error, failedLoader){
    if(error){
      failedLoader = utils.extractFromArray(failedLoader);
      callback(error, failedLoader);
    }else{
      // its important to build this block after the loader
      // as the loader may create chapters...
      
      self.pages.forEach(function(page){
        pageJobs.push(function(next){
          var start = new Date().getTime();
          self.book.log('- Starting Page: ' + page.name, 'debug');
          
          self.book.emit('state', {
            state: 'page:start',
            time: new Date(),
            name: page.name,
            data: JSON.stringify({})
          });

          page.run(function(error, closingMessage){
            var delta = Math.round(new Date().getTime() - start) / 1000;
            if(!error){
              if(!closingMessage){ closingMessage = ''; }
              if(typeof closingMessage === 'object'){ closingMessage = JSON.stringify(closingMessage); }
              if(typeof closingMessage === 'number'){ closingMessage = '[' + closingMessage + ' objects modified]'; }
              self.book.log('- Page Complete: ' + page.name + ' (' + delta + 's) ' + closingMessage); 
              
              self.book.emit('state', {
                state: 'page:end',
                time: new Date(),
                name: page.name,
                duration: delta,
                data: JSON.stringify({
                  closingMessage: closingMessage
                })
              });

            }
            if(error){ 
              next(error, {name: page.name}); 
            }
            else{ next(); }
          });
        });
      });

      async.parallelLimit(pageJobs, self.options.threads, function(error, failedPage){
        if(error){ failedPage = utils.extractFromArray(failedPage); }
        callback(error, failedPage);
      });
    }
  });
  
};

exports.chapter = chapter;