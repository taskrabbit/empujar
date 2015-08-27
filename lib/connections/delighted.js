var dateformat = require('dateformat');
var Delighted  = require('delighted');

var connection = function(name, type, options, book){
  this.name       = name;
  this.type       = type;
  this.options    = options;
  this.book       = book;
  this.connection = null;
};

connection.prototype.connect = function(callback){
  var self = this;
  self.connection = Delighted(self.options.apiKey);
  callback();
};

connection.prototype.getAll = function(since, dataCallback, doneCallback, page, rowsFound){
  var self = this;
  var data = [];
  if(page === undefined || page === null){ page = 1; }
  if(!rowsFound){ rowsFound = 0; }

  var options = {
    per_page : 100, 
    since    : since, // in unix timestamps (not JS timestamps)
    page     : page,
    expand   : 'person',
  };
  
  self.connection.surveyResponse.all(options).then(function(responses) {

    if(responses.length === 0){
      doneCallback(null, rowsFound);
    }else{
      rowsFound = rowsFound + responses.length;

      responses.forEach(function(resp){
        data.push({
          id:            parseInt(resp.id),
          person:        parseInt(resp.person.id),
          score:         parseInt(resp.score),
          comment:       resp.comment,
          permalink:     resp.permalink,
          created_at:    dateformat(resp.created_at * 1000, 'yyyy-mm-dd HH:MM:ss'),
          updated_at:    dateformat(resp.updated_at * 1000, 'yyyy-mm-dd HH:MM:ss'),
          customer_type: resp.customer_type,
          email:         resp.person.email,
          name:          resp.person.name,
        });
      });

      dataCallback(null, data, function(){
        if(self.book.options.getAllLimit > rowsFound){
          self.getAll(since, dataCallback, doneCallback, (page + 1), rowsFound);
        }else{
          doneCallback(null, rowsFound);
        }
      });

    }
  });
};

exports.connection = connection;