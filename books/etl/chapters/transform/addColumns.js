exports.chapterLoader = function(book){

  // define
  var chapter = book.addChapter(2, 'TRANSFORM: Add Columns', {threads: 5});

  // helpers
  var destination = book.connections.destination.connection;
  
  var newColumns = [
    [ 'users',    'totalPurchases',   'integer NULL' ],
    [ 'users',    'totalSpentCents',  'integer NULL' ],
    [ 'products', 'totalPurchases',   'integer NULL' ],
    [ 'products', 'totalEarnedCents', 'integer NULL' ],
  ];

  newColumns.forEach(function(data){
    var table  = data[0];
    var column = data[1];
    var type   = data[2];

    chapter.addPage('alter table `' + table + '`, ensure column `' + column + '`', function(next){
      destination.showColumns(table, function(error, columnData){
        if(error){ return next(error); }
        else if(columnData[column]){
          next();
        }else{
          var query = 'ALTER TABLE ' + table + ' ADD COLUMN ' + column + ' ' + type ;
          destination.query(query, next);
        }
      });
    });
  });
};
