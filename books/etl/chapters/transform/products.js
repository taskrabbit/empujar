exports.chapterLoader = function(book){

  // define
  var chapter = book.addChapter(3, 'TRANSFORM: `products`', {threads: 1});

  // helpers
  var destination = book.connections.destination.connection;

  chapter.addPage('products.totalPuchases', function(next){
    var query = '';
    query += 'UPDATE `products`                                                ';
    query += 'SET `totalPurchases` = (                                         ';
    query += '  SELECT count(1) FROM `purchases` WHERE productId = products.id ';
    query += ')                                                                ';
    
    destination.query(query, next);
  });

  chapter.addPage('products.totalEarnedCents', function(next){
    var query = '';
    query += 'UPDATE `products`                                                 ';
    query += 'JOIN (                                                            ';
    query += '  SELECT                                                          ';
    query += '    products.id AS "productId",                                   ';
    query += '    sum(`products`.`priceInCents`) AS "totalEarnedCents"          ';
    query += '  FROM products                                                   ';
    query += '  JOIN `purchases` ON `purchases`.`productId` = `products`.`id`   ';
    query += '  GROUP BY `products`.`id`                                        ';
    query += ') AS `sumTable` ON `sumTable`.`productId` = `products`.`id`       ';
    query += 'SET `products`.`totalEarnedCents` = `sumTable`.`totalEarnedCents` ';
    
    destination.query(query, next);
  });
};
