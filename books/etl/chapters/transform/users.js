exports.chapterLoader = function(book){

  // define
  var chapter = book.addChapter(3, 'TRANSFORM: `users`', {threads: 1});

  // helpers
  var destination = book.connections.destination.connection;

  chapter.addPage('users.totalPuchases', function(next){
    var query = '';
    query += 'UPDATE `users`                                            ';
    query += 'SET `totalPurchases` = (                                  ';
    query += '  SELECT count(1) FROM `purchases` WHERE userId = users.id ';
    query += ')                                                         ';
    
    destination.query(query, next);
  });

  chapter.addPage('users.totalSpentCents', function(next){
    var query = '';
    query += 'UPDATE `users`                                                 ';
    query += 'JOIN (                                                         ';
    query += '  SELECT                                                       ';
    query += '    users.id AS "userId",                                      ';
    query += '    sum(`products`.`priceInCents`) AS "totalSpentCents"        ';
    query += '  FROM users                                                   ';
    query += '  JOIN `purchases` ON `purchases`.`userId` = `users`.`id`      ';
    query += '  JOIN `products` ON `purchases`.`productId` = `products`.`id` ';
    query += '  GROUP BY `users`.`id`                                        ';
    query += ') AS `sumTable` ON `sumTable`.`userId` = `users`.`id`          ';
    query += 'SET `users`.`totalSpentCents` = `sumTable`.`totalSpentCents`   ';
    
    destination.query(query, next);
  });
};
