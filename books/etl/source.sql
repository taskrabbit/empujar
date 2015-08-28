DROP TABLE IF EXISTS `products`;

CREATE TABLE `products` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `priceInCents` int(11) DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `updatedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `products` WRITE;

INSERT INTO `products` (`id`, `name`, `description`, `category`, `priceInCents`, `createdAt`, `updatedAt`)
VALUES
	(1,'Civic','That car you see everywhere','car',1500000,'2015-01-01 00:00:00','2015-01-01 00:00:00'),
	(2,'Tesla','No oil here!','car',7000000,'2015-01-02 00:00:00','2015-01-02 00:00:00'),
	(3,'Ram','A Truck for moving heavy things','car',5000000,'2015-01-03 00:00:00','2015-01-03 00:00:00'),
	(4,'Ferrari','Zoom Zoom','car',10000000,'2015-01-04 00:00:00','2015-01-04 00:00:00'),
	(5,'Delorian','88 MPH is the max speed','car',7000000,'2015-01-05 00:00:00','2015-01-05 00:00:00'),
	(6,'Tug Boat','Pull things that are fancier then you','boat',10000000,'2015-01-06 00:00:00','2015-01-06 00:00:00'),
	(7,'Jet Ski','Fast but small','boat',2000000,'2015-01-07 00:00:00','2015-01-07 00:00:00'),
	(8,'Kayak','Slow and small','boat',100000,'2015-01-08 00:00:00','2015-01-08 00:00:00'),
	(9,'Yacht','big slow and fancy','boat',25000000,'2015-01-09 00:00:00','2015-01-09 00:00:00'),
	(10,'Aircraft Carrier','planes on a boat!','boat',100000000,'2015-01-10 00:00:00','2015-01-10 00:00:00');

UNLOCK TABLES;

DROP TABLE IF EXISTS `purchases`;

CREATE TABLE `purchases` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `userId` int(11) DEFAULT NULL,
  `productId` int(11) DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `updatedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `purchases` WRITE;

INSERT INTO `purchases` (`id`, `userId`, `productId`, `createdAt`, `updatedAt`)
VALUES
	(1,1,1,'2015-08-01 00:00:00','2015-08-01 00:00:00'),
	(2,1,1,'2015-08-02 00:00:00','2015-08-02 00:00:00'),
	(3,1,1,'2015-08-03 00:00:00','2015-08-03 00:00:00'),
	(4,2,10,'2015-08-04 00:00:00','2015-08-04 00:00:00'),
	(6,3,6,'2015-08-05 00:00:00','2015-08-05 00:00:00'),
	(7,3,8,'2015-08-06 00:00:00','2015-08-06 00:00:00'),
	(8,4,1,'2015-08-07 00:00:00','2015-08-07 00:00:00'),
	(9,4,2,'2015-08-08 00:00:00','2015-08-08 00:00:00'),
	(10,4,3,'2015-08-09 00:00:00','2015-08-09 00:00:00'),
	(11,4,4,'2015-08-10 00:00:00','2015-08-10 00:00:00'),
	(12,4,5,'2015-08-11 00:00:00','2015-08-11 00:00:00'),
	(13,5,9,'2015-08-12 00:00:00','2015-08-12 00:00:00');

UNLOCK TABLES;

DROP TABLE IF EXISTS `users`;

CREATE TABLE `users` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` text,
  `email` varchar(255) DEFAULT NULL,
  `createdAt` datetime DEFAULT NULL,
  `updatedAt` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `users` WRITE;

INSERT INTO `users` (`id`, `name`, `email`, `createdAt`, `updatedAt`)
VALUES
	(1,'Evan','evan@example.com','2015-01-01 00:00:00','2015-02-02 00:00:00'),
	(2,'Brian','brian@example.com','2015-03-03 00:00:00','2015-04-04 00:00:00'),
	(3,'Kevin','kevin@example.com','2015-05-05 00:00:00','2015-05-05 00:00:00'),
	(4,'Pablo','pablo@example.com','2015-06-06 00:00:00','2015-06-06 00:00:00'),
	(5,'Mike','mike@example.com','2015-07-07 00:00:00','2015-08-08 00:00:00');

UNLOCK TABLES;
