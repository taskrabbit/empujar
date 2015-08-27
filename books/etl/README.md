# ETL Book

This is an example ETL book showing how to use Empujar for this purpose.

## Instructions:
- install node and mysql
- create a `source` and `destination` database within mySQL
- load the contents of `source.sql` into the `source` database.  Leave the `destination` database blank
- inspect `./config/connections/source.js` and `./config/connections/destination.js` and ensure that the settings are correct for your host
- run the book (from the root directory of this project), `node books/etl/book.js`
