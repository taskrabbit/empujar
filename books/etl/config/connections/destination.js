module.exports = {
  type:  'mysql',
  options: {
    connectionLimit: 10,
    host:     'localhost',
    port:     3306,
    database: 'destination',
    user:     'root',
    password: null,
    charset: 'utf8mb4',
    dateStrings: false,
    varCharLength: 191,
  }
};
