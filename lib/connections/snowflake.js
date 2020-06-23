var fs        = require('fs');
var snowflake = require('snowflake-sdk');
var async     = require('async');
var filesize  = require('filesize');
var exec      = require('child_process').exec;
var utils     = require(__dirname + '/../utils.js');
const util    = require('util')

// Data 1193 : Updating log level to trace for snowflake connector. 
// Need to remove this after investigation.
snowflake.configure({logLevel : 'trace'});

var connection = function(name, type, options, book){
    this.name         = name;
    this.type         = type;
    this.options      = options;
    this.book         = book;
    this.connection   = null;
    this.tables       = [];
    this.connection_id = '';
    this.settings     = {
        max_allowed_packet: 4194304,
    };

    if(!this.options.varCharLength){
        this.options.varCharLength = 0;
    }

    if(!this.options.primaryKey){
        this.options.primaryKey = 'ID';
    }

    if(!this.options.dumpLogTimer){
        this.options.dumpLogTimer = 1000 * 30;
    }

};

connection.prototype.connect = function(callback){
    var self = this;
    self.connection = snowflake.createConnection(self.options);

    self.connection.connect(function(err, conn) {
        if (err) {
            self.book.logger.log('Unable to connect: ' + err.message, 'error');
            callback(err);
        } else {
            self.connection_id = conn.getId();
            self.book.logger.log('Connection Id : ' + self.connection_id, 'debug');
            self.showTables(function(error, tables){
                callback(error);
            });

        }
    });
};

connection.prototype.showTables = function(callback){
    var self = this;
    var rows = [];

    self.query('SHOW TABLES', function(error, dbRows){
        if(error){ 
            callback(error); 
        }
        else{
            dbRows.forEach(function(row){
                rows.push(row['name'])
            });
            rows.sort();
            self.tables = rows;
            callback(null, rows);
        }
    });
};

connection.prototype.showColumns = function(table, callback){
    var self = this;
    var rowObj = {};
    var query = 'SHOW COLUMNS IN ' + table + '';

    self.query(query, function(error, rows){
        if(!error && rows && rows.length > 0){
            rows.forEach(function(row){
                var dataTypeObj = JSON.parse(row.data_type)
                var key = String(row.column_name).toUpperCase()
                rowObj[key] = { type : dataTypeObj.type };

                if(dataTypeObj.length){
                    rowObj[key].charLength = dataTypeObj.length;
                }

                if(dataTypeObj.precision){
                    rowObj[key].precision = dataTypeObj.precision;
                }

                if(dataTypeObj.scale){
                    rowObj[key].scale = dataTypeObj.scale;
                }

            });
        }

        callback(error, rowObj);
    });
};

connection.prototype.query = function(query, data, callback, skipTransactions){
    var self = this;
    if(!skipTransactions){ skipTransactions = true; }

    if(typeof data === 'function' && !callback){
        callback = data; data = null;
    }

    if(query instanceof Array){
        var steps = [];

        //CODE REVIEW - Should I just remove transactions altogether ? - to start , remove transactions
        if(skipTransactions === false){
            steps.push(function(next){
                var runner = self.connection.query(' START TRANSACTION ', next);
                self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.getSqlText(), 'debug');
            });
        }

        query.forEach(function(q){
            steps.push(function(next){
                var runner = self.query(q, next);
                self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + q, 'debug');
            });
        });

        if(skipTransactions === false){
            steps.push(function(next){
                var runner = self.connection.query(' COMMIT ', next);
                self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.getSqlText(), 'debug');
            });
        }

        async.series(steps, function(error){
            if(error && skipTransactions === false){
                var runner = self.connection.query(' ROLLBACK ', function(){
                    callback(error);
                });
                self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.getSqlText(), 'debug');
            }else if(error){
                callback(error);
            }else{
                callback();
            }
        });

    } else {

        var runner = self.connection.execute({
            sqlText: query,
            binds: data,
            complete: function(err, stmt, rows) {

                if (err) {
                    //In SQL Queries if there are empty lines between sql queries , snowflake throws a exception. This statement provides a workaround to not get that error.
                    if(String(err.message).includes('Empty SQL statement')) {
                        callback(null);
                    } else if(!(String(query).startsWith('SHOW COLUMNS') && String(query).endsWith('_TMP'))){
                        self.book.logger.log('Error in : ' + String(query), 'error');
                        self.book.logger.log('Error message : ' + err.message, 'error');
                        self.book.logger.log('Stack Trace : ' + err, 'error');
                        self.book.logger.log('Query Id : ' + stmt.getStatementId() + ' Payload Info : ' + String(data), 'error');
                        self.book.logger.log('Connection Id : ' + self.connection_id, 'error');
                    }
                    callback(err);
                } else {
                    if(String(query).includes('INSERT INTO EMPUJAR')  ) {
                        self.book.logger.log('Empujar insert , Query Id ' + stmt.getStatementId() + ' Payload Info : ' + String(data), 'debug');
                    }

                    callback(null, rows)
                }
            }
        });

        self.book.logger.log('   ' + self.name + '/' + self.type + ' >> ' + runner.getSqlText(), 'debug');
    }
};

connection.prototype.getVariable = function(variable, callback){
    var self = this;

    self.query('SHOW PARAMETERS LIKE ?', variable, function(error, rows){
        if(error){ callback(error); }
        else if(!rows || rows.length === 0){ callback(new Error('Variable not found')); }
        else{
            callback(null, rows[0].Value);
        }
    });
};

connection.prototype.getMax = function(table, column, callback){
    table = String(table).toUpperCase()
    column = String(column).toUpperCase()
    var self = this;
    self.showTables(function(error){
        if(!error && self.tables.indexOf(table) >= 0){
            self.showColumns(table, function(error, rowObj){
                if(rowObj[column]){
                    var backendQuery = 'SELECT MAX(' + column + ') as "max" from ' + table + ''
                    self.query(backendQuery, function(error, rows){
                        if(!error && rows && rows[0].max){
                            var max = rows[0].max;
                            callback(null, max);
                        }else{
                            callback(error);
                        }
                    });
                }else{
                    callback(error);
                }
            });
        }else if(!error){
            callback();
        }else{
            callback(error);
        }
    });
};

connection.prototype.getAll = function(queryBase, chunkSize, dataCallback, doneCallback, rowsFound){
    var self = this;
    chunkSize = parseInt(chunkSize);
    if(!rowsFound){ rowsFound = 0; }
    var query = '' + queryBase;
    query += " OFFSET " + rowsFound + " FETCH NEXT " + chunkSize + " ROWS";

    self.query(query, function(error, rows){
        if(error){
            return doneCallback(error, rowsFound);
        }else if(rows.length === 0){
            return doneCallback(null, rowsFound);
        }else{
            rowsFound = rowsFound + rows.length;
            dataCallback(null, rows, function(){
                if(self.book.options.getAllLimit > rowsFound){
                    self.getAll(queryBase, chunkSize, dataCallback, doneCallback, rowsFound);
                }else{
                    doneCallback(null, rowsFound);
                }
            });
        }
    });
};

connection.prototype.insertData = function(table, data, callback, mergeOnDuplicates, mergeKey){

    var self = this;
    table = String(table).toUpperCase()

    self.book.logger.log('Inside insertData for table ' + table, 'trace');

    if(data.length === 0){
        return callback();
    }

    //Creating the temporary staging table
    var tmp_table = ''
    if (table === 'EMPUJAR') {
        tmp_table = table
    } else {
        tmp_table = table + "_TMP"
    }

    var needsComma = false;
    if(!mergeKey) { mergeKey = self.options.primaryKey}

    self.createtemporaryTable(tmp_table, data, function(error) {
        if(error) {
            self.book.logger.log('Error inside createtemporaryTable for table ' + table, 'error');
            callback(error);
        } else {

            if(tmp_table === 'EMPUJAR') {
                return null;
            }

            self.showTables(function(error, tables){

                self.showColumns(table, function(error, columnData) {
                    //Check if original Table exists
                    if(tables.indexOf(table) < 0) {
                        self.copyTableSchema(tmp_table, table, function(error, rows) {
                            if(error) {
                                self.book.logger.log('Error in copying table Schema for table ' + table + ' . Exiting out...', 'error');
                                callback(error)
                            }
                            self.book.logger.log('Copying the data into the newly created table ' + table , 'trace');
                            self.insertData(table, data, callback, mergeOnDuplicates, mergeKey);
                        });
                    } else {
                        self.showColumns(tmp_table, function(error, tempColumnData){
                            var tempTableKeys = Object.keys(tempColumnData)
                            var mainTableKeys = Object.keys(columnData)

                            columnDifference = arr_diff(tempTableKeys, mainTableKeys);

                            //Running it recursively for each time's last value of columndifference
                            if(columnDifference.length > 0) {
                                column=columnDifference[columnDifference.length-1]
                                if(mainTableKeys.indexOf(column) < 0) {
                                    self.book.logger.log('There a difference of column ' + column + ' between the main and temporary table. Adding that column now to main table.', 'trace');
                                    var columnDefinition = getColumnDefinition(tempColumnData, column)
                                    self.addColumnUsingDataType(table, column, columnDefinition, function(error, rows) {
                                        if(error) {
                                            self.book.logger.log('Error altering table ' + table + ' for column ' + column, 'error');
                                        }
                                        else {
                                            self.insertData(table, data, callback, mergeOnDuplicates, mergeKey)
                                        }
                                    });
                                }
                            }else {
                                //Check if there's been a change in the column type in the temp table
                                var columnList = [];
                                mainTableKeys.forEach(function (column) {

                                    if (tempColumnData[column] !== null && tempColumnData[column] !== undefined) {
                                        if ((columnData[column].type === 'TEXT') && (columnData[column].charLength !== 16777216 ) && (columnData[column].type === tempColumnData[column].type) && (columnData[column].charLength !== tempColumnData[column].charLength)) {
                                            self.book.logger.log('For Column ' + column + ' the data size is greater the allocated char length, so we need to change that to TEXT type', 'trace');
                                            columnList.push(column);
                                        }
                                    }

                                });

                                if(columnList.length > 0) {
                                    self.alterTableModifyColumns(table, columnList, function(error){
                                        if(error) {
                                            self.book.logger.log('Error while doing alter table for Text columns', 'error');
                                        } else {
                                            self.insertData(table, data, callback, mergeOnDuplicates, mergeKey);
                                        }
                                    });
                                } else {

                                    needsComma = false

                                    var mergeQuery = '';
                                    mergeQuery += 'MERGE INTO ' + table + ' USING ( '

                                    // Build sub-query that groups the rows on all the fields, producing a unique set of rows.
                                    needsComma = false;
                                    mergeQuery += 'SELECT '
                                    tempTableKeys.forEach(function(key){
                                        if(needsComma) mergeQuery += ', ';
                                        mergeQuery += tmp_table + '."' + key + '" \n'
                                        needsComma = true;
                                    });
                                    needsComma = false;
                                    mergeQuery += ' FROM ' + tmp_table + ' GROUP BY '
                                    tempTableKeys.forEach(function(key){
                                        if(needsComma) mergeQuery += ', ';
                                        mergeQuery += tmp_table + '."' + key + '" \n'
                                        needsComma = true;
                                    });

                                    mergeQuery += ' ) AS ' + tmp_table + ' ON \n'
                                    mergeQuery += table + '.' + '\"' + String(mergeKey).toUpperCase() + '\"' + ' = ' + tmp_table + '.' + '\"' + String(mergeKey).toUpperCase() + '\"' + ' \n'
                                    mergeQuery += ' WHEN MATCHED THEN UPDATE SET \n'

                                    needsComma = false;
                                    tempTableKeys.forEach(function(key){
                                        if(needsComma) mergeQuery += ', ';
                                        mergeQuery += table + '.' + '\"' + key + '\"' + ' = ' + tmp_table + '.' + '\"' + key + '\"'+ ' \n'
                                        needsComma = true;
                                    });

                                    needsComma = false

                                    mergeQuery += ' WHEN NOT MATCHED THEN INSERT ('

                                    tempTableKeys.forEach(function(key){
                                        if(needsComma) mergeQuery += ', ';
                                        mergeQuery += '\"' + key + '\"'
                                        needsComma = true;
                                    });

                                    needsComma = false

                                    mergeQuery += ') VALUES ('

                                    tempTableKeys.forEach(function(key){
                                        if(needsComma) mergeQuery += ', ';
                                        mergeQuery += tmp_table + '.' + '\"' + key + '\"' + ' \n'
                                        needsComma = true;
                                    });

                                    mergeQuery += ')'
                                    // self.book.logger.log('Running following Merge Query -: ' + mergeQuery, 'trace');
                                    self.query(mergeQuery, function(error, rows){

                                        if(error) {
                                            self.book.logger.log('Error While running merge query for table ' + table, 'error');
                                            callback(error, rows);
                                        } else {

                                            var clearQuery = 'delete from ' + tmp_table;

                                            self.query(clearQuery, function (clearError, clearRows) {

                                                if(clearError) {
                                                    self.book.logger.log('Error clearing out temp table for table ' + table + ' . Exiting out...', 'error');
                                                }
                                                callback(clearError, clearRows);

                                            });
                                        }
                                    });
                                }
                            }
                        });
                    }
                });
            });
        }
    }, mergeOnDuplicates, mergeKey);
};

function getColumnDefinition(columnData, key) {
    var self = this;
    var columnDefinition = '';

    if( columnData[String(key).toUpperCase()].type === 'TEXT') {
        columnDefinition = 'TEXT(' + columnData[String(key).toUpperCase()].charLength +')'
        return columnDefinition;
    } else if( ( columnData[String(key).toUpperCase()].type === 'FIXED' ) ) {
        columnDefinition = 'NUMBER';

        columnDefinition += '('

        if(columnData[String(key).toUpperCase()].precision) {
            columnDefinition += columnData[String(key).toUpperCase()].precision
        }
        if(columnData[String(key).toUpperCase()].scale) {
            columnDefinition += ',' + columnData[String(key).toUpperCase()].scale + ')'
        } else {
            columnDefinition += ')'
        }

        return columnDefinition;

    } else {
        return columnData[String(key).toUpperCase()].type;
    }
};


function arr_diff (a1, a2) {

    var a = [], diff = [];

    for (var i = 0; i < a1.length; i++) {
        a[a1[i]] = true;
    }

    for (var i = 0; i < a2.length; i++) {
        if (a[a2[i]]) {
            delete a[a2[i]];
        }
    }

    for (var k in a) {
        diff.push(k);
    }

    return diff;
}


function computeDataToBePushed(row, columnData, key) {
    //To convert 0 Timestamps which cause error in snowflake to snull
    if(row[key] === '0000-00-00 00:00:00') {
        return null;
    }

    if(row[key] == null) {
        return row[key];
    }

    if( row[key] !== null && row[key] !== undefined ) {
        if(row[key] instanceof Date === true) {
            var dbTime = null;

            if(!isNaN(row[key].getTime())) {
                dbTime = row[key].toISOString();
            }

            //To convert 0 Timestamps which cause error in snowflake to snull
            if(dbTime === '0000-00-00T00:00:00Z') {
                return null;
            }

            return dbTime;
        }

        //Add Cast for Boolean valyes to 1 or 0
        else if(typeof row[key] === 'boolean') {

            if(!row[key]) {
                return 0;
            }

            if(row[key]) {
                return 1;
            }

        }

        else if((typeof row[key]) === 'string' ) {
            return  String(row[key]);
        } else {
            return row[key];
        }
    }
};

connection.prototype.createtemporaryTable = function(table, data, callback, mergeOnDuplicates, mergeKey){

    var self = this;
    var query = '';
    var max_insert_length = (self.settings.max_allowed_packet) * (3/4);
    var thisData = [];
    var keys = [];
    var insertArray = [];
    var row;
    var needsComma = false;
    var updatableColumns = [];
    var insertLength = 0;

    self.book.logger.log('Inside createtemporaryTable for table ' + table, 'trace');

    if(!callback){
        self.book.logger.log('No callback assigned , so creating an anonymous function for table ' + table, 'trace');
        callback = function(){};
    }
    if(mergeOnDuplicates == null){ mergeOnDuplicates = true; }

    if(data.length === 0){
        self.book.logger.log('Data Length is 0. returning empty callback for table ' + table, 'trace');
        return callback();
    }


    self.showTables(function(error, tables){

        self.book.logger.log('Show Tables count : ' + tables.length, 'debug');
        self.book.logger.log('Checking Table : ' + table, 'debug');

        self.showColumns(table, function(error, columnData){

            if(tables.indexOf(table) < 0){
                self.book.logger.log('Table ' + table + ' does not exist. Calling buildTableFromData to create it.', 'trace');
                self.buildTableFromData(table, data, true, function(error){
                    if(error){
                        self.book.logger.log('Error in buildTableFromData for table  ' + table + ' .Exiting out..', 'error');
                        callback(error);
                    }
                    else{ self.createtemporaryTable(table, data, callback, mergeOnDuplicates, mergeKey); }
                });

            }else{
                while(data.length > 0){
                    row = data.shift();
                    Object.keys(row).forEach(function(key){
                        if( (keys.indexOf(key) < 0) && ( row[key] !== null && row[key] !== undefined ) ) { keys.push(key); }
                        insertLength += String( row[key] ).length; // TODO: UTF8 safe the string lengths
                    });
                    thisData.push( row );
                }

                var missingKey = false;
                keys.forEach(function(key){
                    if(!columnData[String(key).toUpperCase()]){
                        missingKey = key;
                    }
                });

                if(missingKey){

                    var rowData = [];
                    data = thisData.concat(data);

                    data.forEach(function(row){

                        var missingDataTobePushed = computeDataToBePushed(row,columnData, missingKey);

                        rowData.push( missingDataTobePushed );

                    });

                    self.addColumn(table, missingKey, rowData, function(error){
                        if(error){ callback(error); }
                        else{ self.createtemporaryTable(table, data, callback, mergeOnDuplicates, mergeKey); }
                    });

                }else{
                    var jobs = [];

                    var updatableColumnData = {};
                    var textColumnsToExpand = [];
                    var columnSet = new Set();
                    var mapOfMergeKeys = new Map();

                    thisData.forEach(function(row){
                        var thisData = [];
                        var tupleData = "";

                        if (!mapOfMergeKeys.has(row[String(mergeKey).toLowerCase()])) {

                            keys.forEach(function(key){


                                var datatobePushed = computeDataToBePushed(row, columnData, key);

                                thisData.push(datatobePushed);

                                if(row[key] !== null && row[key] !== undefined){
                                    // TODO: UTF8 safe the string lengths
                                    if(columnData[String(key).toUpperCase()].type.match(/TEXT/) && columnData[String(key).toUpperCase()].charLength && row[key].length > columnData[String(key).toUpperCase()].charLength){
                                        columnSet.add(key);
                                    }
                                }
                            });
                            insertArray.push( thisData );
                            mapOfMergeKeys.set(row[String(mergeKey).toLowerCase()], 1)

                        }

                    });

                    self.book.logger.log('After removing duplicate entries for table ' + table + ' Original and new numbers are ' + thisData.length + ' and ' + insertArray.length, 'trace');

                    if(columnSet.size > 0) {
                        self.alterTableModifyColumns(table, columnSet, function (error, rows) {

                            if(error) {
                                self.book.logger.log('Error Modifying Columns in createtemporaryTable for table  ' + table + ' .Exiting out..', 'error');
                            } else {
                                self.createtemporaryTable(table, data, callback, mergeOnDuplicates, mergeKey);
                            }

                        });
                    } else {
                        query += ' INSERT INTO ' + table + ' ( ';

                        keys.forEach(function(key){
                            if(needsComma){ query += ','; }
                            query += ' ' + '\"' + String(key).toUpperCase() + '\"' + ' ';
                            needsComma = true;
                        });
                        query += ' ) VALUES  (';

                        needsComma = false;

                        keys.forEach(function(key){
                            if(needsComma){ query += ','; }
                            query += ' ? ';
                            needsComma = true;
                        });

                        query += " )"

                        self.query(query, insertArray, function(error, rows) {

                            if(error) {
                                callback(error)
                            }
                            else {
                                callback(null)

                            }
                        });
                    }
                }
            }
        });
    });
};

//TO CHECK - check if one column value is 1 , followed by A , what happens to alter column  -
connection.prototype.buildTableFromData = function(table, data, isTempory , callback){
    var self       = this;
    var query      = '';
    var types      = {};
    var needsComma = false;
    var overrideType = {};
    if(!data || data.length === 0){
        return callback(new Error('No data provided to create table ' + table + ''));
    }

    data.forEach(function(row){
        for(var key in row){

            if( row[key] !== null && row[key] !== undefined ){

                if(types[key] === 'NUMBER' && utils.isFloat(row[key]) ){
                    types[key] = 'FLOAT';
                }

                else if(!types[key] ){

                    if(typeof row[key] === 'boolean'){
                        types[key] = 'SMALLINT';
                    }else if(typeof row[key] === 'number'){
                        if( utils.isInt(row[key]) ){
                            types[key] = 'NUMBER';
                        }else{
                            overrideType[key] = 'FLOAT';
                        }
                    }else if(row[key] instanceof Date === true){
                        types[key] = 'DATETIME';
                    }else{
                        types[key] = 'VARCHAR';
                    }
                }
                if(key === self.options.primaryKey && types[key].indexOf('PRIMARY') < 0){
                    if(typeof row[key] === 'number'){
                        types[key] += ' PRIMARY KEY NOT NULL AUTOINCREMENT';
                    }else{
                        types[key] += ' PRIMARY KEY';
                    }
                }
            }
        }
    });

	//Override Int values with Float if we find even a single float
	if(Object.keys(overrideType).length > 0) {
	for (var key in overrideType) {
		if (overrideType.hasOwnProperty(key)) {
		types[key] = overrideType[key]
		}
	}
	}

    var sortedKeys = Object.keys(types);
    [ self.options.primaryKey ].reverse().forEach(function(e){
        if(sortedKeys.indexOf(e) >= 0) {
            sortedKeys.splice(sortedKeys.indexOf(e), 1);
            sortedKeys.unshift(e);
        }

    });

    query += ' CREATE '

    if(isTempory == true) {
        query += 'TEMPORARY '
    }

    query += 'TABLE IF NOT EXISTS ' + table + ' ( \n';

    sortedKeys.forEach(function(key){
        if(needsComma) query += ', ';
        query += '  ' + '\"' + String(key).toUpperCase() + '\"' + ' ' + types[key];
        query += ' \n';
        needsComma = true;
    });

    query += ' ) \n';
    self.query(query, callback);
};

connection.prototype.addColumnUsingDataType = function(table, column, type, callback){
    var self = this;
    query = 'ALTER TABLE ' + table + ' ADD ' + column + ' ' + type;
    self.query(query, function(error, rows){

        if(error) {
            callback(error);
        }

        callback(null, rows);
    });
};

connection.prototype.alterTableModifyColumns = function(table, columnList, callback){
    var self = this;

    var needsComma = false;

    var query = 'ALTER TABLE ' + table + ' MODIFY ';

    // FIXME: 2018-11-18 [Aaron Binns] Need to wrap column name in double-quotes.  Sometimes column names use reserved words, such as "group"
    columnList.forEach(function(column){
        if(needsComma) query += ', ';
        query += '"' + column + '" TEXT'
        needsComma = true;
    });

    self.query(query, function(error, rows){

        if(error) {
            callback(error);
        }

        callback(null, rows);

    });
};

connection.prototype.addColumn = function(table, column, rowData, callback){
    var self = this;
    table = String(table).toUpperCase()
    column = String(column).toUpperCase()
    var overrideType = ""
    if(!rowData || rowData.length === 0){
        return callback(new Error('No rowData provided to alter table ' + table + '.' + column + ''));
    }

    rowData.forEach(function(item){

        if(item !== undefined && item !== null){

            if(typeof item === 'boolean'){
                type = 'SMALLINT';
            }else if(typeof item === 'number'){
                if( utils.isInt(item) ){
                    type = 'NUMBER';
                }else{
                    overrideType = 'FLOAT';
                }
            }else if(item instanceof Date === true){
                type = 'DATETIME';
            }else{
                type = 'VARCHAR';
            }
        }
    });

    if(column === self.options.primaryKey){
        if(type === 'NUMBER'){
            type += ' PRIMARY KEY NOT NULL AUTOINCREMENT';
        }else{
            type += ' PRIMARY KEY';
        }
    }

  if(overrideType != "") {
    console.log('Override type is not null , assigning type to overRide type Value')
      type = overrideType
  }


    query = 'ALTER TABLE ' + table + ' ADD ' + '\"' + column + '\"' + ' ' + type;
    self.query(query, callback);

};

connection.prototype.alterColumn = function(table, column, definition, callback){
    var self = this;
    table = String(table).toUpperCase()
    column = String(column).toUpperCase()
    var query = ' ALTER TABLE ' + table + ' MODIFY ' + column + ' ' + definition + '; '
    self.book.log('Alter Table ' + table + ', ' + column + ', ' + definition);
    self.query(query, null, callback, true);
};

connection.prototype.mergeTables = function(sourceTable, destinationTable, callback){
    var self = this;
    var queries = [];
    var fullMerge = false;
    var needsComma = false;
    var mergeKey = self.options.primaryKey;
    sourceTable = String(sourceTable).toUpperCase()
    destinationTable = String(destinationTable).toUpperCase()

    self.showTables(function(error, tables){
        self.showColumns(sourceTable, function(error, sourceColumns){
            self.showColumns(destinationTable, function(error, destinationColumns){

                var sourceKeys      = Object.keys(sourceColumns);
                var destinationKeys = Object.keys(destinationColumns);

                if(tables.indexOf(sourceTable) < 0){
                    return callback(new Error('sourceTable does not exist: ' + sourceTable));
                }else{

                    if(tables.indexOf(destinationTable) < 0){ fullMerge = true; }
                    if(sourceKeys.length > destinationKeys.length){ fullMerge = true; }
                    if(destinationKeys.indexOf(self.options.primaryKey) < 0){ fullMerge = true; }

                    if(fullMerge){
                        queries.push( 'DROP TABLE IF EXISTS ' + destinationTable + '');
                        queries.push( 'CREATE TABLE ' + destinationTable + ' LIKE ' + sourceTable + '' );
                        queries.push( 'INSERT INTO ' + destinationTable + ' ( SELECT * FROM ' + sourceTable + ' )' );
                    }else{
                        var mergeQuery = '';

                        mergeQuery += 'MERGE INTO ' + destinationTable + ' USING ( '

                        mergeQuery += 'SELECT '
                        destinationKeys.forEach(function(key){
                            if(needsComma) mergeQuery += ', ';
                            mergeQuery += sourceTable + '.' + key + ' \n'
                            needsComma = true;
                        });
                        mergeQuery += ' FROM ' + sourceTable + ' GROUP BY '
                        destinationKeys.forEach(function(key){
                            if(needsComma) mergeQuery += ', ';
                            mergeQuery += sourceTable + '.' + key + ' \n'
                            needsComma = true;
                        });

                        mergeQuery += ' ) AS ' + sourceTable + ' ON \n'
                        mergeQuery += destinationTable + '.' + mergeKey + ' = ' + sourceTable + '.' + mergeKey + ' \n'
                        mergeQuery += ' WHEN MATCHED THEN UPDATE SET \n'

                        destinationKeys.forEach(function(key){
                            if(needsComma) mergeQuery += ', ';
                            mergeQuery += destinationTable + '.' + key + ' = ' + sourceTable + '.' + key + ' \n'
                            needsComma = true;
                        });

                        needsComma = false

                        mergeQuery += ' WHEN NOT MATCHED THEN INSERT ('

                        destinationKeys.forEach(function(key){
                            if(needsComma) mergeQuery += ', ';
                            mergeQuery += key
                            needsComma = true;
                        });

                        needsComma = false

                        mergeQuery += ') VALUES ('

                        destinationKeys.forEach(function(key){
                            if(needsComma) mergeQuery += ', ';
                            mergeQuery += sourceTable + '.' + key + ' \n'
                            needsComma = true;
                        });

                        mergeQuery += ')'
                        queries.push(mergeQuery);
                    }

                    self.book.log('  merging ' + sourceTable + ' => ' + destinationTable + '', 'debug');
                    self.query(queries, null, callback, true);
                }
            });
        });
    });
 };

connection.prototype.copyTableSchema = function(sourceTable, destinationTable, callback){
    var self = this;
    var query = 'CREATE TABLE ' + destinationTable + ' LIKE ' + sourceTable + ''
    self.query(query, function(error, rows) {

        if(error) {
            callback(error);
        }
        callback(error, rows)
    });


};

exports.connection = connection;
