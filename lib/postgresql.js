/**
 * 
 * ©2016-2018 EdgeVerve Systems Limited (a fully owned Infosys subsidiary),
 * Bangalore, India. All Rights Reserved.
 * 
 */

/*!
 * PostgreSQL connector for LoopBack
 */
'use strict';
var SG = require('strong-globalize');
var g = SG();
var postgresql = require('pg');
var SqlConnector = require('loopback-connector').SqlConnector;
var ParameterizedSQL = SqlConnector.ParameterizedSQL;
var util = require('util');
var debug = require('debug')('loopback:connector:postgresql');
var Promise = require('bluebird');
var EventEmitter = require('events').EventEmitter;
var async = require('async');

/**
 *
 * Initialize the PostgreSQL connector against the given data source
 *
 * @param {DataSource} dataSource The loopback-datasource-juggler dataSource
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @header PostgreSQL.initialize(dataSource, [callback])
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!postgresql) {
    return;
  }
  postgresql.defaults.parseInt8 = true;
  var dbSettings = dataSource.settings || {};
  dbSettings.host = dbSettings.host || dbSettings.hostname || 'localhost';
  dbSettings.user = dbSettings.user || dbSettings.username;
  dbSettings.debug = dbSettings.debug || debug.enabled;

  dataSource.connector = new PostgreSQL(postgresql, dbSettings);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    if (dbSettings.lazyConnect) {
      process.nextTick(function () {
        callback();
      });
    } else {
      dataSource.connecting = true;
      dataSource.connector.connect(callback);
    }
  }
};

/**
 * PostgreSQL connector constructor
 *
 * @param {PostgreSQL} postgresql PostgreSQL node.js binding
 * @options {Object} settings An object for the data source settings.
 * See [node-postgres documentation](https://github.com/brianc/node-postgres/wiki/Client#parameters).
 * @property {String} url URL to the database, such as 'postgres://test:mypassword@localhost:5432/devdb'.
 * Other parameters can be defined as query string of the url
 * @property {String} hostname The host name or ip address of the PostgreSQL DB server
 * @property {Number} port The port number of the PostgreSQL DB Server
 * @property {String} user The user name
 * @property {String} password The password
 * @property {String} database The database name
 * @property {Boolean} ssl Whether to try SSL/TLS to connect to server
 *
 * @constructor
 */
function PostgreSQL(postgresql, settings) {
  // this.name = 'postgresql';
  // this._models = {};
  // this.settings = settings;
  this.constructor.super_.call(this, 'postgresql', settings);
  if (settings.url) {
    // pg-pool doesn't handle string config correctly
    this.clientConfig = {
      connectionString: settings.url,
    };
  } else {
    this.clientConfig = settings;
  }
  this.clientConfig.Promise = Promise;
  this.clientConfig.max = settings.max;
  this.pg = new postgresql.Pool(this.clientConfig);
  this.settings = settings;
  if (settings.debug) {
    debug('Settings %j', settings);
  }
  this.emitter = new EventEmitter();
}

// Inherit from loopback-datasource-juggler BaseSQL
util.inherits(PostgreSQL, SqlConnector);

PostgreSQL.prototype.debug = function () {
  if (this.settings.debug) {
    debug.apply(debug, arguments);
  }
};

PostgreSQL.prototype.getDefaultSchemaName = function () {
  return 'public';
};


function uuid(id) {
  return id;
}

PostgreSQL.prototype.getDefaultIdType = function () {
  return String;
};

/**
 * Connect to PostgreSQL
 * @callback {Function} [callback] The callback after the connection is established
 */
PostgreSQL.prototype.connect = function (callback) {
  var self = this;
  self.pg.connect(function (err, client, done) {
    self.client = client;
    process.nextTick(done);
    callback && checkDbExistenceAndCreate(err, client, self.settings, function (err, client) {
      if (err) {
        return callback(err, null);
      }
      var triggersObj = self.settings.triggers;
      var triggers = [];
      for (var t in triggersObj) {
        if (triggersObj.hasOwnProperty(t)) {
          triggers.push(triggersObj[t]);
        }
      }
      async.each(triggers, function (t, cb) {
        client.query(t, [], function (err, result) {
          return cb(err);
        });
      }, function (err) {
        if (err) {
          return callback(err, null);
        }
        return callback(null, client);
      });
    });
  });

  self.pg.on("error", function (err) {
    debug("Error in connecting to DB, ", err);
  });
};

/**
 * Check the error code that returns database non existence and creates the same.
 * @err {Object} [err] Error Object
 * @client {Object} [client] Client connection object, if DB existence error comes up,  comes as null.
 * @settings {Object} [settings] Database connection settings
 * @callback {Function} [callback] The callback after check of DB existence and create same.
 */
function checkDbExistenceAndCreate(err, client, settings, callback) {
  if (err && err.code === '3D000' && settings.database && (settings.enableDbCreation || settings.enableDbCreation === undefined)) { // database doesn't exists, trying to create a database.
    // Setting up the default database since a client needs to be connected to a database in order to do anything
    // including CREATE DATABASE
    var dbToCreate = settings.database;
    settings.database = 'postgres';
    var pool = new postgresql.Pool(settings);
    pool.connect(function (err, client, done) {
      client.query('CREATE DATABASE "' + dbToCreate + '"', function (err, res) {
        done();
        if (err && err.code != '42P04') {
          callback(err, client);
        } else {
          // Creating Extension "uuid-ossp" as and when the database is created.
          settings.database = dbToCreate;
          var pool2 = new postgresql.Pool(settings);
          pool2.connect(function (err, client, done) {
            client.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";', function (err, res) {
              done();
              callback(err, client);
            });
          });
        }
      });
    });
  } else {
    callback(err, client);
  }
}

//Lock class
function Lock(name) {
  if (Lock[name]) {
    return Lock[name];
  }
  this.name = name;
  this.free = true;
  Lock[name] = this;
}

Lock.prototype.acquire = function () {
  var self = this;
  if (!self.free)
    return false;
  self.free = false;

  var promise = new Promise(function (resolve, reject) {
    self.resolve = resolve;
    self.reject = reject;
  });
  self.promise = promise;
  return promise;
}

Lock.prototype.wait = function (cb) {
  var self = this;
  self.promise.then(function (data) {
    return cb(undefined, data);
  })
    .catch(function (err) {
      return cb(err);
    });
}

Lock.prototype.release = function () {
  var self = this;
  if (self.free) {
    return true;
  }
  if (!self.resolve) {
    throw new Error(self.name, ' lock not resolved');
  }
  self.free = true;
  self.resolve(undefined, true);
}


PostgreSQL.prototype.updateAndCall = function (fn, args) {
  var self = this;
  if (!self._autoupdateModels) {
    self._autoupdateModels = {};
    self._modelUpdateIds = {};
  }
  var cb = args[args.length - 1]; // also needs to handle promise
  var model = args[0];
  var m = model.toLowerCase();
  var updateId = self.getModelDefinition(model).model.updateId;
  if (self._modelUpdateIds[m] && self._modelUpdateIds[m] !== updateId) {
    self._autoupdateModels[m] = undefined;
  }
  if (process.env.ENABLE_DS_AUTOUPDATE === true || process.env.ENABLE_DS_AUTOUPDATE === 'true') {
    if (self._autoupdateModels[m] !== 'done') {
      var lock = new Lock(m);
      if (!self._autoupdateModels[m]) {
        self._autoupdateModels[m] = 'inprogress';
        lock.acquire();
        self.autoupdate(model, function (err) {
          if (err) debug("Error in creating model ", model, err);
          self._autoupdateModels[m] = 'done';
          self._modelUpdateIds[m] = updateId;
          lock.release();
          //self.emitter.emit('autoupdate-' + m, self);
          return fn.apply(self, [].slice.call(args));
        });
      }
      else {
        lock.wait(function (err) {
          return fn.apply(self, [].slice.call(args));
        });
      }
    }
    else
      return fn.apply(self, [].slice.call(args));
  } else {
    return fn.apply(self, [].slice.call(args));
  }
}

// PostgreSQL.prototype.innerall = PostgreSQL.prototype.all;
PostgreSQL.prototype.innercreate = PostgreSQL.prototype.create;
//PostgreSQL.prototype.innerreplaceOrCreate = PostgreSQL.prototype.replaceOrCreate;
PostgreSQL.prototype.innerdestroyAll = PostgreSQL.prototype.destroyAll;
PostgreSQL.prototype.innersave = PostgreSQL.prototype.save;
PostgreSQL.prototype.innerupdate = PostgreSQL.prototype.update;
PostgreSQL.prototype.innercount = PostgreSQL.prototype.count;
PostgreSQL.prototype.innerbuildSelect = PostgreSQL.prototype.buildSelect;

PostgreSQL.prototype.all = function find(model, filter, options, cb) {
  options.model = model;
  return this.updateAndCall(this.innerall, arguments);
}
PostgreSQL.prototype.create = function create(model, data, options, cb) {
  options.model = model;
  return this.updateAndCall(this.innercreate, arguments);
}
/*
PostgreSQL.prototype.replaceOrCreate = function replaceOrCreate(model, filter, options, cb) {
    if (this.innerreplaceOrCreate) {console.log("came;")} else { console.log("NNNNNNNNNNNNOTTT CAME"); }
  return this.updateAndCall(this.innerreplaceOrCreate, arguments);
}*/
PostgreSQL.prototype.destroyAll = function destroyAll(model, filter, options, cb) {
  options.model = model;
  return this.updateAndCall(this.innerdestroyAll, arguments);
}
PostgreSQL.prototype.save = function save(model, data, options, cb) {
  options.model = model;
  return this.updateAndCall(this.innersave, arguments);
}

PostgreSQL.prototype.update = function update(model, where, data, options, cb) {
  options.model = model;
  return this.updateAndCall(this.innerupdate, arguments);
}

PostgreSQL.prototype.count = function count(model, where, options, cb) {
  options.model = model;
  return this.updateAndCall(this.innercount, arguments);
}

PostgreSQL.prototype._buildFieldsForKeys = function (model, data, keys, excludeIds) {
  var props = this.getModelDefinition(model).properties;
  var fields = {
    names: [], // field names
    columnValues: [], // an array of ParameterizedSQL
    properties: [], // model properties
  };
  for (var i = 0, n = keys.length; i < n; i++) {
    var key = keys[i];
    var p = props[key];
    if (p == null) {
      // Unknown property, ignore it
      debug('Unknown property %s is skipped for model %s', key, model);
      continue;
    }

    if (excludeIds && p.id) {
      continue;
    }
    var k = this.columnEscaped(model, key);
    var v = this.toColumnValue(p, data[key]);
    if (v !== undefined) {
      var paramdata;
      fields.names.push(k);
      if (v instanceof ParameterizedSQL) {
        paramdata = v;
        fields.columnValues.push(v);
      } else {
        paramdata = new ParameterizedSQL(ParameterizedSQL.PLACEHOLDER, [v]);
        fields.columnValues.push(paramdata);
      }
      if (Array.isArray(p.type)) {
        var flag = true;
        // converting string of array from json to plsql eg. '["a", "b", "c"]' will be converted to '{"a", "b", "c"}'
        if (flag && (p.type[0].name === 'String' || p.type[0].name === 'Number')) {
          if (paramdata.params && paramdata.params[0]) {
            var s = paramdata.params[0];
            if (Array.isArray(s)) {
              s = JSON.stringify(s);
            }
            s = s.replace('[', '');
            s = s.replace(/\][ ]*$/, '');
            paramdata.params[0] = '{' + s + '}';
          }
        }
        // Atul : taking care of loopback3 and loopback2 both
        else if (p.type[0].name === 'ModelConstructor' || p.type[0].name === 'Object' || (typeof p.type[0] === 'function' && this._models[p.type[0].name]) ) {
          //console.log('came');
          paramdata.params[0] = JSON.parse(paramdata.params[0]);
        }
      }
      fields.properties.push(p);
    }
  }
  return fields;
}

PostgreSQL.prototype.buildInsert = function (model, data, options) {
  var fields = this.buildFields(model, data);
  var insertStmt = this.buildInsertInto(model, fields, options);
  var columnValues = fields.columnValues;
  var fieldNames = fields.names;
  if (fieldNames.length) {
    for (var i = 0, m = fieldNames.length; i < m; ++i) {
      this.debug('came');
    }
    var values = ParameterizedSQL.join(columnValues, ',');
    values.sql = 'VALUES(' + values.sql + ')';
    insertStmt.merge(values);
  } else {
    insertStmt.merge(this.buildInsertDefaultValues(model, data, options));
  }
  var returning = this.buildInsertReturning(model, data, options);
  if (returning) {
    insertStmt.merge(returning);
  }
  return this.parameterize(insertStmt);
};

/**
 * Execute the sql statement
 *
 * @param {String} sql The SQL statement
 * @param {String[]} params The parameter values for the SQL statement
 * @param {Object} [options] Options object
 * @callback {Function} [callback] The callback after the SQL statement is executed
 * @param {String|Error} err The error string or object
 * @param {Object[])} data The result from the SQL
 */
PostgreSQL.prototype.executeSQL = function (sql, params, options, callback) {
  var self = this;

  if (self.settings.debug) {
    if (params && params.length > 0) {
      self.debug('SQL: %s\nParameters: %j', sql, params);
    } else {
      self.debug('SQL: %s', sql);
    }
  }
  //console.log(sql);
  function executeWithConnection(connection, done) {
    connection.query(sql, params, function (err, data) {
      // if(err) console.error(err);
      if (err && self.settings.debug) {
        self.debug(err);
      }
      if (self.settings.debug && data) self.debug('%j', data);
      if (done) {
        process.nextTick(function () {
          // Release the connection in next tick
          done(err);
        });
      }
      var result = null;
      if (data) {
        switch (data.command) {
          case 'DELETE':
          case 'UPDATE':
            result = { count: data.rowCount };
            break;
          default:
            result = data.rows;
        }
      }
      callback(err ? err : null, result);
    });
  }

  var transaction = options.transaction;
  if (transaction && transaction.connection &&
    transaction.connector === this) {
    debug('Execute SQL within a transaction');
    // Do not release the connection
    executeWithConnection(transaction.connection, null);
  } else {
    self.pg.connect(function (err, connection, done) {
      if (err) return callback(err);
      executeWithConnection(connection, done);
    });
  }
};

PostgreSQL.prototype.buildInsertReturning = function (model, data, options) {
  var idColumnNames = [];
  var idNames = this.idNames(model);
  for (var i = 0, n = idNames.length; i < n; i++) {
    idColumnNames.push(this.columnEscaped(model, idNames[i]));
  }
  return 'RETURNING ' + idColumnNames.join(',');
};

PostgreSQL.prototype.buildInsertDefaultValues = function (model, data, options) {
  return 'DEFAULT VALUES';
};

// FIXME: [rfeng] The native implementation of upsert only works with
// postgresql 9.1 or later as it requres writable CTE
// See https://github.com/strongloop/loopback-connector-postgresql/issues/27
/**
 * Update if the model instance exists with the same id or create a new instance
 *
 * @param {String} model The model name
 * @param {Object} data The model instance data
 * @callback {Function} [callback] The callback function
 * @param {String|Error} err The error string or object
 * @param {Object} The updated model instance
 */
/*
 PostgreSQL.prototype.updateOrCreate = function (model, data, callback) {
 var self = this;
 data = self.mapToDB(model, data);
 var props = self._categorizeProperties(model, data);
 var idColumns = props.ids.map(function(key) {
 return self.columnEscaped(model, key); }
 );
 var nonIdsInData = props.nonIdsInData;
 var query = [];
 query.push('WITH update_outcome AS (UPDATE ', self.tableEscaped(model), ' SET ');
 query.push(self.toFields(model, data, false));
 query.push(' WHERE ');
 query.push(idColumns.map(function (key, i) {
 return ((i > 0) ? ' AND ' : ' ') + key + '=$' + (nonIdsInData.length + i + 1);
 }).join(','));
 query.push(' RETURNING ', idColumns.join(','), ')');
 query.push(', insert_outcome AS (INSERT INTO ', self.tableEscaped(model), ' ');
 query.push(self.toFields(model, data, true));
 query.push(' WHERE NOT EXISTS (SELECT * FROM update_outcome) RETURNING ', idColumns.join(','), ')');
 query.push(' SELECT * FROM update_outcome UNION ALL SELECT * FROM insert_outcome');
 var queryParams = [];
 nonIdsInData.forEach(function(key) {
 queryParams.push(data[key]);
 });
 props.ids.forEach(function(key) {
 queryParams.push(data[key] || null);
 });
 var idColName = self.idColumn(model);
 self.query(query.join(''), queryParams, function(err, info) {
 if (err) {
 return callback(err);
 }
 var idValue = null;
 if (info && info[0]) {
 idValue = info[0][idColName];
 }
 callback(err, idValue);
 });
 };
 */

PostgreSQL.prototype.fromColumnValue = function (prop, val) {
  if (val == null) {
    return val;
  }
  var type = prop.type && prop.type.name;
  if (prop && type === 'Boolean') {
    if (typeof val === 'boolean') {
      return val;
    } else {
      return (val === 'Y' || val === 'y' || val === 'T' ||
        val === 't' || val === '1');
    }
  } else if (prop && type === 'GeoPoint' || type === 'Point') {
    if (typeof val === 'string') {
      // The point format is (x,y)
      var point = val.split(/[\(\)\s,]+/).filter(Boolean);
      return {
        lat: +point[0],
        lng: +point[1],
      };
    } else if (typeof val === 'object' && val !== null) {
      // Now pg driver converts point to {x: lng, y: lat}
      return {
        lng: val.x,
        lat: val.y,
      };
    } else {
      return val;
    }
  } else {
    return val;
  }
};

/*!
 * Convert to the Database name
 * @param {String} name The name
 * @returns {String} The converted name
 */
PostgreSQL.prototype.dbName = function (name) {
  if (!name) {
    return name;
  }
  // PostgreSQL default to lowercase names
  return name.toLowerCase();
};

function escapeIdentifier(str) {
  var escaped = '"';
  for (var i = 0; i < str.length; i++) {
    var c = str[i];
    if (c === '"') {
      escaped += c + c;
    } else {
      escaped += c;
    }
  }
  escaped += '"';
  return escaped;
}

function escapeLiteral(str) {
  var hasBackslash = false;
  var escaped = '\'';
  for (var i = 0; i < str.length; i++) {
    var c = str[i];
    if (c === '\'') {
      escaped += c + c;
    } else if (c === '\\') {
      escaped += c + c;
      hasBackslash = true;
    } else {
      escaped += c;
    }
  }
  escaped += '\'';
  if (hasBackslash === true) {
    escaped = ' E' + escaped;
  }
  return escaped;
}

/*!
 * Escape the name for PostgreSQL DB
 * @param {String} name The name
 * @returns {String} The escaped name
 */
PostgreSQL.prototype.escapeName = function (name) {
  if (!name) {
    return name;
  }
  return escapeIdentifier(name);
};

PostgreSQL.prototype.escapeValue = function (value) {
  if (typeof value === 'string') {
    return escapeLiteral(value);
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }
  // Can't send functions, objects, arrays
  if (typeof value === 'object' || typeof value === 'function') {
    return null;
  }
  return value;
};

PostgreSQL.prototype.tableEscaped = function (model) {
  var schema = this.schema(model) || 'public';
  return this.escapeName(schema) + '.' +
    this.escapeName(this.table(model));
};

function buildLimit(limit, offset) {
  var clause = [];
  if (isNaN(limit)) {
    limit = 0;
  }
  if (isNaN(offset)) {
    offset = 0;
  }
  if (!limit && !offset) {
    return '';
  }
  if (limit) {
    clause.push('LIMIT ' + limit);
  }
  if (offset) {
    clause.push('OFFSET ' + offset);
  }
  return clause.join(' ');
}

PostgreSQL.prototype.applyPagination = function (model, stmt, filter) {
  var limitClause = buildLimit(filter.limit, filter.offset || filter.skip);
  return stmt.merge(limitClause);
};

PostgreSQL.prototype._getActualProperty = function (model, propertyName) {
  var self = this;
  var props = self.getModelDefinition(model).properties;
  var p = props[key];
  var key2 = propertyName, key = propertyName;
  var keyPath = key;
  if (key.indexOf('.') > 0) {
    key = key2.split('.')[0];
    keyPath = key.split('.');
  }
  var p = props[key];
  if (p == null) {
    // Unknown property, ignore it
    debug('Unknown property %s is skipped for model %s', key, model);
    return null;
  }
  var stillModel = true;
  var currentProperty;
  /* eslint-disable one-var */
  var columnName = self.columnEscaped(model, key);

  var currentProperty = p;
  if (key !== key2) {
    var currentProperty = this._models[model].properties[key]; //.type.getPropertyType("name")
    var elements = key2.split('.');
    for (var i = 1, n = elements.length; i < n; ++i) {
      var temp = elements[i];
      if (stillModel && currentProperty.type && this._models[currentProperty.type.modelName]) {
        currentProperty = this._models[currentProperty.type.modelName].properties[temp];
        if (!(currentProperty && currentProperty.type && (this._models[currentProperty.type.name] || currentProperty.type.name === 'ModelConstructor'))) {
          stillModel = false;
        }
      }
      else {
        stillModel = false;
      }
    }
  }
  return currentProperty;
};

PostgreSQL.prototype._buildWhere = function (model, where) {
  if (!where) {
    return new ParameterizedSQL('');
  }
  if (typeof where !== 'object' || Array.isArray(where)) {
    debug('Invalid value for where: %j', where);
    return new ParameterizedSQL('');
  }
  var self = this;
  var props = self.getModelDefinition(model).properties;

  var whereStmts = [];
  for (var key in where) {
    var stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      var branches = [];
      var branchParams = [];
      var clauses = where[key];
      if (Array.isArray(clauses)) {
        for (var i = 0, n = clauses.length; i < n; i++) {
          var stmtForClause = self._buildWhere(model, clauses[i]);
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: branches.join(' ' + key.toUpperCase() + ' '),
          params: branchParams,
        });
        whereStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }

    var key2 = key;
    if (key.indexOf('.') > 0) {
      key = key2.split('.')[0];
    }
    var p = self._getActualProperty(model, key2);//props[key];
    if (p == null) {
      // Unknown property, ignore it
      debug('Unknown property %s is skipped for model %s', key, model);
      stmt.merge({
        sql: key2 + '= ?',
        params: [where[key2]],
      });
      whereStmts.push(stmt);
      continue;
    }
    /* eslint-disable one-var */
    var columnName = self.columnEscaped(model, key);
    if (key !== key2) {
      var columnPath = columnName;
      var elements = key2.split('.');
      for (var i = 1, n = elements.length; i < n - 1; ++i) {
        columnPath += "->'" + elements[i] + "'";
      }
      columnPath += "->>'" + elements[elements.length - 1] + "'";
      columnName = columnPath;
    }
    var expression = where[key2];
    var columnValue;
    var sqlExp;
    /* eslint-enable one-var */
    if (expression === null || expression === undefined) {
      stmt.merge(columnName + ' IS NULL');
    } else if (expression && (expression.constructor === Object || expression.constructor === RegExp)) {
      var operator = Object.keys(expression)[0];
      // Get the expression without the operator
      if (operator === 'like' || operator === 'nlike' || operator === 'ilike' || operator === 'nilike') {
        this._expression = expression = new RegExp(expression[operator]);
      } else if (expression.constructor === RegExp) {
        operator = 'regexp';
      } else {
        expression = expression[operator];
      }
      if (operator === 'inq' || operator === 'nin' || operator === 'between') {
        columnValue = [];
        if (Array.isArray(expression)) {
          // Column value is a list
          for (var j = 0, m = expression.length; j < m; j++) {
            columnValue.push(this.toColumnValue(p, expression[j]));
          }
        } else {
          columnValue.push(this.toColumnValue(p, expression));
        }
        if (operator === 'between') {
          // BETWEEN v1 AND v2
          var v1 = columnValue[0] === undefined ? null : columnValue[0];
          var v2 = columnValue[1] === undefined ? null : columnValue[1];
          columnValue = [v1, v2];
        } else {
          // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
          if (columnValue.length === 0) {
            if (operator === 'inq') {
              columnValue = [null];
            } else {
              // nin () is true
              continue;
            }
          }
        }
      } else if ((operator === 'regexp' || operator === 'like' || operator === 'nlike'
        || operator === 'ilike' || operator === 'nilike') && expression instanceof RegExp) {
        // do not coerce RegExp based on property definitions
        columnValue = expression;
      } else {
        columnValue = this.toColumnValue(p, expression);
      }
      sqlExp = self.buildExpression(
        columnName, operator, columnValue, p);
      stmt.merge(sqlExp);
    } else {
      // The expression is the field value, not a condition
      columnValue = self.toColumnValue(p, expression);
      if (columnValue === null) {
        stmt.merge(columnName + ' IS NULL');
      } else {
        if (columnValue instanceof ParameterizedSQL) {
          stmt.merge(columnName + '=').merge(columnValue);
        } else {
          if (key !== key2) {
            var stringValue = JSON.stringify(columnValue);
            columnName = columnName.replace('->>', '->');
            stmt.merge({
              sql: columnName + ' @> ?::jsonb and ' + columnName + ' <@ ?::jsonb',
              params: [stringValue, stringValue],
            });
          } else {
            stmt.merge({
              sql: columnName + '=?',
              params: [columnValue],
            });
          }
        }
      }
    }
    whereStmts.push(stmt);
  }
  var params = [];
  var sqls = [];
  for (var k = 0, s = whereStmts.length; k < s; k++) {
    sqls.push(whereStmts[k].sql);
    params = params.concat(whereStmts[k].params);
  }
  var whereStmt = new ParameterizedSQL({
    sql: sqls.join(' AND '),
    params: params,
  });
  return whereStmt;
};


PostgreSQL.prototype.buildExpression = function (columnName, operator,
  operatorValue, propertyDefinition) {
  switch (operator) {
    case 'like':
      this._notLike = false;
      this._ignoreCase = false;
      return this.buildExpression(columnName, 'regexp', operatorValue, propertyDefinition);
    case 'nlike':
      this._notLike = true;
      this._ignoreCase = false;
      return this.buildExpression(columnName, 'regexp', operatorValue, propertyDefinition);
    case 'ilike':
      this._notLike = false;
      this._ignoreCase = true;
      return this.buildExpression(columnName, 'regexp', operatorValue, propertyDefinition);
    case 'nilike':
      this._notLike = true;
      this._ignoreCase = true;
      return this.buildExpression(columnName, 'regexp', operatorValue, propertyDefinition);
    case 'regexp':
      if (operatorValue.global)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`g`}} flag');

      if (operatorValue.multiline)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`m`}} flag');

      var regexOperator = (this._ignoreCase || operatorValue.ignoreCase) ? '~* ?' : '~ ?';
      regexOperator = (this._notLike ? ' !' : ' ') + regexOperator;
      return new ParameterizedSQL(columnName + regexOperator,
        [operatorValue.source]);
    case 'contains':
      var expr = this.invokeSuper('buildExpression', columnName, 'inq',
        operatorValue, propertyDefinition);
      if (Array.isArray(propertyDefinition.type)) {
        expr.sql = expr.sql.replace(' IN ', ' <@ ARRAY ');
        expr.sql = expr.sql.replace('(', '[');
        expr.sql = expr.sql.replace(/\)[ ]*$/, ']');
        if (propertyDefinition.type[0] && propertyDefinition.type[0].name === 'Number') {
          expr.sql += ':: DOUBLE PRECISION[]';
        }
        else if (propertyDefinition.type[0] && propertyDefinition.type[0].name === 'String') {
          expr.sql += ':: text[]';
        }
        else if (propertyDefinition.type[0] && propertyDefinition.type[0].name === 'Date') {
          expr.sql += ':: TIMESTAMP WITH TIME ZONE[]';
        }
      }
      return expr;
    case 'inq':
      // invoke the base implementation of `buildExpression`
      var expr = this.invokeSuper('buildExpression', columnName, operator,
        operatorValue, propertyDefinition);
      if (Array.isArray(propertyDefinition.type) && columnName.indexOf("->") < 0) {
        expr.sql = expr.sql.replace(' IN ', ' && ARRAY ');
        expr.sql = expr.sql.replace('(', '[');
        expr.sql = expr.sql.replace(/\)[ ]*$/, ']');
        if (propertyDefinition.type[0]) {
          if (propertyDefinition.type[0].name === 'Number')
            expr.sql += '::DOUBLE PRECISION[]';
          else if (propertyDefinition.type[0].name === 'Date') {
            expr.sql += '::TIMESTAMP WITH TIME ZONE[]';
          }
          else if (propertyDefinition.type[0].name === 'Boolean') {
            expr.sql += '::BOOLEAN[]';
          }
        }
      }
      else
        if (Array.isArray(propertyDefinition.type) || (Array.isArray(operatorValue) && propertyDefinition.type.name === 'Object')) {
          // expr.sql = "'replace(replace('" + expr.sql + "'[', '{'), ']', '}')::DOUBLE PRECISION[] && ARRAY [?, ?] ::DOUBLE PRECISION[]";
          var s = expr.sql.replace(' IN ', ' && ARRAY ');
          s = s.replace('(', '[');
          s = s.replace(/\)[ ]*$/, ']');
          s = s.replace(columnName, '');
          var cast = "::text[]";
          if (Array.isArray(propertyDefinition.type)) {
            if (propertyDefinition.type[0].name === 'Number') {
              cast = '::DOUBLE PRECISION[]';
            }
            else if (propertyDefinition.type[0].name === 'Boolean') {
              cast = '::BOOLEAN[]';
            }
            else if (propertyDefinition.type[0].name === 'Date') {
              cast = '::TIMESTAMP WITH TIME ZONE[]';
            }
            else if (propertyDefinition.type[0].name === 'String') {
              cast = '::text[]';
            }
          }
          else {
            if (operatorValue[0]) {
              if (typeof operatorValue[0] === 'number') {
                cast = '::DOUBLE PRECISION[]';
              }
              else if (typeof operatorValue[0] === 'string') {
                cast = '::text[]';
              }
              else if (typeof operatorValue[0] === 'boolean') {
                cast = '::BOOLEAN[]';
              }
              else if (typeof operatorValue[0] === 'object') {
                if (operatorValue[0].constructor.name === 'Date') {
                  cast = '::TIMESTAMP WITH TIME ZONE[]';
                }
              }
            }
          }
          // converting to array and then compare
          // s = "regexp_replace(regexp_replace(" + columnName + ", '^\\[', '{'), '\\]$', '}')" + cast + s + cast ;
          s = "( '{' || regexp_replace(regexp_replace(" + columnName + ", '^\\[', ''), '\\]$', '') || '}')" + cast + s + cast;
          expr.sql = s; //"regexp_replace(regexp_replace(\"data\"->>'y', '^\\[', '{'), '\\]$', '}')::DOUBLE PRECISION[] && ARRAY [?, ?] ::DOUBLE PRECISION[]";
        }
        else if (propertyDefinition.type.name === 'Object') {
          expr.sql = expr.sql.replace(' IN ', ' @> ');
          expr.sql = expr.sql.replace('(', '\'[');
          expr.sql = expr.sql.replace(/\)[ ]*$/, ']\' :: jsonb ');
        }
        else if (propertyDefinition.type.name === 'ModelConstructor') {
          expr.sql = expr.sql.replace(' IN ', ' && ARRAY ');
          expr.sql = expr.sql.replace('(', '[');
          expr.sql = expr.sql.replace(/\)[ ]*$/, ']');
        }
      return expr;
    default:
      // invoke the base implementation of `buildExpression`
      return this.invokeSuper('buildExpression', columnName, operator,
        operatorValue, propertyDefinition);
  }
};

/**
 * Disconnect from PostgreSQL
 * @param {Function} [cb] The callback function
 */
PostgreSQL.prototype.disconnect = function disconnect(cb) {
  if (this.pg) {
    if (this.settings.debug) {
      this.debug('Disconnecting from ' + this.settings.hostname);
    }
    var pg = this.pg;
    this.pg = null;
    pg.end();  // This is sync
  }

  if (cb) {
    process.nextTick(cb);
  }
};

PostgreSQL.prototype.ping = function (cb) {
  this.execute('SELECT 1 AS result', [], cb);
};

PostgreSQL.prototype.getInsertedId = function (model, info) {
  var idColName = this.idColumn(model);
  var idValue;
  if (info && info[0]) {
    idValue = info[0][idColName];
  }
  return idValue;
};

/*!
 * Convert property name/value to an escaped DB column value
 * @param {Object} prop Property descriptor
 * @param {*} val Property value
 * @returns {*} The escaped value of DB column
 */
PostgreSQL.prototype.toColumnValue = function (prop, val) {
  if (val == null) {
    // PostgreSQL complains with NULLs in not null columns
    // If we have an autoincrement value, return DEFAULT instead
    if (prop.autoIncrement || prop.id) {
      return new ParameterizedSQL('DEFAULT');
    } else {
      return null;
    }
  }
  if (prop.type === String) {
    return String(val);
  }
  if (prop.type === Number) {
    if (isNaN(val)) {
      // Map NaN to NULL
      return val;
    }
    return val;
  }

  if (prop.type === Date || prop.type.name === 'Timestamp') {
    if (!val.toISOString) {
      val = new Date(val);
    }
    var iso = val.toISOString();

    // Pass in date as UTC and make sure Postgresql stores using UTC timezone
    return new ParameterizedSQL({
      sql: '?',
      params: [iso],
    });
  }

  // PostgreSQL support char(1) Y/N
  if (prop.type === Boolean) {
    if (val) {
      return true;
    } else {
      return false;
    }
  }

  if (prop.type.name === 'GeoPoint' || prop.type.name === 'Point') {
    return new ParameterizedSQL({
      sql: 'point(?,?)',
      // Postgres point is point(lng, lat)
      params: [val.lng, val.lat],
    });
  }

  return val;
};

/**
 * Get the place holder in SQL for identifiers, such as ??
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForIdentifier = function (key) {
  throw new Error(g.f('{{Placeholder}} for identifiers is not supported'));
};

/**
 * Get the place holder in SQL for values, such as :1 or ?
 * @param {String} key Optional key, such as 1 or id
 * @returns {String} The place holder
 */
PostgreSQL.prototype.getPlaceholderForValue = function (key) {
  return '$' + key;
};

PostgreSQL.prototype.getCountForAffectedRows = function (model, info) {
  return info && info.count;
};

/**
 * Build the ORDER BY clause
 * @param {string} model Model name
 * @param {string[]} order An array of sorting criteria
 * @returns {string} The ORDER BY clause
 */
PostgreSQL.prototype.buildOrderBy = function (model, order) {
  if (!order) {
    return '';
  }
  var self = this;
  if (typeof order === 'string') {
    order = [order];
  }
  var clauses = [];
  var cols = this.getModelDefinition(model).properties;
  for (var i = 0, n = order.length; i < n; i++) {
    var t = order[i].split(/[\s,]+/);
    if (cols[t[0]]) {
      if (t.length === 1) {
        clauses.push(self.columnEscaped(model, order[i]));
      } else {
        clauses.push(self.columnEscaped(model, t[0]) + ' ' + t[1]);
      }
    } else if (t[0].indexOf('.') !== -1) {
      var key1 = t[0].split('.')[0];
      var key2 = t[0].split('.')[1];
      if (cols[key1].type.name === 'Object') {
        clauses.push(key1 + "->> '" + key2 + "' " + t[1]);
      } else if (cols[key1].type.name === 'ModelConstructor' || (typeof cols[key1].type === 'function' && this._models[cols[key1].type.name]) ) {
        if (cols[key1].type.definition.properties[key2].type.name === 'Number') {
          clauses.push("cast(" + key1 + "->> '" + key2 + "'as integer) " + t[1]);
        } else if (cols[key1].type.definition.properties[key2].type.name === 'Date') {
          clauses.push("cast(" + key1 + "->> '" + key2 + "'as date) " + t[1]);
        } else {
          clauses.push(key1 + "->> '" + key2 + "' " + t[1]);
        }
      }
    }
  }
  return 'ORDER BY ' + clauses.join(',');
};

/**
 * Build a list of escaped column names for the given model and fields filter
 * @param {string} model Model name
 * @param {object} filter The filter object
 * @returns {string} Comma separated string of escaped column names
 */
PostgreSQL.prototype.buildColumnNames = function (model, filter) {
  var fieldsFilter = filter && filter.fields;
  var cols = this.getModelDefinition(model).properties;
  if (!cols) {
    return '*';
  }
  var self = this;
  var keys = Object.keys(cols);
  var objectKeys = {};
  var objectFilterKeys = [];
  var filterObjectTypeFields = function (fieldFilter) {
    fieldFilter.forEach(function (item) {
      if (item.indexOf('.') !== -1 && typeof cols[item.split('.')[0]] === 'object') {
        var key1 = self.columnEscaped(model, item.split('.')[0]);
        var key2 = item.split('.')[1];
        objectKeys[key1] = objectKeys[key1] || [];
        objectKeys[key1].push(key2);
      }
    });
    Object.keys(objectKeys).forEach(function (key) {
      var value = objectKeys[key];
      var propStr = [];
      value.forEach(function (prop) {
        propStr.push("'" + prop + "'," + key + " -> '" + prop + "'");
      });
      propStr = propStr.join(',');
      objectFilterKeys.push("json_build_object(" + propStr + ") as " + key);
    });
  }
  if (Array.isArray(fieldsFilter) && fieldsFilter.length > 0) {
    // Not empty array, including all the fields that are valid properties
    keys = fieldsFilter.filter(function (f) {
      return cols[f];
    });
    filterObjectTypeFields(fieldsFilter);
  } else if ('object' === typeof fieldsFilter &&
    Object.keys(fieldsFilter).length > 0) {
    // { field1: boolean, field2: boolean ... }
    var included = [];
    var excluded = [];
    keys.forEach(function (k) {
      if (fieldsFilter[k]) {
        included.push(k);
      } else if ((k in fieldsFilter) && !fieldsFilter[k]) {
        excluded.push(k);
      }
    });
    if (included.length > 0) {
      keys = included;
    } else if (excluded.length > 0) {
      excluded.forEach(function (e) {
        var index = keys.indexOf(e);
        keys.splice(index, 1);
      });
    }

    var includedObjects = [];
    var excludedObjects = [];
    Object.keys(fieldsFilter).forEach(function (key) {
      if (key.indexOf('.') !== -1 && typeof cols[key.split('.')[0]] === 'object') {
        if (fieldsFilter[key]) {
          includedObjects.push(key);
        } else if ((key in fieldsFilter) && !fieldsFilter[key]) {
          excludedObjects.push(key);
        }
      }
    });
    if (includedObjects.length > 0) {
      filterObjectTypeFields(includedObjects);
    } else if (excludedObjects.length > 0) {
      excludedObjects.forEach(function (item) {
        if (item.indexOf('.') !== -1 && typeof cols[item.split('.')[0]] === 'object') {
          var key1 = self.columnEscaped(model, item.split('.')[0]);
          var key2 = item.split('.')[1];
          objectKeys[key1] = objectKeys[key1] || [];
          objectKeys[key1].push(key2);
        }
      });
      Object.keys(objectKeys).forEach(function (key) {
        var value = objectKeys[key];
        var propStr = [];
        value.forEach(function (prop) {
          propStr.push(" '" + prop + "' ");
        });
        propStr = propStr.join('-');
        objectFilterKeys.push(key + " - " + propStr + " as " + key);
      });
    }
    if (included.length === 0 && excluded.length === 0 && includedObjects.length > 0) {
      keys = [];
    }
  }
  var names = keys.map(function (c) {
    return self.columnEscaped(model, c);
  });
  var names = names.join(',');
  if (objectFilterKeys.length) {
    var objectFilterKeys = objectFilterKeys.join(',');
    names = names ? names + "," + objectFilterKeys : objectFilterKeys;
  }
  return names;
};
/**
 * Build the SQL WHERE clause for the where object
 * @param {string} model Model name
 * @param {object} where An object for the where conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
PostgreSQL.prototype.buildGroupBy = function (model, group) {
  var groupByClause = this._buildGroupBy(model, group);
  if (groupByClause.sql) {
    groupByClause.sql = 'GROUP BY ' + groupByClause.sql;
  }
  return groupByClause;
};

PostgreSQL.prototype._buildGroupBy = function (model, group) {
  if (!group) {
    return new ParameterizedSQL('');
  }
  if (typeof group !== 'object' || Array.isArray(group)) {
    debug('Invalid value for group: %j', group);
    return new ParameterizedSQL('');
  }
  var self = this;
  var props = self.getModelDefinition(model).properties;

  var groupByStmts = [];
  for (var key in group) {
    if (key === 'groupBy') {
      var groupBy = group[key];
      if (Array.isArray(groupBy)) {
        groupBy.forEach(function (col) {
          var columnName = self.columnEscaped(model, col);
          groupByStmts.push(columnName);
        });
      } else {
        debug('Invalid value for groupBy: %j', group);
        return new ParameterizedSQL('');
      }
    }
  }
  return new ParameterizedSQL(groupByStmts.join(','));
};

/**
 * Build a SQL SELECT statement
 * @param {String} model Model name
 * @param {Object} filter Filter object
 * @param {Object} options Options object
 * @returns {ParameterizedSQL} Statement object {sql: ..., params: [...]}
 */
PostgreSQL.prototype.buildSelect = function (model, filter, options) {
  if (!filter.order) {
    var idNames = this.idNames(model);
    if (idNames && idNames.length) {
      filter.order = idNames;
    }
  }
  if (filter && filter.group) {
    var groupFields = filter.group.groupBy ? filter.group.groupBy : [];
    var filterFields = filter.fields || {};
    var fields;
    if (Array.isArray(filterFields) && filterFields.length > 0) {
      fields = [];
      filterFields.forEach(function (prop) {
        if (groupFields.indexOf(prop) !== -1) {
          fields.push(prop);
        }
      });
      filter.fields = fields;
    } else if (typeof filterFields === 'object' && Object.keys(filterFields).length > 0) {
      var included = [];
      var excluded = [];
      var keys = Object.keys(filterFields);
      keys.forEach(function (k) {
        if (filterFields[k]) {
          included.push(k);
        } else if ((k in filterFields) && !filterFields[k]) {
          excluded.push(k);
        }
      });
      if (included.length > 0) {
        fields = included;
      } else if (excluded.length > 0) {
        excluded.forEach(function (e) {
          var index = keys.indexOf(e);
          fields.splice(index, 1);
        });
      }
      filter.fields = fields;
    } else if (filter.group && filter.group.groupBy) {
      filter.fields = [];
      filter.group.groupBy.forEach(function (property) {
        filter.fields.push(property);
      });
    }

    if (filter.order) {
      var self = this;
      if (typeof filter.order === 'string') {
        filter.order = [filter.order];
      }
      filter.order.forEach(function (e) {
        var orderProp = e.split(/[\s,]+/);
        if (filter.group.groupBy && filter.group.groupBy.indexOf(orderProp[0]) === -1) {
          var index = filter.order.indexOf(e);
          filter.order.splice(index, 1);
        }
      });
      if (filter.order.length === 0) {
        delete filter.order;
      }
    }

  }
  var selectStmt = new ParameterizedSQL('SELECT ' +
    this.buildColumnNames(model, filter) + this.buildColumnAggregations(model, filter) +
    ' FROM ' + this.tableEscaped(model)
  );

  if (filter) {
    if (filter.where) {
      var whereStmt = this.buildWhere(model, filter.where);
      selectStmt.merge(whereStmt);
    }

    if (filter.group) {
      var groupByStmt = this.buildGroupBy(model, filter.group);
      selectStmt.merge(groupByStmt);
    }

    if (filter.group && filter.group.groupBy && filter.having) {
      var havingStmt = this.buildHavingClause(model, filter.having);
      selectStmt.merge(havingStmt);
    }

    if (filter.order) {
      selectStmt.merge(this.buildOrderBy(model, filter.order));
    }

    if (filter.selectForUpdate) {
      if (options.transaction) {
        selectStmt.merge(' for update ');
      }
      else {
        console.err('Ignoring selectForUpdate filter since not within transaction');
      }
    }

    if (filter.limit || filter.skip || filter.offset) {
      selectStmt = this.applyPagination(
        model, selectStmt, filter);
    }
  }
  return this.parameterize(selectStmt);
};

/**
 * Build the SQL WHERE clause for the where object
 * @param {string} model Model name
 * @param {object} filter An object for the filter conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
PostgreSQL.prototype.buildColumnAggregations = function (model, filter) {
  var colAggregate = this._buildColumnAggregations(model, filter);
  if (colAggregate) {
    colAggregate = ', ' + colAggregate;
    return colAggregate;
  }
  return "";
};

/**
 * Build a list of escaped column names for the given model and group filter
 * @param {string} model Model name
 * @param {object} filter The filter object
 * @returns {string} Comma separated string of escaped column names for aggregation functions
 */
PostgreSQL.prototype._buildColumnAggregations = function (model, filter) {
  var self = this;
  var aggCols = [];

  var buildColumnsForAggregation = function (obj, fn) {
    Object.keys(obj).forEach(function (col) {
      if (col.indexOf('.') !== -1) {
        var key = col.split('.')[0];
        var key2 = col.split('.')[1];
        aggCols.push(fn + '((' + self.columnEscaped(model, key) + '->> \'' + key2 + '\') :: DOUBLE PRECISION) AS "' + obj[col] + '"');
      } else {
        aggCols.push(fn + '(' + self.columnEscaped(model, col) + ') AS "' + obj[col] + '"');
      }
    });
    return;
  };
  if (filter && filter.group) {
    var aggregations = Object.keys(filter.group);
    aggregations.forEach(function (agFn) {
      var aggregationFnName = agFn.toUpperCase();
      var fnTypes = ['MIN', 'MAX', 'COUNT', 'AVG', 'SUM'];
      if (fnTypes.indexOf(aggregationFnName) !== -1) {
        buildColumnsForAggregation(filter.group[agFn], aggregationFnName);
      }
    });
  }
  aggCols = aggCols.join(',');
  return aggCols;
};

/**
 * Transform the row data into a model data object
 * @param {string} model Model name
 * @param {object} rowData An object representing the row data from DB
 * @returns {object} Model data object
 */
PostgreSQL.prototype.fromRow = PostgreSQL.prototype.fromDatabase =
  function (model, rowData) {
    if (rowData == null) {
      return rowData;
    }
    var props = this.getModelDefinition(model).properties;
    var data = Object.assign({}, rowData);
    for (var p in props) {
      var columnName = this.column(model, p);
      // Load properties from the row
      var columnValue = this.fromColumnValue(props[p], rowData[columnName]);
      if (columnValue !== undefined && data[p]) {
        data[p] = columnValue;
      } else if (columnValue !== undefined && !data[p]) {
        data[p] = columnValue;
        delete data[columnName];
      }
    }
    return data;
  };

/**
 * Find matching model instances by the filter
 *
 * Please also note the name `all` is confusing. `Model.find` is to find all
 * matching instances while `Model.findById` is to find an instance by id. On
 * the other hand, `Connector.prototype.all` implements `Model.find` while
 * `Connector.prototype.find` implements `Model.findById` due to the `bad`
 * naming convention we inherited from juggling-db.
 *
 * @param {String} model The model name
 * @param {Object} filter The filter
 * @param {Function} [cb] The cb function
 */
PostgreSQL.prototype.innerall = function find(model, filter, options, cb) {
  var self = this;
  // Order by id if no order is specified
  filter = filter || {};
  var stmt = this.buildSelect(model, filter, options);
  this.execute(stmt.sql, stmt.params, options, function (err, data) {
    if (err) {
      return cb(err, []);
    }
    if (filter.group) {
      Object.keys(filter.group).forEach(function (key) {
        key = key.toLowerCase();
        if (key !== 'groupby') {
          var val = filter.group[key];
          Object.keys(val).forEach(function (elem) {
            filter.fields.push(val[elem]);
          });
        }
      });
    }
    var objs = data.map(function (obj) {
      return self.fromRow(model, obj);
    });
    if (filter && filter.include) {
      self.getModelDefinition(model).model.include(
        objs, filter.include, options, cb);
    } else {
      cb(null, objs);
    }
  });
};

/**
 * Build the SQL WHERE clause for the where object
 * @param {string} model Model name
 * @param {object} filter An object for the filter conditions
 * @returns {ParameterizedSQL} The SQL WHERE clause
 */
PostgreSQL.prototype.buildHavingClause = function (model, filter) {
  var havingClause = this._buildHavingClause(model, filter);
  if (havingClause.sql) {
    havingClause.sql = 'HAVING ' + havingClause.sql;
    return havingClause;
  }
  return "";
};


/*!
 * @param model
 * @param where
 * @returns {ParameterizedSQL}
 * @private
 */
PostgreSQL.prototype._buildHavingClause = function (model, having) {
  var columnValue;
  var sqlExp;
  if (!having) {
    return new ParameterizedSQL('');
  }
  if (typeof having !== 'object' || Array.isArray(having)) {
    debug('Invalid value for having: %j', having);
    return new ParameterizedSQL('');
  }
  var self = this;
  var props = self.getModelDefinition(model).properties;

  var havingStmts = [];
  for (var key in having) {
    var aggregationFn = key.toUpperCase();
    var stmt = new ParameterizedSQL('', []);
    // Handle and/or operators
    if (key === 'and' || key === 'or') {
      var branches = [];
      var branchParams = [];
      var clauses = having[key];
      if (Array.isArray(clauses)) {
        for (var i = 0, n = clauses.length; i < n; i++) {
          var stmtForClause = self._buildHavingClause(model, clauses[i]);
          if (stmtForClause.sql) {
            stmtForClause.sql = '(' + stmtForClause.sql + ')';
            branchParams = branchParams.concat(stmtForClause.params);
            branches.push(stmtForClause.sql);
          }
        }
        stmt.merge({
          sql: branches.join(' ' + key.toUpperCase() + ' '),
          params: branchParams,
        });
        havingStmts.push(stmt);
        continue;
      }
      // The value is not an array, fall back to regular fields
    }
    var aggregation = having[key];
    for (var innerKey in aggregation) {
      var p = props[innerKey];
      if (p == null) {
        // Unknown property, ignore it
        debug('Unknown property %s is skipped for model %s', innerKey, model);
        continue;
      }
      var expression = aggregation[innerKey];
      var columnName = self.columnEscaped(model, innerKey);
      var innerKey2 = innerKey;
      if (innerKey.indexOf('.') > 0) {
        innerKey = innerKey2.split('.')[0];
      }
      if (innerKey !== innerKey2) {
        var columnPath = columnName;
        var elements = innerKey2.split('.');
        for (var i = 1, n = elements.length; i < n - 1; ++i) {
          columnPath += "->'" + elements[i] + "'";
        }
        columnPath += "->>'" + elements[elements.length - 1] + "'";
        columnName = columnPath;
      }

      // eslint-enable one-var
      if (expression === null || expression === undefined) {
        stmt.merge(columnName + ' IS NULL');
      } else if (expression && expression.constructor === Object) {
        var operator = Object.keys(expression)[0];
        // Get the expression without the operator
        expression = expression[operator];
        if (operator === 'inq' || operator === 'nin' || operator === 'between') {
          columnValue = [];
          if (Array.isArray(expression)) {
            // Column value is a list
            for (var j = 0, m = expression.length; j < m; j++) {
              columnValue.push(this.toColumnValue(p, expression[j]));
            }
          } else {
            columnValue.push(this.toColumnValue(p, expression));
          }
          if (operator === 'between') {
            // BETWEEN v1 AND v2
            var v1 = columnValue[0] === undefined ? null : columnValue[0];
            var v2 = columnValue[1] === undefined ? null : columnValue[1];
            columnValue = [v1, v2];
          } else {
            // IN (v1,v2,v3) or NOT IN (v1,v2,v3)
            if (columnValue.length === 0) {
              if (operator === 'inq') {
                columnValue = [null];
              } else {
                // nin () is true
                continue;
              }
            }
          }
        } else if (operator === 'regexp' && expression instanceof RegExp) {
          // do not coerce RegExp based on property definitions
          columnValue = expression;
        } else {
          columnValue = this.toColumnValue(p, expression);
        }
        sqlExp = self.buildAggregationExpression(aggregationFn, columnName, operator, columnValue, p);
        stmt.merge(sqlExp);
      } else {
        // The expression is the field value, not a condition
        columnValue = self.toColumnValue(p, expression);
        if (columnValue === null) {
          stmt.merge(aggregationFn + '(' + columnName + ') IS NULL');
        } else {
          if (columnValue instanceof ParameterizedSQL) {
            if (p.type.name === 'GeoPoint')
              stmt.merge(aggregationFn + '(' + columnName + ')~=').merge(columnValue);
            else
              stmt.merge(aggregationFn + '(' + columnName + ')=').merge(columnValue);
          } else {
            stmt.merge({
              sql: aggregationFn + '(' + columnName + ')=?',
              params: [columnValue],
            });
          }
        }
      }
      havingStmts.push(stmt);
    }
  }
  var params = [];
  var sqls = [];
  for (var k = 0, s = havingStmts.length; k < s; k++) {
    sqls.push(havingStmts[k].sql);
    params = params.concat(havingStmts[k].params);
  }
  var havingStmt = new ParameterizedSQL({
    sql: sqls.join(' AND '),
    params: params,
  });
  return havingStmt;
};

/**
 * Build SQL expression
 * @param {String} aggregationFn Aggregation function name 
 * @param {String} columnName Escaped column name
 * @param {String} operator SQL operator
 * @param {*} operatorValue SQL operator value
 * @param {*} propertyDefinition Property definition
 * @returns {ParameterizedSQL} The SQL expression
 */
PostgreSQL.prototype.buildAggregationExpression = function (aggregationFn, columnName, operator,
  operatorValue, propertyDefinition) {
  switch (operator) {
    case 'like':
      this._notLike = false;
      this._ignoreCase = false;
      return this.buildAggregationExpression(aggregationFn, columnName, 'regexp', operatorValue, propertyDefinition);
    case 'nlike':
      this._notLike = true;
      this._ignoreCase = false;
      return this.buildAggregationExpression(aggregationFn, columnName, 'regexp', operatorValue, propertyDefinition);
    case 'ilike':
      this._notLike = false;
      this._ignoreCase = true;
      return this.buildAggregationExpression(aggregationFn, columnName, 'regexp', operatorValue, propertyDefinition);
    case 'nilike':
      this._notLike = true;
      this._ignoreCase = true;
      return this.buildAggregationExpression(aggregationFn, columnName, 'regexp', operatorValue, propertyDefinition);
    case 'regexp':
      if (!(operatorValue instanceof RegExp)) {
        operatorValue = new RegExp(operatorValue);
      }
      if (operatorValue.global)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`g`}} flag');

      if (operatorValue.multiline)
        g.warn('{{PostgreSQL}} regex syntax does not respect the {{`m`}} flag');

      var regexOperator = (this._ignoreCase || operatorValue.ignoreCase) ? '~* ?' : '~ ?';
      regexOperator = (this._notLike ? ' !' : ' ') + regexOperator;
      return new ParameterizedSQL(aggregationFn + '(' + columnName + ')' + regexOperator,
        [operatorValue.source]);
    case 'contains':
      var expr = this._buildAggregationExpression(aggregationFn, columnName, 'inq',
        operatorValue, propertyDefinition);
      if (Array.isArray(propertyDefinition.type)) {
        expr.sql = expr.sql.replace(' IN ', ' <@ ARRAY ');
        expr.sql = expr.sql.replace('(', '[');
        expr.sql = expr.sql.replace(/\)[ ]*$/, ']');
        if (propertyDefinition.type[0] && propertyDefinition.type[0].name === 'Number') {
          expr.sql += ':: DOUBLE PRECISION[]';
        }
        else if (propertyDefinition.type[0] && propertyDefinition.type[0].name === 'String') {
          expr.sql += ':: text[]';
        }
        else if (propertyDefinition.type[0] && propertyDefinition.type[0].name === 'Date') {
          expr.sql += ':: TIMESTAMP WITH TIME ZONE[]';
        }
      }
      return expr;
    case 'inq':
      var expr = this._buildAggregationExpression(aggregationFn, columnName, operator,
        operatorValue, propertyDefinition);
      if (Array.isArray(propertyDefinition.type) && columnName.indexOf("->") < 0) {
        expr.sql = expr.sql.replace(' IN ', ' && ARRAY ');
        expr.sql = expr.sql.replace('(', '[');
        expr.sql = expr.sql.replace(/\)[ ]*$/, ']');
        if (propertyDefinition.type[0]) {
          if (propertyDefinition.type[0].name === 'Number')
            expr.sql += '::DOUBLE PRECISION[]';
          else if (propertyDefinition.type[0].name === 'Date') {
            expr.sql += '::TIMESTAMP WITH TIME ZONE[]';
          }
          else if (propertyDefinition.type[0].name === 'Boolean') {
            expr.sql += '::BOOLEAN[]';
          }
        }
      }
      else
        if (Array.isArray(propertyDefinition.type) || (Array.isArray(operatorValue) && propertyDefinition.type.name === 'Object')) {
          // expr.sql = "'replace(replace('" + expr.sql + "'[', '{'), ']', '}')::DOUBLE PRECISION[] && ARRAY [?, ?] ::DOUBLE PRECISION[]";
          var s = expr.sql.replace(' IN ', ' && ARRAY ');
          s = s.replace('(', '[');
          s = s.replace(/\)[ ]*$/, ']');
          s = s.replace(columnName, '');
          var cast = "::text[]";
          if (Array.isArray(propertyDefinition.type)) {
            if (propertyDefinition.type[0].name === 'Number') {
              cast = '::DOUBLE PRECISION[]';
            }
            else if (propertyDefinition.type[0].name === 'Boolean') {
              cast = '::BOOLEAN[]';
            }
            else if (propertyDefinition.type[0].name === 'Date') {
              cast = '::TIMESTAMP WITH TIME ZONE[]';
            }
            else if (propertyDefinition.type[0].name === 'String') {
              cast = '::text[]';
            }
          }
          else {
            if (operatorValue[0]) {
              if (typeof operatorValue[0] === 'number') {
                cast = '::DOUBLE PRECISION[]';
              }
              else if (typeof operatorValue[0] === 'string') {
                cast = '::text[]';
              }
              else if (typeof operatorValue[0] === 'boolean') {
                cast = '::BOOLEAN[]';
              }
              else if (typeof operatorValue[0] === 'object') {
                if (operatorValue[0].constructor.name === 'Date') {
                  cast = '::TIMESTAMP WITH TIME ZONE[]';
                }
              }
            }
          }
          // converting to array and then compare
          // s = "regexp_replace(regexp_replace(" + columnName + ", '^\\[', '{'), '\\]$', '}')" + cast + s + cast ;
          s = "( '{' || regexp_replace(regexp_replace(" + columnName + ", '^\\[', ''), '\\]$', '') || '}')" + cast + s + cast;
          expr.sql = s; //"regexp_replace(regexp_replace(\"data\"->>'y', '^\\[', '{'), '\\]$', '}')::DOUBLE PRECISION[] && ARRAY [?, ?] ::DOUBLE PRECISION[]";
        }
        else if (propertyDefinition.type.name === 'Object') {
          expr.sql = expr.sql.replace(' IN ', ' @> ');
          expr.sql = expr.sql.replace('(', '\'[');
          expr.sql = expr.sql.replace(/\)[ ]*$/, ']\' :: jsonb ');
        }
        else if (propertyDefinition.type.name === 'ModelConstructor') {
          expr.sql = expr.sql.replace(' IN ', ' && ARRAY ');
          expr.sql = expr.sql.replace('(', '[');
          expr.sql = expr.sql.replace(/\)[ ]*$/, ']');
        }
      return expr;
    default:
      // invoke the base implementation of `buildExpression`
      var stmt = this._buildAggregationExpression(aggregationFn, columnName, operator,
        operatorValue, propertyDefinition);
      return stmt;
  }
};

/**
 * Build SQL expression
 * @param {String} aggregationFn Aggregation function name 
 * @param {String} columnName Escaped column name
 * @param {String} operator SQL operator
 * @param {*} columnValue Column value
 * @param {*} propertyValue Property value
 * @returns {ParameterizedSQL} The SQL expression
 */
PostgreSQL.prototype._buildAggregationExpression =
  function (aggregationFn, columnName, operator, columnValue, propertyValue) {
    function buildClause(columnValue, separator, grouping) {
      var values = [];
      for (var i = 0, n = columnValue.length; i < n; i++) {
        if (columnValue[i] instanceof ParameterizedSQL) {
          values.push(columnValue[i]);
        } else {
          values.push(new ParameterizedSQL(ParameterizedSQL.PLACEHOLDER, [columnValue[i]]));
        }
      }
      separator = separator || ',';
      var clause = ParameterizedSQL.join(values, separator);
      if (grouping) {
        clause.sql = '(' + clause.sql + ')';
      }
      return clause;
    }

    var sqlExp = columnName;
    var clause;
    if (columnValue instanceof ParameterizedSQL) {
      clause = columnValue;
    } else {
      clause = new ParameterizedSQL(ParameterizedSQL.PLACEHOLDER, [columnValue]);
    }
    switch (operator) {
      case 'gt':
        sqlExp = aggregationFn + '(' + sqlExp + ')>';
        break;
      case 'gte':
        sqlExp = aggregationFn + '(' + sqlExp + ')>=';
        break;
      case 'lt':
        sqlExp = aggregationFn + '(' + sqlExp + ')<';
        break;
      case 'lte':
        sqlExp = aggregationFn + '(' + sqlExp + ')<=';
        break;
      case 'between':
        sqlExp = aggregationFn + '(' + sqlExp + ') BETWEEN ';
        clause = buildClause(columnValue, ' AND ', false);
        break;
      case 'inq':
        sqlExp = aggregationFn + '(' + sqlExp + ') IN ';
        clause = buildClause(columnValue, ',', true);
        break;
      case 'nin':
        sqlExp = aggregationFn + '(' + sqlExp + ') NOT IN ';
        clause = buildClause(columnValue, ',', true);
        break;
      case 'neq':
        if (columnValue == null) {
          return new ParameterizedSQL(aggregationFn + '(' + sqlExp + ') IS NOT NULL');
        }
        sqlExp = aggregationFn + '(' + sqlExp + ')!=';
        break;
      case 'like':
        sqlExp = aggregationFn + '(' + sqlExp + ') LIKE ';
        break;
      case 'nlike':
        sqlExp = aggregationFn + '(' + sqlExp + ') NOT LIKE ';
        break;
      // this case not needed since each database has its own regex syntax, but
      // we leave the MySQL syntax here as a placeholder
      case 'regexp':
        sqlExp = aggregationFn + '(' + sqlExp + ') REGEXP ';
        break;
    }
    var stmt = ParameterizedSQL.join([sqlExp, clause], '');
    return stmt;
  }

require('./discovery')(PostgreSQL);
require('./migration')(PostgreSQL);
require('./transaction')(PostgreSQL);
require('./lock')(PostgreSQL);
