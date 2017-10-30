var Writable = require('stream').Writable;
var util = require('util');
var MongoClient = require('mongodb').MongoClient;

util.inherits(StreamToMongo, Writable);

module.exports = StreamToMongo;

function StreamToMongo(options) {
  console.log('XXXXXXXX const');
  console.log(options);
  if(!(this instanceof StreamToMongo)) {
    return new StreamToMongo(options);
  }
  Writable.call(this, { objectMode: true });
  this.options = options;

  console.log('XXXXXXXX const done');
}


StreamToMongo.prototype._write = function (obj, encoding, done) {
  console.log('XXXXXXXX _write');
  var self = this;

  // Custom action definition
  var action = this.options.action || function insert (obj, cb) {
      console.log('XXXXXXXX');
      this.collection.insert(obj, cb);
    };

  if (!this.db) {
    console.log('XXXXXXXX pre con');
    console.log(this.options);
    MongoClient.connect(this.options.db, function (err, db) {
      console.log(err);
      if (err) throw err;
      console.log('XXXXXXXX connected');
      self.db = db;
      self.on('finish', function () {
        self.db.close();
      });
      self.collection = db.collection(self.options.collection);
      action.call(self, obj, done);
    });
  } else {
    action.call(self, obj, done);
  }
};
