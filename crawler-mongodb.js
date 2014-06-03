var mongodb = require('mongodb');
var client  = mongodb.MongoClient;
var request = require('request');
var async   = require('async');

var API_JOBS_URL           = 'https://api.angel.co/1/jobs';
var MAX_CONCURRENT_WORKERS = 5;
var MONGODB_URI            = 'mongodb://127.0.0.1:27017/shigoto';
var MONGODB_COLLECTION     = 'jobs';


client.connect(MONGODB_URI, function(error, db) {
  if (error) {
    throw error;
  }

  crawler(db);
});

function crawler(db) {
  var worker = async.queue(function process(task, callback) {
    return request({
      url: task.url,
      qs: {page: task.page},
      method: 'GET',
      json: true
    }, function finish(error, response, body) {
      if(error) {
        return callback(error);
      }

      response.body.jobs.forEach(function(job) {
        // Set the MongoDB primary key
        job._id = job.id;
        delete job.id;
      });

      db.collection(MONGODB_COLLECTION).insert(response.body.jobs, function(error, result) {
        if(error) {
          return callback(error);
        }
        
        console.log('Page ' + task.page + ' processed');
        callback();
      });
    });

  }, MAX_CONCURRENT_WORKERS);

  worker.drain = function drain() {
    console.log('all items have been processed');
    process.exit();
  };

  // First request help us to know how many pages there are
  request({
    url: API_JOBS_URL,
    method: 'GET',
    json: true
  }, function finish(error, response, body) {
    if(error) {
      throw error;
    }

    for (var i = response.body.last_page; i >= 1; i--) {
      worker.push({ url: API_JOBS_URL, page: i });
    }
  });
}