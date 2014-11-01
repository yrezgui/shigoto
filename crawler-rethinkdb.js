var rethink = require('rethinkdb');
var request = require('request');
var async   = require('async');

var API_JOBS_URL           = 'https://api.angel.co/1/jobs';
var MAX_CONCURRENT_WORKERS = 5;

function databaseConnect(callback) {
  rethink.connect({ host: 'localhost', port: 28015, db: 'shigoto' }, function connect(error, client) {
    if (error) {
      return callback(error, null);
    }

    callback(null, client);
  });
}

function crawler(client, finalCallback) {
  var worker = async.queue(function process(task, callback) {
    return request({
      url: task.url,
      qs: {page: task.page},
      method: 'GET',
      json: true
    }, function finish(error, response, body) {
      if(error) {
        console.error('fetch', error);
        return callback(error);
      }

      rethink.table('jobs').insert(response.body.jobs).run(client, function(error, result) {
        if(error) {
          console.error('save', error);
          return callback(error);
        }
        
        console.log('Page ' + task.page + ' processed');
        callback();
      });
    });

  }, MAX_CONCURRENT_WORKERS);

  worker.drain = function drain() {
    finalCallback('all items have been processed');
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

async.waterfall([databaseConnect, crawler], function finish(error, result) {
  console.log(error, result);
});