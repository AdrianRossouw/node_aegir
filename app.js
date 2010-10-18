require.paths.unshift(__dirname + '/modules', __dirname + '/lib/node', __dirname);

var sys = require('sys');
var exec = require('child_process').exec
var settings = require('settings')
var mysql = require("mysql-native");

var db = null

var max_tasks = 5;

var current_tasks = {}

var size = function(obj) {
  var size = 0, key;
  for (key in obj) {
      if (obj.hasOwnProperty(key)) size++;
  }
  return size;
};


drush_exec('hostmaster', 'status', [], {}, function(results) {
  settings.mysql.user = results.context.db_user;
  settings.mysql.pass = results.context.db_passwd;
  settings.mysql.host = results.context.db_host;
  settings.mysql.name = results.context.db_name;
  settings.mysql.port = results.context.db_port;

  db = mysql.createTCPClient(settings.mysql.host, settings.mysql.port);
  db.autoprepare = true;
  db.auth(settings.mysql.name, settings.mysql.user, settings.mysql.pass)

  console.log("connected to db, starting task queue poll");
  poll_tasks();

});

function poll_tasks() {
  db.query("SELECT t.nid FROM hosting_task t INNER JOIN node n ON t.vid = n.vid "
    + "WHERE t.task_status = 0 ORDER BY n.changed, n.nid ASC LIMIT 10")
    .addListener('row', function(r) { 
      if ((size(current_tasks) < max_tasks) && !current_tasks.hasOwnProperty(r[0])) {
        run_task(r[0])
      }
    });

  setTimeout(function () { poll_tasks() } , 1000);
}


function run_task(task) {
  (function(task_id) { 
    current_tasks[task_id] = task_id
    console.log("Queuing task : " + task_id)

    drush_exec('hostmaster', 'hosting-task', [task_id], {}, function(results) {
      console.log("Completed task : " + task_id + ' return status: ' + results['error_status']);
      delete current_tasks[task_id];
    });

  }) (task);
}

function drush_exec(context, command, args, options, callback) {
  args = (args != undefined) ? args : []
  options = (options != undefined) ? options : {}
  var option_string = ' ';

  for (key in options) {
    var ret = ''
    ret = options[key].replace(/[^\\]'/g, function(m, i, s) {
            return m.slice(0, 1)+'\\\'';
    });
    option_string = option_string+' --'+key+"='"+ret+"'";
  }

  command = 'drush @' + context + ' ' + command + ' ' + args.join(' ') + option_string +' -b';
  console.log("Executing " + command)
  child = exec( command, { maxBuffer : Number.MAX_VALUE },
    function (error, stdout, stderr) {
      if (error !== null) {
        console.log('exec error: ' + error);
      }
      else {
        output = JSON.parse(stdout.match(/DRUSH_BACKEND_OUTPUT_START>>>(.*)<<<DRUSH_BACKEND_OUTPUT_END/)[1])
        if (callback) {
          callback(output);
        }
      }
  });
}


