//Starting a child process  
var spawn = require('child_process').spawn,
    child = spawn('sbt');

child.stdout.on('data', function(data){
    console.log('from child stdout', data.toString());
    if(data.toString()){
        io.emit('chat message', {'message': data.toString()});
    }
})

child.on('close', function (code) {
    console.log('child process exited with code ' + code);
    process.exit();
});

child.stderr.on('data', function(data){
    console.log('from child error', data.toString());
    io.emit('chat message', {'message': data.toString()});
})

//Running Portal Shell with hideCharacter option
child.stdin.write("run --hideCharactersInTerminal\n");

//Start of the web app
var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);

app.get('/', function(req, res){
	 res.sendFile("index.html", {"root": 'WebTerminal/public'});
});

io.on('connection', function(socket){
	socket.on('chat message', function(msg){
		io.emit('chat message', {'message': 'Portal>' + msg});
		console.log('message: ' + msg);
		child.stdin.write(msg + "\\" + "\n");
	});
})

http.listen(7180, function(){
	console.log('listening on *:7180');
});
