var http = require('http');
var AWS = require('aws-sdk');
var exceptionCount = 0;
var exData = '';
var shardIterator = '';
var url = require('url');

var kinesis = new AWS.Kinesis({apiVersion: '2013-12-02',
accessKeyId:'X' ,
secretAccessKey:'X' ,
region:  'us-east-1'
});





var app = http.createServer(function(req,res){

    console.log('YES!!!!!!!!! New Request received');

    var queryObject = url.parse(req.url,true).query;
    var myStartTime = Math.floor(Date.now()/1000)  - queryObject.duration *60;
    console.log("GAnesh , start time is : "+myStartTime);

    var params = {
     ShardId: '0', /* required */
     ShardIteratorType: 'AT_TIMESTAMP', /* required */
     StreamName: 'VC_ExceptionLogs', /* required */
     Timestamp: myStartTime
    };
    kinesis.getShardIterator(params, function(err, data) {
     if (err) console.log(err, err.stack); // an error occurred
     else{
          exceptionCount = 0;
          shardIterator = data.ShardIterator;
          if (shardIterator !='') {
               var streamData ;
               var finalData = '';
               params = {
                 ShardIterator: shardIterator, /* required */
                 Limit: 100
               };
               kinesis.getRecords(params, function(err, data) {
                 if (err) console.log(err, err.stack); // an error occurred
                 else{
                   //streamData = new Buffer(data.Records[0].Data, 'base64').toString()
                   var records =  data.Records;
                   var record;
                   exceptionCount = records.length;
                   //----------------------------
                   for (var i = 0 ; i <records.length ; ++i) {
                     record=records[i];
                     data = new Buffer(record.Data, 'base64').toString();
                     var exJson = JSON.parse(data);
                     if (i==0) {
                       finalData = '{Exceptions:[';
                     }

                    if(finalData.indexOf(exJson.VCException.Text) == -1){
                     finalData += '{\"Text\":\"'+exJson.VCException.Text+'\", \"Message\":\"'+exJson.VCException.Message+'\"}';
                     if (i!=records.length-1)
                          finalData += ',';
                   }
                    if (i==records.length-1)
                       finalData += ']}';

                  }



                   //-------------------------------
                   //console.log('Ganesh Your Final data is :  '+finalData);
                  if(finalData !='')
                   {
                      finalData = [finalData.slice(0,1),"\"count\":\""+exceptionCount+"\",",finalData.slice(1)].join('');
                      res.setHeader('Content-type', 'application/json');
                      //res.write();
                      res.write(finalData);
                      res.end();
                    }
                  else {
                      res.setHeader('Content-type', 'application/json');
                      res.write("{\"count\":\""+exceptionCount+"\"}");
                      res.end();
                  }
                 }                // successful response
               });
     }
     else {
       res.setHeader('Content-type', 'application/json');
       res.write("No Shard Iterator received");
       res.end();
      }
     }          // successful response
    });



});
var port = process.env.PORT || 4000;
app.listen(port);
