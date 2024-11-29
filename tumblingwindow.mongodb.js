s = {$source : {
    connectionName : "kafka",
    topic : ["temperature", "humidity"],
    timeField : { $dateFromString: { dateString: '$timestamp' } },
    partitionIdleTimeout: { "size": 5,"unit": "second" }
}}

w = {$tumblingWindow : {
        interval: {size: 30, unit: "second"},
        idleTimeout: {size: 10, unit: "second"},
        allowedLateness : {size: 5, unit: "second"},
        pipeline : [
            { $group : {
                _id : "$sensorIdGroup",      
                humidity: {$top : { output : ["$humidity", "$_ts"],  sortBy : { "humidity" : -1, "timestamp" : -1,  }}},
                temperature: {$top : { output : ["$temperature", "$_ts"],  sortBy : { "temperature" : -1, "timestamp" : -1,  }}},
            }},
            ]
}}
p = {$project : {
    _id :1,
    humidity : {$arrayElemAt : ["$humidity" ,0]},
    temperature :  {$arrayElemAt : ["$temperature" ,0]},
}}

// af = {
//     $addFields: {
//         nullFilter: {
//             $arrayToObject:{
//                 $filter:{
//                     input:{$objectToArray:"$$ROOT"}, 
//                     cond:{$not:{$in:["$$this.v", [null, "", {}]  ]}}
//                 }
//             }
//         }
//     }
// }
// According to Joe : its the logic of the processor (on tumblin window) didnt account for null when it was not late
// So when the next window comes and it contains null values, it will not be able to merge the data

// WITHOUT WORKAROUND 
// with this data :    {"sensorIdGroup": 7, "temperature": 20, "timestamp": "2024-11-04T20:02:00.000"},
// it will produce this document {"_id": 7, "temperature": 20, "humidity": null}
// with this data : {"sensorIdGroup": 7, "humidity": 55, "timestamp": "2024-11-04T20:02:45.000"}
// it will produce this document {"_id": 7, "temperature": null, "humidity": 55}
// Since it contains null values, it will not be able to merge the data

// The workaround is to filter out the null values before merging the data

// WITH WORKAROUND : 
// with this data :    {"sensorIdGroup": 7, "temperature": 20, "timestamp": "2024-11-04T20:02:00.000"},
// it will produce this document {"_id": 7, "temperature": 20} 
// with this data : {"sensorIdGroup": 7, "humidity": 55, "timestamp": "2024-11-04T20:02:45.000"}
// it will produce this document {"_id": 7, "humidity": 55} 
// So when you merge them, you will get {"_id": 7, "temperature": 20, "humidity": 55}


// rr = {$replaceRoot : { newRoot : "$nullFilter"}}

m = {
    $merge: {
        into: {
            connectionName: 'atlas',
            db: 'stream',
            coll: 'weather'
        },
        on: ['_id'],
        whenMatched: 'merge',
        whenNotMatched: 'insert'
    }
}

dlq = {dlq: {connectionName: "atlas", db: "stream", coll: "sensorDLQ"}}
sp.createStreamProcessor('sensorJoins',[s,w,p,m],dlq)
sp.sensorJoins.start()