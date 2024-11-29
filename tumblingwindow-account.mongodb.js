s = {$source : {
    connectionName : "kafka",
    topic : ["account", "account_ex"],
    timeField : { $dateFromString: { dateString: '$timestamp' } },
    partitionIdleTimeout: { "size": 5,"unit": "second" }
}}

w = {$tumblingWindow : {
        interval: {size: 30, unit: "second"},
        idleTimeout: {size: 10, unit: "second"},
        allowedLateness : {size: 5, unit: "second"},
        pipeline : [
            
            { $group : {
                _id : "$accountId",   
                name : {$top : { output : ["$name"],  sortBy : { "name" : -1 }}},
                phone : {$top : { output : ["$phone"],  sortBy : { "phone" : -1 }}},
                email : {$top : { output : ["$email"],  sortBy : { "email" : -1 }}}
            }},
            ]
}}

p = {$project : {
    _id :1,
    name : {$arrayElemAt : ["$name" ,0]},
    phone :  {$arrayElemAt : ["$phone" ,0]},
    email :  {$arrayElemAt : ["$email" ,0]},
}}

// p2 = {$unset : [
//     "_stream_meta" ]}

af = {
    $addFields: {
        nullFilter: {
            $arrayToObject:{
                $filter:{
                    input:{$objectToArray:"$$ROOT"}, 
                    cond:{$not:{$in:["$$this.v", [null, "", {}]  ]}}
                }
            }
        }
    }
}

rr = {$replaceRoot : { newRoot : "$nullFilter"}}

m = {
    $merge: {
        into: {
            connectionName: 'atlas',
            db: 'stream',
            coll: 'account'
        },
        on: ['_id'],
        whenMatched: 'merge',
        whenNotMatched: 'insert'
    }
}

dlq = {dlq: {connectionName: "atlas", db: "stream", coll: "accountDLQ"}}
sp.createStreamProcessor('tumblingAccount',[s,w,p,af,rr,m],dlq)
sp.tumblingAccount.start()