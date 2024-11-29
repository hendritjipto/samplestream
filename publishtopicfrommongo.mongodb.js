src_hard_coded =
{
    $source: {
        connectionName: 'atlas',
        db : "Customer",
        coll : ["bakesales"],
    }
}

project = {
    $project: {
        "_id": 0,
        "fullDocument.item": 1,
        "fullDocument.quantity" :1,
        "fullDocument.amount": 1
    }
}

emit = {
    $emit: {
        connectionName: 'kafka',
        topic: 'from_mongo',
    }
}

sp_pipeline = [src_hard_coded, project, emit];
// sp_pipeline = [src_hard_coded, emit];
sp.process(sp_pipeline);
// sp.createStreamProcessor("Stream1", sp_pipeline);
// db.runCommand({startStreamProcessor:"Stream1", workers:1})
//sp.Stream1.start();