src_hard_coded =
{
    $source: {
        connectionName: 'kafka',
        topic: 'new_account',
        config: {
            //auto_offset_reset: "earliest",
            group_id: "mongodbatlas1"
        }
    }
}

merge = {
    $merge: {
        into: {
            connectionName: "atlas",
            db: "stream",
            coll: "account"
        },
        //on: "transaction_id",
    }
}

sp_pipeline = [src_hard_coded, merge];
//sp.process(sp_pipeline);
sp.createStreamProcessor("Stream1", sp_pipeline);
//db.runCommand({startStreamProcessor:"Stream1", workers:1})
sp.Stream1.start();