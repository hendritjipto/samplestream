src_hard_coded = {
    $source: {
        // our hard-coded dataset
        documents: [
            {
                
                'name': "adAm sanDler",
                'gender' : "M",
                'city': "Manila"
            },
            {
                
                'name': "SIr AlOn Balon po",
                'gender' : "M",
                'city': "manila"
            }
        ]
    }
}

upperCityCase = {
    $set: {
        city: {
            $reduce: {
                input: { $split: [{ $trim: { input: "$city" } }, " "] },
                initialValue: "",
                in: {
                    $concat: [
                        "$$value",
                        { $cond: [{ $eq: ["$$value", ""] }, "", " "] },
                        {
                            $concat: [
                                { $toUpper: { $substr: ["$$this", 0, 1] } },
                                { $toLower: { $substr: ["$$this", 1, { $subtract: [{ $strLenCP: "$$this" }, 1] }] } }
                            ]
                        }
                    ]
                }
            }
        }
    }
}

validate = {
    $validate : {   
        validator: {
            $jsonSchema: {
              bsonType: "object",
              required: ["city"], // Specify the field you want to validate
              properties: {
                city: {
                  enum: ["Manila", "Valencia", "Victorias"], // Allowed values for the field
                  description: "Must be one of the specified values."
                }
              }
            }
          },
          validationAction : "dlq"
    }
}

upperNameCase = {
    $set: {
        name: {
            $reduce: {
                input: { $split: [{ $trim: { input: "$name" } }, " "] },
                initialValue: "",
                in: {
                    $concat: [
                        "$$value",
                        { $cond: [{ $eq: ["$$value", ""] }, "", " "] },
                        {
                            $concat: [
                                { $toUpper: { $substr: ["$$this", 0, 1] } },
                                { $toLower: { $substr: ["$$this", 1, { $subtract: [{ $strLenCP: "$$this" }, 1] }] } }
                            ]
                        }
                    ]
                }
            }
        }
    }
}

genderString = {
    $set: {
        gender: {
          $cond: {
            if: {
              $eq: ["$gender", "M"]
            },
            then: "Male",
            else: {
              $cond: {
                if: {
                  $eq: ["$gender", "F"]
                },
                then: "Female",
                else: "$gender"
              }
            }
          }
        }
      }
}

mergex = {
    $merge: {
        into: {
          connectionName: "altas",
          db: "stream",
          coll: "profile"
      }
}}

deadLetter = {
    dlq: {
      connectionName: "altas",
      db: "stream",
      coll: "dlq"
    }
  }

sp_pipeline = [src_hard_coded, upperCityCase, validate, upperNameCase, genderString,mergex];
//sp.createStreamProcessor("Stream1", sp_pipeline, deadLetter);
sp.process(sp_pipeline);


//sp.Stream1.start()
//sp.listStreamProcessors()
//sp.Stream1.drop()