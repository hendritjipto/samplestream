src_hard_coded = {
    $source: {
        // our hard-coded dataset
        documents: [
            {
                'id': 'entity_1',
                'name': "adAm sanDler",
                'gender' : "M",
                'city': "Manila"
            },
            {
                'id': 'entity_1',
                'name': "SIr AlOn Balon po",
                'gender' : "M",
                'city': "manilax"
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

sp_pipeline = [src_hard_coded, upperCityCase, validate, upperNameCase, genderString];
sp.process(sp_pipeline);


