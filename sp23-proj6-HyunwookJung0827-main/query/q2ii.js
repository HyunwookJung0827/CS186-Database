// Task 2ii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    {$project: {
    _id: 0,
        hehe: {$split: ["$tagline", " "]}
        }
    },
    {$unwind: "$hehe"}/*,
    {$project: {
        hehe: {$split: ["$hehe", "..."]}
        }
        },
    {$unwind: "$hehe"},
    {$project: {
            hehe: {$split: ["$hehe", "-"]}
            }
            },
        {$unwind: "$hehe"},
    {$project: {
            hehe: {$split: ["$hehe", "'"]}
            }
            },
        {$unwind: "$hehe"},
    {$project: {
        hehe: {$trim: {input: "$hehe", chars: ",.?!"}}
    }},
          {$project: {
              hehe: {$toLower: "$hehe"},
              wordNum: {$strLenCP: "$hehe"}
          }},
      {$match: {wordNum: {$gt: 3}}},
    {$group: {
        _id: "$hehe",
        count: {$sum: 1}
    }},
    {$sort: {count: -1}}*/
    ,
    {$project: {
    hihi: {$toLower: {$trim: {input: "$hehe", chars: ".,?!"}}}
            }
            },
    {$project: {
    hihi: 1,
    wordNum: {$strLenCP: "$hihi"}
    }}

        ,
    {$match: {wordNum: {$gt: 3}}},
    {$group: {
        _id: "$hihi",
        count: {$sum: 1}
    }},
    {$sort: {count: -1}},
    {$limit: 20}
]);