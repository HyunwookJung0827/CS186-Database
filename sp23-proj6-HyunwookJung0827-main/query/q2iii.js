// Task 2iii

db.movies_metadata.aggregate([
    // TODO: Write your query here
    {$project:
            {_id: 0,
            budget: {
                         $cond: { if: {$eq: ["$budget", false]}, then: null, else: "$budget" }
                       }}},
    {$project:
                {_id: 0,
                budget: {
                             $cond: { if: {$eq: ["$budget", '']}, then: null, else: "$budget" }
                           }}},
    {$project: {
                budget: {$convert: {input: "$budget", to: "string"}}
                }
            },/*
    {$unwind: "$budget"},*/
    {$project: {
        _id: 0,
            budget: {$toInt: {$trim: {input: {$trim: {input: "$budget", chars: " USD$"}}, chars: " USD$"}}}
            }
        },
    {$project:
        {_id: 0,
        budget: {
                     $cond: { if: {$eq: ["$budget", "undefined"]}, then: "unknown", else: "$budget" }
                   }}},
   {$project:
        {_id: 0,
        budget: {
                     $cond: { if: {$eq: ["$budget", null]}, then: "unknown", else: "$budget" }
                   }}},
    {$group: {
        _id: {
            $cond: {if: {$eq: ["$budget", "unknown"]}, then: "unknown", else: {$round: ["$budget", -7]}}},
        count: {$sum: 1}
    }},
    {$project:{
    _id: 0,
    budget: "$_id",
    count: 1}},
    {$sort: {budget: 1}}
]);