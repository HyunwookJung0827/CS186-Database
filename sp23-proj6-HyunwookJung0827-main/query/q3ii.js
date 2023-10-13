// Task 3ii

db.credits.aggregate([
    // TODO: Write your query here
    {$unwind: "$crew"},
    {$match: {"crew.id": {$eq: 5655}}},
    {$match: {"crew.job": {$eq: "Director"}}},
    {$project: {
        _id: 0,
        cast: 1
    }
    }
    ,

    {$unwind: "$cast"},
    {$project: {
    _id: 0,
    name: "$cast.name",
    id: "$cast.id"
    }},
    {$group: {
        _id: {id: "$id", name: "$name"},
        //name: name,
        count: {$sum: 1}
    }},
    {$project: {
    _id: 0,
        count: "$count",
        id: "$_id.id",
        name: "$_id.name"
    }},
    {$sort: {count: -1, id: 1}},
    {$limit: 5}
]);