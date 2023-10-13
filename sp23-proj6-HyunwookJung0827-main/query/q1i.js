// Task 1i

db.keywords.aggregate([
    // TODO: Write your query here
    {$match:
        {$or: [{keywords: {$elemMatch: {name: "mickey mouse"}}},
            {keywords: {$elemMatch: {name: "marvel comic"}}}]}},

    {$project: {
    movieId: 1,
    _id: 0}},
    {$sort: {movieId: 1}}
]);