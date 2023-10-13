// Task 1iii

db.ratings.aggregate([
    // TODO: Write your query here
        // Perform an aggregation
        {
            $group: {
                _id: "$rating", // Group by the field movieId
                count: {$sum: 1} // Get the count for each group
            }
         },
         {$sort: {_id: -1}},
         {$project: {count: 1, _id: 0, rating: "$_id"}}



    ]);