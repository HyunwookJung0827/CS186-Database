// Task 3i

db.credits.aggregate([
    // TODO: Write your query here
    {$project: {_id: 0, cast: 1, movieId: 1}},
    {$unwind: "$cast"},
    {$match: {"cast.id": {$eq: 7624}}},
    {$project: {
    movieId: 1,
    character: "$cast.character"
    }},
    {
      $lookup: {
          from: "movies_metadata",
          localField: "movieId",
          foreignField: "movieId",
          as: "movies"
      }
  },
  {$project: {
      title: "$movies.title",
      release_date: "$movies.release_date",
      character: 1,

      }},
    {$unwind: "$title"},
    {$unwind: "$release_date"},

      {$sort: {release_date: -1}}
]);