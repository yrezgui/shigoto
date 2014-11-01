db.jobs.aggregate(
  { $match : {
    salary_min : {
      $gt : 1000,
      $lt : 200000
    }
  }},
  { $project : {
      title : 1,
      salary_min : 1 ,
      salary_max : 1,
      tags: 1
  }},
  { $unwind : "$tags" },
  { $project : {
    title : 1,
    salary_min : 1 ,
    salary_max : 1,
    skill: "$tags.display_name",
    skillType: "$tags.tag_type"
  }},
  { $match : {
    skillType : "SkillTag"
  }},
  { $group: {
    _id : '$skill',
    salary_min : { $avg : "$salary_min" },
    salary_max : { $avg : "$salary_max" },
    count : { $sum : 1 }
  }},
  { $sort: {
    count: -1,
    salary_min: -1,
    salary_max: -1
  }}
);