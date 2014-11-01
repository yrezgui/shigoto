r.db('shigoto').table('jobs').concatMap(function(job) {
    return job('tags').map(function(tag) {
      return {salary_min: job('salary_min'), salary_max: job('salary_max'), skill: tag('display_name'), skillType: tag('tag_type')};
    });
  }).group('skill');

var jobs = r.db('shigoto').table('jobs').filter(function(job) {
    return job('salary_min').gt(1000) && job('salary_min').lt(500000);
});