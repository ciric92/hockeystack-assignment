const disallowedValues = [
  '[not provided]',
  'placeholder',
  '[[unknown]]',
  'not set',
  'not provided',
  'unknown',
  'undefined',
  'n/a'
];

const filterNullValuesFromObject = object =>
  Object
    .fromEntries(
      Object
        .entries(object)
        .filter(([_, v]) =>
          v !== null &&
          v !== '' &&
          typeof v !== 'undefined' &&
          (typeof v !== 'string' || !disallowedValues.includes(v.toLowerCase()) || !v.toLowerCase().includes('!$record'))));

const normalizePropertyName = key => key.toLowerCase().replace(/__c$/, '').replace(/^_+|_+$/g, '').replace(/_+/g, '_');

const goal = actions => {
  // this is where the data will be written to the database
  // in order to see everything that is being processed, we will log each action as JSON 
  // individually.
  actions.forEach(action => {
    console.log(JSON.stringify(action, null, 2));
  })
};

module.exports = {
  filterNullValuesFromObject,
  normalizePropertyName,
  goal
};
