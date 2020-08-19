const ParseXbrl = require('parse-xbrl');

// path to locally accessible file, does not load file over http/https
ParseXbrl.parse('./test.xbrl.xml').then(function(parsedDoc) {
  console.log(parsedDoc);
});
