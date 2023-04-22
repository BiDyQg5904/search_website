var Parser = require('@postlight/parser');
const he = require('he');

const s = process.argv[2];

try {
    Parser.parse(s).then(result => {
        var content = result.content;
        content = he.decode(content);
        console.log(content);	
    });
} catch (error) {
    console.log(null);
}


