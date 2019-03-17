const mongo = require('mongodb').MongoClient;
const fs = require('fs');
const parse = require('csv-parse');

const output = [];
const ipaResult = {};
const mapping = {};

// Create the parser
const parser = parse({
  delimiter: ','
})
// Use the readable stream api
parser.on('readable', function () {
  let record
  while (record = parser.read()) {
    output.push(record)
  }
})
// Catch any error
parser.on('error', function (err) {
  console.error(err.message)
})

// When we are done, test that the parsed output matched what expected
parser.on('end', function () {
  console.log(`Loaded ${output.length} entries`);
})

const loadConfig = () =>
  new Promise((resolve, reject) => {
    fs.readFile('./appconfig.json', (err, data) => {
      if (err) return reject(err)
      resolve(JSON.parse(data))
    })
  })

const mongoConnect = (url) =>
  new Promise((resolve, reject) => {
    mongo.connect(url, { useNewUrlParser: true }, (err, db) => {
      if (err) return reject(err);
      resolve(db);
    });
  })

const getEDICTTypes = (db) =>
  new Promise((resolve, reject) => {
    db.db('jedict').collection('entries').distinct('meanings.partofspeech', (err, data) => {
      if (err) return reject(err)
      resolve(data)
    });
  })

const getEntriesByEDICTType = (db, type) =>
  new Promise((resolve, reject) => {
    db.db('jedict').collection('entries').find({ "meanings.partofspeech": type }).toArray((err, data) => {
      if (err) return reject(err)
      resolve(data)
    });
  })

const listIPAFiles = () =>
  new Promise((resolve, reject) => {
    fs.readdir('./ipa', (err, items) => {
      if (err) return reject(err);
      resolve(items.map(i => `./ipa/${i}`))
    })
  })

const readIPAFile = (path) =>
  new Promise((resolve, reject) => {
    fs.readFile(path, (err, data) => {
      if (err) return reject(err);
      parser.write(data)
      resolve();
    })
  })

const parseEntry = (raw) => {
  const surface = raw[0].trim();
  const obj = {
    type1: raw[4].trim(),
    type2: raw[5].trim(),
    type3: raw[6].trim(),
    type4: raw[7].trim(),
    edicts: new Set(),
  }
  if (ipaResult[surface]) {
    ipaResult[surface].push(obj);
  } else {
    ipaResult[surface] = [obj];
  }
}

const populateResult = (db, edictType) =>
  new Promise(async (resolve) => {
    const entries = await getEntriesByEDICTType(db, edictType);
    entries.forEach(e => {
      const surface = e.kanji[0] || e.readings[0];
      const ipa = ipaResult[surface];
      if (!ipa || !ipa.length) return;
      ipa.forEach(i => {
        const ipaString = `${i.type1},${i.type2},${i.type3},${i.type4}`;
        const meanings = e.meanings.map(m => m.partofspeech);
        if (mapping[ipaString]) {
          mapping[ipaString] = mapping[ipaString].add(...meanings);
        } else {
          mapping[ipaString] = new Set(meanings);
        }
      })
    })
    resolve();
  })

const writeResult = (mapping) =>
  new Promise((resolve, reject) => {
    fs.writeFile('./mapping.json', JSON.stringify(mapping, null, 2), err => {
      if (err) return reject(err);
      resolve();
    })
  })

const generateMapping = async () => {
  const config = await loadConfig();
  const mongoClient = await mongoConnect(config.MONGODB_CONNECTION_STRING);
  const files = await listIPAFiles();
  const jobs = files.map(f => readIPAFile(f))
  await Promise.all(jobs)
  parser.end()
  output.forEach(r => parseEntry(r))
  const edictTypes = await getEDICTTypes(mongoClient);
  const edictJobs = edictTypes.map(e => populateResult(mongoClient, e))
  await Promise.all(edictJobs);
  const cleanMapping = {};
  Object.keys(mapping).forEach(m => {
    cleanMapping[m] = [...mapping[m]];
  })
  await writeResult(cleanMapping);
  console.log('Done');

  mongoClient.close()
}

// run();
