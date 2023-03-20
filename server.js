const express = require('express');
const app = express();
const mongoose = require('mongoose');
const csv = require('csv-parser');
const fs = require('fs');
const cors = require('cors');

app.use(cors()); // enable CORS

// Connect to MongoDB
mongoose.connect('mongodb://0.0.0.0:27017/advertising_data', {
  useNewUrlParser: true,
  useUnifiedTopology: true
})
  .then(() => {
    console.log('Connected to MongoDB');
  
// Read and parse CSV file
    const dataArray = [];
    fs.createReadStream('data.csv')
      .pipe(csv())
      .on('data', (row) => {
        const data = {
          date: row.date,
          source: row.source,
          attributed_conversions: row.attributed_conversions,
          attributed_revenue: row.attributed_revenue,
          type: row.type,
          spends: row.spends,
          partition_id: row.partition_id,
          optimisation_target: row.optimisation_target
        };
        dataArray.push(data);
      })
      .on('end', () => {
        const uniqueData = Array.from(new Set(dataArray.map(JSON.stringify))).map(JSON.parse);
        const bulkUpdateOps = uniqueData.map((data) => ({
          updateOne: {
            filter: { date: data.date, source: data.source, type: data.type, partition_id: data.partition_id, optimisation_target: data.optimisation_target },
            update: data,
            upsert: true
          }
        }));
        Data.bulkWrite(bulkUpdateOps)
          .then(() => {
            console.log('Data successfully saved to database');
          })
          .catch((err) => {
            console.error(err);
          });
      });
  })
  .catch((err) => {
    console.error(err);
  });

// Create a schema for the data
const advertisingDataSchema = new mongoose.Schema({
  date: String,
  source: String,
  attributed_conversions: Number,
  attributed_revenue: Number,
  type: String,
  spends: Number,
  partition_id: String,
  optimisation_target: String
});

// Create a model for the data
const Data = mongoose.model('Data', advertisingDataSchema);

// Define endpoint for getting all data
app.get('/data', (req, res) => {
  Data.find({})
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});

// Filter data by source
app.get('/data/source/:source', (req, res) => {
  const source = req.params.source;
  Data.find({ source: source })
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});

// Filter data by optimisation_target
app.get('/data/target/:target', (req, res) => {
  const target = req.params.target;
  Data.find({ optimisation_target: target })
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});

// Filter data by type
app.get('/data/type/:type', (req, res) => {
  const type = req.params.type;
  Data.find({ type: type })
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});

// Filter data by source and type
app.get('/data/source/:source/type/:type', (req, res) => {
  const source = req.params.source;
  const type = req.params.type;
  Data.find({ source: source, type: type })
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});

  // Filter data by source and optimisation_target
app.get('/data/source/:source/target/:target', (req, res) => {
  const source = req.params.source;
  const target = req.params.target;
  Data.find({ source: source, optimisation_target: target })
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});

// Filter data by optimisation_target and type
app.get('/data/target/:target/type/:type', (req, res) => {
  const target = req.params.target;
  const type = req.params.type;
  Data.find({ optimisation_target: target, type: type })
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});

// Filter data by source, optimisation_target and type
app.get('/data/source/:source/target/:target/type/:type', (req, res) => {
  const source = req.params.source;
  const target = req.params.target;
  const type = req.params.type;
  Data.find({ source: source, optimisation_target: target, type: type })
    .then((data) => {
      res.json(data);
    })
    .catch((err) => {
      res.status(500).send(err);
    });
});
  
// Start the server
app.listen(3000, () => {
  console.log('Server started on port 3000');
});
