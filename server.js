const express = require('express');
require('dotenv').config();
const { MongoClient } = require('mongodb');

const client = new MongoClient(process.env.MONGO_URI);
let database;
client
  .connect()
  .then(() => {
    console.log('db connected');
    database = client.db('myFirstDatabase');
  })
  .catch(err => {
    console.log(err);
  });

const app = express();

app.get('/task1', async (req, res) => {
  const orders = database.collection('orders');
  const pipeline = [
    {
      $project: {
        'buyer._id': 1,
        'offers.price': 1,
        'offers.quantityOrdered': 1,
      },
    },
    {
      $unwind: {
        path: '$offers',
      },
    },
    {
      $project: {
        'buyer._id': 1,
        order_price: {
          $multiply: ['$offers.price', '$offers.quantityOrdered'],
        },
      },
    },
    {
      $group: {
        _id: '$buyer._id',
        sum_total: {
          $sum: '$order_price',
        },
      },
    },
    {
      $sort: {
        sum_total: -1,
      },
    },
    {
      $limit: 5,
    },
  ];

  const data = await orders.aggregate(pipeline).toArray();
  res.json(data);
});

app.get('/task2', async (req, res) => {
  const orders = database.collection('orders');
  const pipeline = [
    {
      $project: {
        seller_account_id: 1,
        'offers.price': 1,
        'offers.quantityOrdered': 1,
      },
    },
    {
      $unwind: {
        path: '$offers',
      },
    },
    {
      $project: {
        seller_account_id: 1,
        order_price: {
          $multiply: ['$offers.price', '$offers.quantityOrdered'],
        },
      },
    },
    {
      $group: {
        _id: '$seller_account_id',
        total_price: {
          $sum: '$order_price',
        },
      },
    },
    {
      $sort: {
        total_price: -1,
      },
    },
    {
      $limit: 5,
    },
  ];

  const data = await orders.aggregate(pipeline).toArray();
  res.json(data);
});

app.get('/task3', async (req, res) => {
  const orders = database.collection('orders');
  const pipeline = [
    {
      $project: {
        seller_account_id: 1,
        'offers.price': 1,
        'offers.quantityOrdered': 1,
        buyer: 1,
      },
    },
    {
      $unwind: {
        path: '$offers',
      },
    },
    {
      $project: {
        seller_account_id: 1,
        buyer: 1,
        order_price: {
          $multiply: ['$offers.price', '$offers.quantityOrdered'],
        },
      },
    },
    {
      $group: {
        _id: {
          seller_id: '$seller_account_id',
          buyer: '$buyer',
        },
        buyer_total_sum: {
          $sum: '$order_price',
        },
      },
    },
    {
      $sort: {
        '_id.seller_id': -1,
        buyer_total_sum: -1,
      },
    },
    {
      $group: {
        _id: '$_id.seller_id',
        buyer_array: {
          $push: {
            buyer: '$_id.buyer',
            total_sum: '$buyer_total_sum',
          },
        },
      },
    },
    {
      $project: {
        buyer_array: {
          $slice: ['$buyer_array', 3],
        },
      },
    },
  ];

  const data = await orders.aggregate(pipeline).toArray();
  res.json(data);
});

app.listen(process.env.PORT, () => {
  console.log(`Server started on PORT ${process.env.PORT}`);
});
