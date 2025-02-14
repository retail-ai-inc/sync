// init.js
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "localhost:27017" }]
});

db = db.getSiblingDB('source_db'); 

db.users.insertOne({
id: 1,
name: 'John',
email: 'John@mail'
});

db.createCollection("orders");