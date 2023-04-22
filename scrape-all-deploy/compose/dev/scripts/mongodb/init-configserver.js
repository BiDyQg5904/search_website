rs.initiate({
    _id: "rs-config-server", 
    configsvr: true, 
    version: 1, 
    members: [ 
        { _id: 0, host: 'configsvr01:27017' }, 
        { _id: 1, host: 'configsvr02:27017' }, 
        // { _id: 2, host: 'configsvr03:27017' } 
    ] 
})

// admin = db.getSiblingDB("admin");
// admin.createUser(
//   {
//     user: "admin",
//     pwd: "admin",
//     roles: [
//       { role: "clusterAdmin", db: "admin" },
//       { role: "userAdmin", db: "admin" }
//     ]
//   }
// );
