const { client } = require('pg');

const Client = new client({
    host: "10.4.199.133",
    port: 5432,
    user: "postgres",
    password: "soporte010203",
    database: "mihis",
    connectionTimeoutMillis: 30000, // 30 segundos (ajustable)
    idleTimeoutMillis: 30000,       // 30 segundos
    max: 20                         // Número máximo de conexiones en el pool
  });
  
  module.exports = Client;