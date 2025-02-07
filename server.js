// server.js
const express = require('express');
const app = express();
const routes = require('./routes'); // Se importan las rutas definidas

// Middleware para parsear el cuerpo de las peticiones en formato JSON
app.use(express.json());

// Se usan las rutas bajo el prefijo /api (por ejemplo: /api/hello)
app.use('/api', routes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Servidor corriendo en el puerto ${PORT}`);
});
