const express = require('express');
const router = express.Router();

// Ejemplo de endpoint GET: /api/hello
router.get('/hello', (req, res) => {
  res.json({ message: 'Hola desde Express!' });
});

// Puedes agregar más endpoints, por ejemplo:
router.post('/data', (req, res) => {
  const data = req.body;
  // Procesa los datos recibidos...
  res.json({ status: 'Datos recibidos', data });
});


router.get('/paciente', async (req, res) => {
    const { rut } = req.query;
    if (!rut) {
      return res.status(400).json({ error: 'El parámetro rut es requerido.' });
    }
    try {
      const paciente = await getPaciente(rut);
      if (paciente.length === 0) {
        return res.status(404).json({ error: 'El paciente no existe.' });
      }
      res.json({ paciente });
    } catch (error) {
      console.error('Error consultando paciente:', error);
      res.status(500).json({ error: 'Error en el servidor.' });
    }
  });

  router.post('/funcionario', async (req, res) => {
    const { nombre, fechaIngreso } = req.body;
    if (!nombre || !fechaIngreso) {
      return res.status(400).json({ error: 'Se requieren "nombre" y "fechaIngreso".' });
    }
    try {
      const funcionario = await createOrGetFuncionario(nombre, fechaIngreso);
      res.json({ funcionario });
    } catch (error) {
      console.error('Error consultando/creando funcionario:', error);
      res.status(500).json({ error: 'Error en el servidor.' });
    }
  });
module.exports = router;