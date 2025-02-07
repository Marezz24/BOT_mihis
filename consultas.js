// consultas.js
const dayjs = require('dayjs');
require('dayjs/locale/es'); // Importamos el idioma español
const customParseFormat = require('dayjs/plugin/customParseFormat');

dayjs.extend(customParseFormat);
dayjs.locale('es'); // Usamos el español globalmente

const { Client } = require('pg');

// Configuración de conexión a PostgreSQL
const config = {
  host: "10.4.199.133",
  port: 5432,
  user: "postgres",
  password: "soporte010203",
  database: "mihis",
};

// Variables fijas que ya usabas
const datosPaciente = {
  tabla: "pacientes",
  rut: "3530102-K"
};

const selectResumen = {
  fechaIngreso: "31/01/2025 17:30:00",
  nombre: "DIAZ FERNANDEZ DAGOBERTO"
};

// Función para formatear fechas (de "DD/MM/YYYY HH:mm:ss" a "YYYY-MM-DD")
const formatDate = (date) => {
  return dayjs(date, 'DD/MM/YYYY HH:mm:ss').format('YYYY-MM-DD');
};

/**
 * Función que consulta los datos del paciente según lo definido en "datosPaciente".
 */
async function consultaPaciente() {
  const client = new Client(config);
  await client.connect();
  try {
    const query = `SELECT pac_id FROM ${datosPaciente.tabla} WHERE ${datosPaciente.tabla}.pac_rut = '${datosPaciente.rut}';`;
    const result = await client.query(query);
    return result.rows;
  } catch (error) {
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Función que realiza la inserción (o consulta) de un funcionario usando los datos de "selectResumen".
 * En este ejemplo se inserta el registro y se retorna lo insertado.
 */
async function consultaFuncionario() {
  const client = new Client(config);
  await client.connect();
  try {
    const query = `
      INSERT INTO funcionario (func_id, func_rut, func_cargo, func_clave, func_ultimo_login)
      VALUES (1, '${selectResumen.nombre}', 'valor1', 'valor2', '${formatDate(selectResumen.fechaIngreso)}')
      RETURNING *;
    `;
    const result = await client.query(query);
    if (result.rowCount === 0) {
      return "El funcionario no existe, debe crearse";
    } else {
      return result.rows;
    }
  } catch (error) {
    throw error;
  } finally {
    await client.end();
  }
}

module.exports = {
  consultaPaciente,
  consultaFuncionario
};
