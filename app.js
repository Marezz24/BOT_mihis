// app.js
const dayjs = require("dayjs");
require("dayjs/locale/es"); // Importamos el idioma español
const customParseFormat = require("dayjs/plugin/customParseFormat");
dayjs.extend(customParseFormat);
dayjs.locale("es"); // Usamos el español globalmente

const { Client } = require("pg");
const procesarExcels = require("./botCargaMasiva/bot");

// Configuración de conexión a PostgreSQL
const config = {
  host: "10.4.199.133",
  port: 5432,
  user: "postgres",
  password: "soporte010203",
  database: "mihis",
};

// Función para formatear la fecha (por ejemplo, de "DD/MM/YYYY" a "YYYY-MM-DD")
const formatSimpleDate = (date, inputFormat = "DD/MM/YYYY") => {
  const d = dayjs(date, inputFormat);
  return d.isValid() ? d.format("YYYY-MM-DD") : null;
};

// ----------------------------------------------------------------
// Procesar Paciente
// ----------------------------------------------------------------
async function procesarPaciente(dataResumen) {
  console.log(">> Procesando paciente...");
  const client = new Client(config);
  await client.connect();
  try {
    // Primero se consulta si existe el paciente, usando el RUT
    const selectQuery = "SELECT pac_id FROM pacientes WHERE pac_rut = $1;";
    const resultSelect = await client.query(selectQuery, [dataResumen.rut]);

    if (resultSelect.rowCount > 0) {
      console.log("Paciente ya existe en la BD.");
      return resultSelect.rows[0];
    } else {
      console.log("Paciente no encontrado, se procede a insertar.");
      const insertQuery = `
        INSERT INTO pacientes (pac_rut, pac_nombres, pac_appat, pac_fono)
        VALUES ($1, $2, $3, $4)
        RETURNING pac_id;
      `;
      const pac_rut = dataResumen.rut ? dataResumen.rut.trim() : null;
      const pac_nombres = dataResumen.nombresPac
        ? dataResumen.nombresPac.trim()
        : "";
      const pac_appat = dataResumen.apePatPac
        ? dataResumen.apePatPac.trim()
        : "";
      const pac_fono = ""; // No se encontró información telefónica en el JSON

      const resultInsert = await client.query(insertQuery, [
        pac_rut,
        pac_nombres,
        pac_appat,
        pac_fono,
      ]);
      console.log("Paciente insertado en la BD.");
      return resultInsert.rows[0];
    }
  } catch (error) {
    console.error("Error al procesar paciente:", error);
    throw error;
  } finally {
    await client.end();
  }
}

// ----------------------------------------------------------------
// Funciones para Procesamiento de Funcionarios
// ----------------------------------------------------------------

/**
 * Función recursiva para recorrer cualquier objeto (u arreglo) y extraer
 * los valores de las propiedades cuyas claves comienzan con "func".
 * Se usan en un Set para evitar duplicados.
 */
function extraerFuncionarios(obj) {
  const funcionarios = new Set();

  const recorrer = (valor) => {
    if (valor && typeof valor === "object") {
      if (Array.isArray(valor)) {
        for (const item of valor) {
          recorrer(item);
        }
      } else {
        for (const key in valor) {
          if (Object.hasOwnProperty.call(valor, key)) {
            // Si la clave inicia con "func" y el valor es una cadena no vacía
            if (
              key.startsWith("func") &&
              typeof valor[key] === "string" &&
              valor[key].trim() !== ""
            ) {
              funcionarios.add(valor[key].trim());
            }
            recorrer(valor[key]);
          }
        }
      }
    }
  };

  recorrer(obj);
  return Array.from(funcionarios);
}

/**
 * Función que consulta si existe un funcionario (identificado por su "func_nombre")
 * y, si no se encuentra, aborta el proceso.
 * Se acepta como parámetro un string (nombre) o un objeto que contenga la propiedad "nombre".
 */
async function procesarFuncionario(funcData) {
  let nombre;
  let cargo = null;
  let clave = null;
  let email = null;
  let servicio = null;
  const ultimoLog = new Date();

  if (typeof funcData === "string") {
    nombre = funcData.trim();
  } else if (typeof funcData === "object" && funcData !== null) {
    nombre = funcData.nombre ? funcData.nombre.trim() : null;
    cargo = funcData.cargo ? funcData.cargo.trim() : null;
    clave = funcData.clave ? funcData.clave.trim() : null;
    email = funcData.email ? funcData.email.trim() : null;
    servicio = funcData.servicio ? funcData.servicio.trim() : null;
  }

  if (!nombre) {
    console.error("Nombre de funcionario inválido o vacío. Abortando proceso.");
    throw new Error("Funcionario no encontrado: nombre inválido");
  }

  console.log(`>> Buscando funcionario: "${nombre}"`);
  const client = new Client(config);
  await client.connect();
  try {
    const selectQuery = `
      SELECT func_id, func_cargo, func_clave, func_email, func_servicio 
      FROM funcionario 
      WHERE func_nombre = $1;
    `;
    const resultSelect = await client.query(selectQuery, [nombre]);
    if (resultSelect.rowCount > 0) {
      console.log(`Funcionario "${nombre}" encontrado en la BD.`);
      return resultSelect.rows[0];
    } else {
      console.error(
        `Funcionario "${nombre}" no encontrado en la base de datos. Abortando el procesamiento del excel.`
      );
      throw new Error(`Funcionario "${nombre}" no encontrado.`);
    }
  } catch (error) {
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Función para obtener el ID del funcionario para traslado a partir de su nombre.
 * Si no se encuentra, se lanza un error y se corta el proceso.
 */
async function obtenerFuncionarioIdPorNombre(nombre) {
  if (!nombre || !nombre.trim()) {
    console.error(
      "Nombre de funcionario para traslado es inválido o vacío. Abortando proceso."
    );
    throw new Error("Funcionario de traslado no encontrado: nombre inválido");
  }
  console.log(`>> Buscando funcionario para traslado: "${nombre.trim()}"`);
  const client = new Client(config);
  await client.connect();
  try {
    const query = `
      SELECT func_id 
      FROM funcionario 
      WHERE func_nombre ILIKE '%' || $1 || '%'
      LIMIT 1;
    `;
    const result = await client.query(query, [nombre.trim()]);
    if (result.rowCount > 0) {
      console.log(`Funcionario para traslado "${nombre}" encontrado.`);
      return result.rows[0].func_id;
    } else {
      console.error(
        `No se encontró funcionario con nombre similar a: "${nombre}". Abortando proceso.`
      );
      throw new Error(`Funcionario de traslado "${nombre}" no encontrado.`);
    }
  } catch (error) {
    throw error;
  } finally {
    await client.end();
  }
}

// ----------------------------------------------------------------
// Inserción de Traslado de Paciente
// ----------------------------------------------------------------
async function insertarPacienteTraslado(trasladoData) {
  console.log(">> Insertando traslado de paciente...");
  const client = new Client(config);
  await client.connect();
  console.log("Datos de la info de traslado:", JSON.stringify(trasladoData));
  try {
    const insertQuery = `
      INSERT INTO paciente_traslado (
        ptras_fecha, 
        ptras_cama_origen, 
        ptras_cama_destino, 
        ptras_func_id,
        hosp_id
      ) VALUES ($1, $2, $3, $4, $5)
      RETURNING ptras_id;
    `;
    const values = [
      trasladoData.ptras_fecha,
      trasladoData.ptras_cama_origen,
      trasladoData.ptras_cama_destino === "(Sin Asignar...)"
        ? 0
        : trasladoData.ptras_cama_destino,
      trasladoData.ptras_func_id,
      trasladoData.hosp_id,
    ];
    const result = await client.query(insertQuery, values);
    console.log("Traslado insertado correctamente.");
    return result.rows[0];
  } catch (error) {
    console.error("Error al insertar traslado:", error);
    throw error;
  } finally {
    await client.end();
  }
}

// ----------------------------------------------------------------
// Inserción en Hospitalización
// ----------------------------------------------------------------
async function insertarHospitalizacion(hospData) {
  console.log(">> Insertando hospitalización...");
  const client = new Client(config);
  await client.connect();
  try {
    const insertQuery = `
      INSERT INTO hospitalizacion (
        hosp_fecha_ing, 
        hosp_pac_id, 
        hosp_func_id,
        hosp_criticidad,
        hosp_diag_cod,
        hosp_diagnostico,
        hosp_servicio
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING hosp_id;
    `;

    // Si el servicio es un valor no numérico (por ejemplo, "(Sin Asignar...)"), se asigna null.
    const servicioValue =
      hospData.hosp_servicio === "(Sin Asignar...)"
        ? null
        : hospData.hosp_servicio;

    const values = [
      hospData.hosp_fecha_ing,
      hospData.hosp_pac_id,
      hospData.hosp_func_id,
      hospData.hosp_criticidad,
      hospData.hosp_diag_cod,
      hospData.hosp_diagnostico,
      servicioValue,
    ];

    const result = await client.query(insertQuery, values);
    console.log("Hospitalización insertada correctamente.");
    return result.rows[0];
  } catch (error) {
    console.error("Error al insertar hospitalización:", error);
    throw error;
  } finally {
    await client.end();
  }
}

// ----------------------------------------------------------------
// Función Principal
// ----------------------------------------------------------------
async function Principal() {
  console.log(">> Iniciando proceso de carga masiva...");
  try {
    // Se procesa el excel y se obtiene el objeto JSON
    console.log(">> Procesando archivos Excel...");
    const dataExcels = JSON.parse(procesarExcels());
    // Seleccionamos la hoja deseada (en este ejemplo, la del décimo excel, índice 9)
    const datosExcelDesignado = dataExcels[9].sheets;
    console.log(
      ">> Datos del excel obtenidos:",
      JSON.stringify(datosExcelDesignado, null, 2)
    );

    // 1. Procesar paciente a partir del objeto "Resumen"
    const paciente = await procesarPaciente(datosExcelDesignado.Resumen);
    console.log(">> Paciente procesado:", paciente);

    // 2. Extraer y procesar los funcionarios encontrados en cualquier parte del JSON
    const funcionariosEncontrados = extraerFuncionarios(datosExcelDesignado);
    console.log(">> Funcionarios extraídos del JSON:", funcionariosEncontrados);

    // Para cada funcionario extraído se verifica que exista en la BD;
    // si alguno no se encuentra, se aborta el proceso.
    for (const func of funcionariosEncontrados) {
      console.log(`>> Procesando funcionario extraído: "${func}"`);
      const funcionarioDB = await procesarFuncionario(func);
      console.log(
        `>> Funcionario procesado ("${func}"): ${JSON.stringify(funcionarioDB)}`
      );
    }

    // 3. Insertar registro en hospitalización
    // Se utiliza el campo "admision" de DatosPaciente para la fecha de ingreso
    const admisionRaw = datosExcelDesignado.DatosPaciente?.admision ?? null;
    const hosp_fecha_ing = admisionRaw
      ? formatSimpleDate(admisionRaw, "DD/MM/YYYY")
      : null;

    // Se obtiene el funcionario de admisión (por ejemplo, "funcAdmision" en DatosPaciente)
    const funcAdmision =
      datosExcelDesignado.DatosPaciente?.funcAdmision ?? null;
    if (!funcAdmision) {
      console.error(
        "Funcionario de admisión no proporcionado. Abortando proceso."
      );
      throw new Error("Funcionario de admisión no encontrado.");
    }
    const funcionarioAdmision = await procesarFuncionario(funcAdmision);
    const hosp_func_id = funcionarioAdmision
      ? funcionarioAdmision.func_id
      : null;

    // Construir objeto con los datos para hospitalización
    const hospData = {
      hosp_fecha_ing, // Fecha de admisión convertida
      hosp_pac_id: paciente.pac_id, // ID del paciente
      hosp_func_id, // ID del funcionario de admisión
      hosp_criticidad: datosExcelDesignado.Resumen?.categoria ?? null, // Categoría/criticidad
      hosp_diag_cod: datosExcelDesignado.Resumen?.codDiagnostico ?? null, // Código diagnóstico
      hosp_diagnostico: datosExcelDesignado.Resumen?.diagnostico ?? null, // Diagnóstico
      hosp_servicio: datosExcelDesignado.Resumen?.servicio ?? null, // Servicio o clasificación de cama
    };

    const resultadoHospitalizacion = await insertarHospitalizacion(hospData);
    console.log(">> Hospitalización insertada:", resultadoHospitalizacion);

    // 4. Insertar registro en paciente_traslado
    // Se extrae la fecha para el traslado (usando el mismo campo "admision")
    const trasladoFechaRaw =
      datosExcelDesignado.DatosPaciente?.admision ?? null;
    const ptras_fecha = trasladoFechaRaw
      ? formatSimpleDate(trasladoFechaRaw, "DD/MM/YYYY")
      : null;

    // Se usan los campos "ctaCorriente" y "servicio" del objeto Resumen para origen y destino
    const ptras_cama_origen = datosExcelDesignado.Resumen?.ctaCorriente ?? null;
    const ptras_cama_destino = datosExcelDesignado.Resumen?.servicio ?? null;

    // Se obtiene el funcionario del traslado a partir del campo "funcEgreso" de DatosPaciente
    const nombreFuncionarioTraslado =
      datosExcelDesignado.DatosPaciente?.funcEgreso ?? null;
    if (!nombreFuncionarioTraslado) {
      console.error(
        "Funcionario de egreso (traslado) no proporcionado. Abortando proceso."
      );
      throw new Error("Funcionario de traslado no encontrado.");
    }
    const ptras_func_id = await obtenerFuncionarioIdPorNombre(
      nombreFuncionarioTraslado
    );

    // Se utiliza el hosp_id obtenido de la hospitalización (para cumplir con el trigger)
    const hosp_id = resultadoHospitalizacion.hosp_id;

    const trasladoData = {
      ptras_fecha,
      ptras_cama_origen,
      ptras_cama_destino,
      ptras_func_id,
      hosp_id,
    };

    const resultadoTraslado = await insertarPacienteTraslado(trasladoData);
    console.log(">> Paciente traslado insertado:", resultadoTraslado);

    console.log(">> PROCESO FINALIZADO CORRECTAMENTE.");
  } catch (error) {
    console.error(">> Error en el proceso:", error);
    // Se puede salir del proceso si se desea:
    // process.exit(1);
  }
}

// Ejecutamos la función principal
Principal();
