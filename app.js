// app.js
const dayjs = require("dayjs");
require("dayjs/locale/es"); // Importamos el idioma español
const customParseFormat = require("dayjs/plugin/customParseFormat");
dayjs.extend(customParseFormat);
dayjs.locale("es");

const { Client } = require("pg");
const procesarExcels = require("./botCargaMasiva/bot");

// Configuración de conexión a PostgreSQL


const config = {
  host: "10.4.199.133",
    port: 5432,
    user: "postgres",
    password: "soporte010203",
    database: "mihis",
    connectionTimeoutMillis: 30000, // 30 segundos (ajustable)
    idleTimeoutMillis: 30000,       // 30 segundos
    max: 20 ,
};

// Función para formatear la fecha (por ejemplo, de "DD/MM/YYYY" a "YYYY-MM-DD")
const formatSimpleDate = (date, inputFormat = "DD/MM/YYYY") => {
  const d = dayjs(date, inputFormat);
  return d.isValid() ? d.format("YYYY-MM-DD") : null;
};

/**
 * Procesa al paciente utilizando la sección "Resumen" del JSON.
 * Si se encuentra definido el rut, se busca el paciente por rut y se inserta (o se retorna el existente).
 * Si no se encuentra el rut, se inserta el paciente usando nombres, apellido paterno y apellido materno.
 */
async function procesarPaciente(dataResumen) {
  console.log(">> Procesando paciente...");
  const client = new Client(config);
  await client.connect();
  try {
    if (!dataResumen) {
      throw new Error("No se proporcionaron datos del paciente.");
    }

    // Si se proporciona el rut (y no está vacío)
    if (dataResumen.rut && dataResumen.rut.trim() !== "") {
      const pac_rut = dataResumen.rut.trim();
      const selectQuery = "SELECT pac_id FROM pacientes WHERE pac_rut = $1;";
      const resultSelect = await client.query(selectQuery, [pac_rut]);
      if (resultSelect.rowCount > 0) {
        console.log("Paciente ya existe en la BD.");
        return resultSelect.rows[0];
      } else {
        console.log("Paciente no encontrado, se procede a insertar (con rut).");
        const insertQuery = `
          INSERT INTO pacientes (pac_rut, pac_nombres, pac_appat, pac_fono)
          VALUES ($1, $2, $3, $4)
          RETURNING pac_id;
        `;
        const pac_nombres = dataResumen.nombresPac ? dataResumen.nombresPac.trim() : "";
        const pac_appat = dataResumen.apePatPac ? dataResumen.apePatPac.trim() : "";
        const pac_fono = "";
        const resultInsert = await client.query(insertQuery, [
          pac_rut,
          pac_nombres,
          pac_appat,
          pac_fono,
        ]);
        console.log("Paciente insertado en la BD (con rut).");
        return resultInsert.rows[0];
      }
    } else {
      // Si no se encuentra definido el rut, se inserta usando los otros campos
      console.log("No se encontró rut, se insertará paciente sin rut.");
      const insertPacQuery = "INSERT INTO pacientes (pac_nombres, pac_appat, pac_apmat) VALUES ($1, $2, $3) RETURNING pac_id;";
      const pac_nombres = dataResumen.nombresPac ? dataResumen.nombresPac.trim() : "";
      const pac_appat = dataResumen.apePatPac ? dataResumen.apePatPac.trim() : "";
      const pac_apmat = dataResumen.apeMatPac ? dataResumen.apeMatPac.trim() : "";
      const resultInsert = await client.query(insertPacQuery, [pac_nombres, pac_appat, pac_apmat]);
      console.log("Paciente insertado en la BD (sin rut).");
      return resultInsert.rows[0];
    }
  } catch (error) {
    console.error("Error al procesar paciente:", error);
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Crea un nuevo funcionario con el nombre proporcionado.
 * @param {string} nombre - El nombre del funcionario.
 * @returns {Object} - Objeto con la propiedad func_id del funcionario insertado.
 */
async function crearFuncionario(nombre) {
  const client = new Client(config);
  await client.connect();
  try {
    const insertQuery = "INSERT INTO funcionario (func_nombre) VALUES ($1) RETURNING func_id;";
    const result = await client.query(insertQuery, [nombre]);
    console.log(`Funcionario "${nombre}" creado.`);
    return result.rows[0];
  } catch (error) {
    console.error("Error al crear funcionario:", error);
    throw error;
  } finally {
    await client.end();
  }
}


/**
 * Busca al funcionario en la BD por su nombre.
 * Si no se encuentra, lo crea y retorna el registro.
 * @param {string} nombre - El nombre del funcionario.
 * @returns {Object} - Objeto con la propiedad func_id.
 */
async function procesarFuncionario(nombre) {
  if (!nombre || !nombre.trim()) {
    throw new Error("Nombre de funcionario inválido.");
  }
  const funcionarioNombre = nombre.trim();
  console.log(`>> Buscando funcionario: "${funcionarioNombre}"`);
  const client = new Client(config);
  await client.connect();
  try {
    const query = `SELECT func_id FROM funcionario WHERE func_nombre = $1;`;
    const result = await client.query(query, [funcionarioNombre]);
    if (result.rowCount > 0) {
      console.log(`Funcionario "${funcionarioNombre}" encontrado.`);
      return result.rows[0];
    } else {
      console.log(`Funcionario "${funcionarioNombre}" no encontrado en la BD. Se creará uno.`);
      // Cerrar el cliente actual para evitar conflictos y llamar a la función que crea el funcionario
      await client.end();
      return await crearFuncionario(funcionarioNombre);
    }
  } catch (error) {
    console.error("Error buscando funcionario:", error);
    throw error;
  } finally {
    // Si el cliente aún está abierto, ciérralo
    if (!client._ending) {
      await client.end();
    }
  }
}


/**
 * Inserta un registro en la tabla hospitalizacion.
 */
async function insertarHospitalizacion(hospData) {
  console.log(">> Insertando hospitalización...");
  const client = new Client(config);
  await client.connect();
  try {
    // Primero, eliminar registros dependientes en hospitalizacion_necesidades
    const deleteDependentQuery = `
      DELETE FROM hospitalizacion_necesidades
      WHERE hosp_id IN (
        SELECT hosp_id FROM hospitalizacion
        WHERE hosp_pac_id = $1 AND hosp_fecha_ing = $2 AND hosp_func_id = $3
      );
    `;
    await client.query(deleteDependentQuery, [
      hospData.hosp_pac_id,
      hospData.hosp_fecha_ing,
      hospData.hosp_func_id,
    ]);

    // Eliminar registro duplicado en hospitalizacion
    const deleteQuery = `
      DELETE FROM hospitalizacion
      WHERE hosp_pac_id = $1 AND hosp_fecha_ing = $2 AND hosp_func_id = $3;
    `;
    await client.query(deleteQuery, [
      hospData.hosp_pac_id,
      hospData.hosp_fecha_ing,
      hospData.hosp_func_id,
    ]);

    // Procesar el valor del servicio: si es "(Sin Asignar...)", "/" o cadena vacía, se asigna null.
    let servicioValue = hospData.hosp_servicio;
    if (!servicioValue || servicioValue.trim() === "" || servicioValue === "(Sin Asignar...)" || servicioValue.trim() === "/") {
      servicioValue = null;
    }

    // Preparar la consulta de inserción
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
    console.log("Hospitalización insertada:", result.rows[0]);
    return result.rows[0];
  } catch (error) {
    console.error("Error al insertar hospitalización:", error);
    throw error;
  } finally {
    await client.end();
  }
}


/**
 * Inserta un registro en la tabla hospitalizacion_observaciones.
 * Se verifica primero si existe un registro con el mismo (hosp_id, hospo_fecha, hospo_func_id)
 * y se elimina en caso afirmativo.
 */
async function insertarHospitalizacionObservaciones(obsData) {
  const client = new Client(config);
  await client.connect();
  try {
    // Eliminar registro duplicado si existe
    const deleteQuery = `
      DELETE FROM hospitalizacion_observaciones
      WHERE hosp_id = $1 AND hospo_fecha = $2 AND hospo_func_id = $3;
    `;
    await client.query(deleteQuery, [obsData.hosp_id, obsData.hospo_fecha, obsData.hospo_func_id]);

    const insertQuery = `
      INSERT INTO hospitalizacion_observaciones (
        hosp_id, hospo_fecha, hospo_observacion, hospo_func_id
      ) VALUES ($1, $2, $3, $4)
      RETURNING *;
    `;
    const values = [
      obsData.hosp_id,
      obsData.hospo_fecha,
      obsData.hospo_observacion,
      obsData.hospo_func_id,
    ];
    const result = await client.query(insertQuery, values);
    console.log("Registro insertado en hospitalizacion_observaciones:", result.rows[0]);
    return result.rows[0];
  } catch (error) {
    console.error("Error al insertar en hospitalizacion_observaciones:", error);
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Inserta múltiples registros en la tabla hospitalizacion_observaciones en una sola consulta.
 * Antes de la inserción en bloque, se elimina (uno por uno) cada registro duplicado según (hosp_id, hospo_fecha, hospo_func_id).
 * @param {Array} obsArray - Array de objetos con las propiedades:
 *   - hosp_id, hospo_fecha, hospo_observacion, hospo_func_id.
 * @returns {Array} - Array de registros insertados.
 */
async function insertarHospitalizacionObservacionesBulk(obsArray) {
  // Si no hay observaciones, retornamos un array vacío para evitar construir una consulta vacía.
  if (!obsArray || obsArray.length === 0) {
    console.log(">> No hay observaciones para insertar.");
    return [];
  }

  const client = new Client(config);
  await client.connect();
  try {
    // Eliminar duplicados uno por uno
    for (const obs of obsArray) {
      const deleteQuery = `
        DELETE FROM hospitalizacion_observaciones
        WHERE hosp_id = $1 AND hospo_fecha = $2 AND hospo_func_id = $3;
      `;
      await client.query(deleteQuery, [obs.hosp_id, obs.hospo_fecha, obs.hospo_func_id]);
    }

    // Construir la consulta de inserción masiva sin incluir un punto y coma final
    let query = "INSERT INTO hospitalizacion_observaciones (hosp_id, hospo_fecha, hospo_observacion, hospo_func_id) VALUES ";
    const values = [];
    const placeholders = obsArray.map((obs, index) => {
      const baseIndex = index * 4;
      values.push(
        obs.hosp_id,
        obs.hospo_fecha,
        obs.hospo_observacion,
        obs.hospo_func_id
      );
      return '(' + ['$' + (baseIndex + 1), '$' + (baseIndex + 2), '$' + (baseIndex + 3), '$' + (baseIndex + 4)].join(', ') + ')';
    });
    // Importante: No incluir el punto y coma final, ya que puede causar problemas en pg
    query += placeholders.join(", ") + " RETURNING *";
    
    const result = await client.query(query, values);
    console.log("Observaciones insertadas en masa:", result.rows);
    return result.rows;
  } catch (error) {
    console.error("Error al insertar en hospitalizacion_observaciones en masa:", error);
    throw error;
  } finally {
    await client.end();
  }
}


/**
 * Inserta un registro en la tabla de requerimiento ventilatorio.
 * Se elimina primero cualquier registro duplicado con la misma (req_vent_fecha, func_id, hosp_id).
 */
async function insertarReqVentilatorio(reqData) {
  console.log(">> Insertando requerimiento ventilatorio...");
  const client = new Client(config);
  await client.connect();

  // Diccionario de estados de COVID
  const diccionarioEstadoCovid = {
    1: "No Aplica",
    2: "Sospecha",
    3: "Positivo",
    4: "Negativo",
  };

  // Buscar la clave correspondiente al valor de reqData.vent_estado
  let estadoClave = Object.keys(diccionarioEstadoCovid).find(
    (key) => diccionarioEstadoCovid[key] === reqData.vent_estado
  );

  if (!estadoClave) {
    console.error("Error: Estado COVID no válido");
    throw new Error("Estado COVID no válido");
  }

  // Manejo de fecha
  let fechaConHora;
  try {
    let fecha = new Date(reqData.vent_fecha);
    if (isNaN(fecha.getTime())) {
      console.warn("⚠ Fecha inválida recibida, usando la fecha de hoy.");
      fecha = new Date();
    }
    const year = fecha.getFullYear();
    const month = String(fecha.getMonth() + 1).padStart(2, "0");
    const day = String(fecha.getDate()).padStart(2, "0");
    fechaConHora = `${year}-${month}-${day} 08:00:00`;
  } catch (error) {
    console.error("Error en el formato de la fecha:", error);
    throw new Error("Error procesando la fecha.");
  }

  // Recortar valores que puedan ser muy largos
  const vent_tipo_truncado = reqData.vent_tipo ? reqData.vent_tipo.substring(0, 15) : null;
  const cargo_truncado = reqData.cargo ? reqData.cargo.substring(0, 15) : null;

  try {
    // Eliminar registro duplicado si existe (criterio: req_vent_fecha, func_id, hosp_id)
    const deleteQuery = `
      DELETE FROM historial_requerimiento_vent
      WHERE req_vent_fecha = $1 AND func_id = $2 AND hosp_id = $3;
    `;
    await client.query(deleteQuery, [fechaConHora, reqData.func_id, reqData.hosp_id]);

    const query = `
      INSERT INTO historial_requerimiento_vent (req_vent_fecha, req_vent_desc, estado_covid, func_id, hosp_id)
      VALUES ($1, $2, $3, $4, $5)
      RETURNING req_vent_id;
    `;
    const values = [
      fechaConHora,
      vent_tipo_truncado,
      parseInt(estadoClave),
      reqData.func_id,
      reqData.hosp_id, // Usamos el hosp_id que se pasó en reqData
    ];
    const result = await client.query(query, values);
    console.log("Requerimiento ventilatorio insertado:", result.rows[0]);
    return result.rows[0];
  } catch (error) {
    console.error("Error al insertar requerimiento ventilatorio:", error);
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Inserta múltiples registros en la tabla hospitalizacion_registro en una sola consulta.
 * Para cada registro, se elimina primero cualquier duplicado (criterio: hreg_fecha, hest_id, hcon_id, hreg_func_id).
 * @param {Array} registros - Arreglo de objetos con las propiedades:
 *   hreg_fecha, hreg_estado, hreg_condicion, hreg_func_id.
 * @returns {Array} - Arreglo de registros insertados.
 */
async function insertarHospitalizacionRegistroBulk(registros) {
  // Si no hay registros, se retorna un array vacío para evitar armar una consulta inválida
  if (!registros || registros.length === 0) {
    console.log(">> No hay registros para insertar en hospitalizacion_registro.");
    return [];
  }

  const client = new Client(config);
  await client.connect();
  try {
    // Eliminar duplicados para cada registro
    for (const reg of registros) {
      const deleteQuery = `
        DELETE FROM hospitalizacion_registro
        WHERE hreg_fecha = $1 AND hest_id = $2 AND hcon_id = $3 AND hreg_func_id = $4;
      `;
      await client.query(deleteQuery, [
        reg.hreg_fecha ?? new Date(),
        reg.hreg_estado ?? 1,
        reg.hreg_condicion ?? 1,
        reg.hreg_func_id,
      ]);
    }

    // Construir la consulta de inserción masiva
    let query = "INSERT INTO hospitalizacion_registro (hreg_fecha, hest_id, hcon_id, hreg_func_id) VALUES ";
    const values = [];
    const placeholders = registros.map((reg, index) => {
      const baseIndex = index * 4;
      values.push(
        reg.hreg_fecha ?? new Date(),
        reg.hreg_estado ?? 1,
        reg.hreg_condicion ?? 1,
        reg.hreg_func_id
      );
      // Por ejemplo, para el primer registro genera: ($1, $2, $3, $4)
      return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4})`;
    });
    // Importante: no incluir punto y coma final, ya que pg no lo requiere
    query += placeholders.join(", ") + " RETURNING *";

    const result = await client.query(query, values);
    console.log("Registros insertados en hospitalizacion_registro:", result.rows);
    return result.rows;
  } catch (error) {
    console.error("Error al insertar en hospitalizacion_registro:", error);
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Crea un nuevo registro en clasifica_camas con el tipo especificado.
 * @param {string} tcama_tipo - El tipo de cama a crear.
 * @returns {Object} - Objeto con la propiedad tcama_id del registro insertado.
 */
async function crearCama(tcama_tipo) {
  const client = new Client(config);
  await client.connect();
  try {
    const insertQuery = "INSERT INTO clasifica_camas (tcama_tipo) VALUES ($1) RETURNING tcama_id;";
    const result = await client.query(insertQuery, [tcama_tipo]);
    console.log(`Cama creada con tipo "${tcama_tipo}".`);
    return result.rows[0];
  } catch (error) {
    console.error("Error al crear cama:", error);
    throw error;
  } finally {
    await client.end();
  }
}


/**
 * Inserta múltiples registros en la tabla paciente_traslado en una sola consulta.
 * Para cada registro se verifica (y se elimina si existe) usando el criterio: 
 * (ptras_fecha, hosp_id, ptras_cama_origen, ptras_func_id).
 * @param {Array} traslados - Arreglo de objetos con las propiedades:
 *   - fechaAsigTras: fecha asignada del traslado.
 *   - origenTras: cadena que contiene el origen, por ejemplo "Sala 1 - algo", de donde se extrae el primer texto.
 *   - funcTras: nombre del funcionario responsable del traslado.
 * @param {Number} hosp_id - Identificador de la hospitalización obtenido en el flujo.
 * @returns {Array} - Arreglo de registros insertados.
 */
async function insertarPacienteTrasladoBulk(traslados, hosp_id) {
  // Si no hay registros para insertar, retornamos un arreglo vacío
  if (!traslados || traslados.length === 0) {
    console.log(">> No hay registros de traslados para insertar.");
    return [];
  }

  const client = new Client(config);
  await client.connect();
  try {
    const processedRecords = [];

    // Procesar cada traslado
    for (const tras of traslados) {
      // Validaciones básicas
      if (!tras.fechaAsigTras) {
        throw new Error("El campo 'fechaAsigTras' es requerido en cada traslado.");
      }
      // if (!tras.origenTras) {
      //   throw new Error("El campo 'origenTras' es requerido en cada traslado.");
      // }
      if (!tras.funcTras || !tras.funcTras.trim()) {
        throw new Error("El campo 'funcTras' es requerido en cada traslado.");
      }

      // 1. ptras_fecha
      const ptras_fecha = tras.fechaAsigTras;

      // 2. ptras_cama_origen: extraemos el primer fragmento antes del guión.
      let origenTexto = tras.origenTras;
      // if (!origenTexto) {
      //   console.log(">> Advertencia: Se omite registro de traslado por falta de 'origenTras' (valor vacío).");
      //   continue; // omite este registro
      // }

      // Consulta para obtener tcama_id de la tabla clasifica_camas
      const queryCama = `SELECT tcama_id FROM clasifica_camas WHERE tcama_tipo = $1 LIMIT 1;`;
      let resCama = await client.query(queryCama, [origenTexto ?? "Hospitalizacion Domiciliaria"]);
      let ptras_cama_origen;
      if (resCama.rowCount === 0) {
        // Si no existe, se crea la cama
        const camaResult = await crearCama(origenTexto);
        ptras_cama_origen = camaResult.tcama_id;
      } else {
        ptras_cama_origen = resCama.rows[0].tcama_id;
      }

      // 3. ptras_cama_destino se asigna como 0
      const ptras_cama_destino = 0;

      // 4. ptras_func_id: se consulta la tabla "funcionario"
      const queryFunc = `SELECT func_id FROM funcionario WHERE func_nombre = $1 LIMIT 1;`;
      const resFunc = await client.query(queryFunc, [tras.funcTras.trim()]);
      if (resFunc.rowCount === 0) {
        throw new Error(`No se encontró funcionario con nombre "${tras.funcTras}".`);
      }
      const ptras_func_id = resFunc.rows[0].func_id;

      // Eliminar registro duplicado si existe (criterio: ptras_fecha, hosp_id, ptras_cama_origen, ptras_func_id)
      const deleteQuery = `
        DELETE FROM paciente_traslado
        WHERE ptras_fecha = $1 AND hosp_id = $2 AND ptras_cama_origen = $3 AND ptras_func_id = $4;
      `;
      await client.query(deleteQuery, [ptras_fecha, hosp_id, ptras_cama_origen, ptras_func_id]);

      // Se arma el objeto con los valores ya procesados
      processedRecords.push({
        ptras_fecha,
        hosp_id,
        ptras_cama_origen,
        ptras_cama_destino,
        ptras_func_id,
      });
    }

    // Si no quedan registros válidos para insertar, retornamos un arreglo vacío
    if (processedRecords.length === 0) {
      console.log(">> No hay registros válidos de traslados para insertar.");
      return [];
    }

    // Construir la consulta de inserción masiva sin incluir un punto y coma final
    let query =
      "INSERT INTO paciente_traslado (ptras_fecha, hosp_id, ptras_cama_origen, ptras_cama_destino, ptras_func_id) VALUES ";
    const values = [];
    const placeholders = processedRecords.map((record, index) => {
      const baseIndex = index * 5; // 5 valores por registro
      values.push(
        record.ptras_fecha,
        record.hosp_id,
        record.ptras_cama_origen,
        record.ptras_cama_destino,
        record.ptras_func_id
      );
      return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${baseIndex + 4}, $${baseIndex + 5})`;
    });
    query += placeholders.join(", ") + " RETURNING *";

    const result = await client.query(query, values);
    console.log("Registros insertados en paciente_traslado:", result.rows);
    return result.rows;
  } catch (error) {
    console.error("Error al insertar en paciente_traslado:", error);
    throw error;
  } finally {
    await client.end();
  }
}




// Función principal del proceso
async function Principal() {
  console.log(">> Iniciando proceso de carga masiva...");

  try {
    // Se procesa el Excel y se obtiene el objeto JSON (se asume que es un array)
    const dataExcels = JSON.parse(procesarExcels());

    // Recorrer cada excel encontrado
    for (let index = 0; index < dataExcels.length; index++) {
      console.log(`\n>> Procesando excel #${index}...`);
      console.log("Excel :", JSON.stringify(dataExcels[index]?.sheets, null, 2));

      // Verificamos que el excel tenga la propiedad "sheets"
      if (!dataExcels[index].sheets) {
        console.log(`>> El excel #${index} no contiene la propiedad 'sheets'. Se omite.`);
        continue;
      }

      const datosExcelDesignado = dataExcels[index].sheets;
      console.log(">> Datos del excel:", JSON.stringify(datosExcelDesignado, null, 2));

      // Verificar que exista la sección "Resumen"
      if (!datosExcelDesignado.Resumen) {
        console.log(`>> No se encontraron datos en la sección 'Resumen' del excel #${index}. Se omite este excel.`);
        continue;
      }
      console.log("datos de Resumen:", datosExcelDesignado.Resumen);

      // 1. Procesar paciente (sección "Resumen")
      const paciente = await procesarPaciente(datosExcelDesignado.Resumen);
      console.log(">> Paciente procesado:", paciente);

      // Variable para hosp_id, se reinicia para cada excel
      let hosp_id = null;

      // 2. Procesar hospitalización usando "DatosPaciente"
      if (datosExcelDesignado.DatosPaciente) {
        const datosPaciente = datosExcelDesignado.DatosPaciente;
        if (datosPaciente.funcAdmision && datosPaciente.funcAdmision.trim() !== "") {
          const funcionarioAdmision = await procesarFuncionario(datosPaciente.funcAdmision);
          if (funcionarioAdmision) {
            const hosp_fecha_ing = datosPaciente.admision
              ? formatSimpleDate(datosPaciente.admision, "DD/MM/YYYY")
              : null;
            const hospData = {
              hosp_fecha_ing,
              hosp_pac_id: paciente.pac_id,
              hosp_func_id: funcionarioAdmision.func_id,
              hosp_criticidad: datosExcelDesignado.Resumen?.categoria || null,
              hosp_diag_cod: datosExcelDesignado.Resumen?.codDiagnostico || null,
              hosp_diagnostico: datosExcelDesignado.Resumen?.diagnostico || null,
              hosp_servicio: datosExcelDesignado.Resumen?.servicio || null,
            };
            const resultadoHospitalizacion = await insertarHospitalizacion(hospData);
            console.log(">> Hospitalización insertada:", resultadoHospitalizacion);
            hosp_id = resultadoHospitalizacion.hosp_id;
          } else {
            console.log(">> El funcionario de admisión no existe en la BD. Se omitirá la inserción de hospitalización.");
          }
        } else {
          console.log(">> No se especifica funcionario de admisión en DatosPaciente. Se omitirá hospitalización.");
        }
      } else {
        console.log(">> No se encontró la sección DatosPaciente en el excel.");
      }

      // Si no se insertó hospitalización (hosp_id es null), omitimos las secciones dependientes
      if (hosp_id === null) {
        console.log(">> No se insertó hospitalización, por lo que se omiten ReqVentilatorio, Evolución y Traslados.");
        continue;
      }

      // 3. Procesar requerimiento ventilatorio (sección "ReqVentilatorio")
      if (datosExcelDesignado.ReqVentilatorio && Array.isArray(datosExcelDesignado.ReqVentilatorio)) {
        for (const rec of datosExcelDesignado.ReqVentilatorio) {
          if (rec.funcionarioVent && rec.funcionarioVent.trim() !== "") {
            const funcionarioReq = await procesarFuncionario(rec.funcionarioVent);
            if (funcionarioReq) {
              const reqData = {
                vent_fecha: rec.fechaVent,
                vent_tipo: rec.tipoReqVent,
                vent_estado: rec.estadoCovidVent,
                func_id: funcionarioReq.func_id,
                cargo: rec.cargoFuncVent,
                hosp_id: hosp_id,
              };
              await insertarReqVentilatorio(reqData);
            } else {
              console.log(`>> Funcionario "${rec.funcionarioVent}" no encontrado. Se omitirá este registro de requerimiento ventilatorio.`);
            }
          } else {
            console.log(">> No se especifica funcionarioVent en el registro de ReqVentilatorio. Registro omitido.");
          }
        }
      } else {
        console.log(">> No se encontró la sección ReqVentilatorio o no es un array en el excel.");
      }

      // 2.2 Procesar evolución de estado en masa (hospitalizacion_registro)
      if (datosExcelDesignado.EvolucionEstado) {
        const evolucionEstado = Array.isArray(datosExcelDesignado.EvolucionEstado)
          ? datosExcelDesignado.EvolucionEstado
          : [datosExcelDesignado.EvolucionEstado];
        await insertarHospitalizacionRegistroBulk(evolucionEstado);
      }

      // 4. Procesar traslados (sección "Traslados")
      if (datosExcelDesignado.Traslados) {
        const traslados = Array.isArray(datosExcelDesignado.Traslados)
          ? datosExcelDesignado.Traslados
          : [datosExcelDesignado.Traslados];

        // Se inserta en la tabla paciente_traslado y se captura el resultado.
        // En la función insertarPacienteTrasladoBulk, si falta 'fechaAsigTras' en algún traslado,
        // se omite ese registro (en lugar de lanzar error).
        console.log("datos del traslado", traslados)
        const trasladosInsertados = await insertarPacienteTrasladoBulk(traslados, hosp_id);
        console.log(">> Datos insertados en paciente_traslado:", JSON.stringify(trasladosInsertados, null, 2));
      } else {
        console.log(">> No se encontró la sección Traslados en el excel.");
      }

      console.log(`>> Excel #${index} procesado correctamente.\n`);
    }

    console.log(">> PROCESO FINALIZADO CORRECTAMENTE PARA TODOS LOS EXCELS.");
  } catch (error) {
    console.error(">> Error en el proceso general:", error);
  }
}

// Ejecutar la función principal
Principal();