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
};

// Función para formatear la fecha (por ejemplo, de "DD/MM/YYYY" a "YYYY-MM-DD")
const formatSimpleDate = (date, inputFormat = "DD/MM/YYYY") => {
  const d = dayjs(date, inputFormat);
  return d.isValid() ? d.format("YYYY-MM-DD") : null;
};

/**
 * Procesa al paciente utilizando la sección "Resumen" del JSON.
 * Si ya existe (por RUT) se retorna el registro; de lo contrario, se inserta.
 */
async function procesarPaciente(dataResumen) {
  console.log(">> Procesando paciente...");
  const client = new Client(config);
  await client.connect();
  try {
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
      const pac_fono = "";
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

/**
 * Busca al funcionario en la BD por su nombre.
 * Si no se encuentra, retorna null.
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
      console.log(`Funcionario "${funcionarioNombre}" no encontrado en la BD.`);
      return null;
    }
  } catch (error) {
    console.error("Error buscando funcionario:", error);
    throw error;
  } finally {
    await client.end();
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
    // Si el servicio es "(Sin Asignar...)" se asigna null
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
 * (Función antigua para inserción individual, se mantiene si se necesita)
 */
async function insertarHospitalizacionObservaciones(obsData) {
  const client = new Client(config);
  await client.connect();
  try {
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
    console.log(
      "Registro insertado en hospitalizacion_observaciones:",
      result.rows[0]
    );
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
 * @param {Array} obsArray - Array de objetos con las propiedades:
 *   - hosp_id, hospo_fecha, hospo_observacion, hospo_func_id.
 * @returns {Array} - Array de registros insertados.
 */
async function insertarHospitalizacionObservacionesBulk(obsArray) {
  const client = new Client(config);
  await client.connect();
  try {
    let query = `INSERT INTO hospitalizacion_observaciones (hosp_id, hospo_fecha, hospo_observacion, hospo_func_id) VALUES `;
    const values = [];
    const placeholders = obsArray.map((obs, index) => {
      const baseIndex = index * 4;
      values.push(
        obs.hosp_id,
        obs.hospo_fecha,
        obs.hospo_observacion,
        obs.hospo_func_id
      );
      return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${
        baseIndex + 4
      })`;
    });
    query += placeholders.join(", ") + " RETURNING *;";
    const result = await client.query(query, values);
    console.log("Observaciones insertadas en masa:", result.rows);
    return result.rows;
  } catch (error) {
    console.error(
      "Error al insertar en hospitalizacion_observaciones en masa:",
      error
    );
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Inserta un registro en la tabla de requerimiento ventilatorio.
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
  const vent_tipo_truncado = reqData.vent_tipo
    ? reqData.vent_tipo.substring(0, 15)
    : null;
  const cargo_truncado = reqData.cargo ? reqData.cargo.substring(0, 15) : null;

  try {
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
 * Función principal.
 * Se procesa el JSON obtenido del Excel y se realizan las inserciones correspondientes.
 */
async function Principal() {
  console.log(">> Iniciando proceso de carga masiva...");
  let hosp_id = null; // Variable para hosp_id

  try {
    // Se procesa el Excel y se obtiene el objeto JSON
    const dataExcels = JSON.parse(procesarExcels());
    const datosExcelDesignado = dataExcels[403].sheets;
    console.log(
      ">> Datos del excel obtenidos:",
      JSON.stringify(datosExcelDesignado, null, 2)
    );

    // 1. Procesar paciente (sección "Resumen")
    const paciente = await procesarPaciente(datosExcelDesignado.Resumen);
    console.log(">> Paciente procesado:", paciente);

    // 2. Procesar hospitalización usando "DatosPaciente"
    if (datosExcelDesignado.DatosPaciente) {
      const datosPaciente = datosExcelDesignado.DatosPaciente;
      if (
        datosPaciente.funcAdmision &&
        datosPaciente.funcAdmision.trim() !== ""
      ) {
        const funcionarioAdmision = await procesarFuncionario(
          datosPaciente.funcAdmision
        );
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
          const resultadoHospitalizacion = await insertarHospitalizacion(
            hospData
          );
          console.log(
            ">> Hospitalización insertada:",
            resultadoHospitalizacion
          );
          hosp_id = resultadoHospitalizacion.hosp_id;

          // 2.1 Procesar observaciones (HistObserv) en masa
          if (datosExcelDesignado.HistObserv) {
            // Si HistObserv no es un array, lo convertimos en uno
            const observaciones = Array.isArray(datosExcelDesignado.HistObserv)
              ? datosExcelDesignado.HistObserv
              : [datosExcelDesignado.HistObserv];

            // Obtener el funcionario para las observaciones (usando el mismo funcAdmision)
            let hospo_func_id = null;
            if (datosPaciente.funcAdmision) {
              const funcionarioObs = await procesarFuncionario(
                datosPaciente.funcAdmision
              );
              hospo_func_id = funcionarioObs ? funcionarioObs.func_id : null;
            }

            // Mapear cada observación al formato requerido
            const observacionesData = observaciones.map((obs) => ({
              hosp_id: hosp_id,
              hospo_fecha: obs.historialHistObserv || null,
              hospo_observacion: obs.evolucionHistObserv || null,
              hospo_func_id: hospo_func_id,
            }));

            const resultadoObservaciones =
              await insertarHospitalizacionObservacionesBulk(observacionesData);
            console.log(
              "Observaciones de hospitalización insertadas:",
              resultadoObservaciones
            );
          }
        } else {
          console.log(
            ">> El funcionario de admisión no existe en la BD. Se omitirá la inserción de hospitalización."
          );
        }
      } else {
        console.log(
          ">> No se especifica funcionario de admisión en DatosPaciente. Se omitirá hospitalización."
        );
      }
    } else {
      console.log(">> No se encontró la sección DatosPaciente en el JSON.");
    }

    // 3. Procesar requerimiento ventilatorio (sección "ReqVentilatorio")
    if (
      datosExcelDesignado.ReqVentilatorio &&
      Array.isArray(datosExcelDesignado.ReqVentilatorio)
    ) {
      for (const rec of datosExcelDesignado.ReqVentilatorio) {
        if (rec.funcionarioVent && rec.funcionarioVent.trim() !== "") {
          const funcionarioReq = await procesarFuncionario(rec.funcionarioVent);
          if (funcionarioReq) {
            const reqData = {
              vent_fecha: rec.fechaVent, // Se asume que la fecha viene en el formato adecuado
              vent_tipo: rec.tipoReqVent,
              vent_estado: rec.estadoCovidVent,
              func_id: funcionarioReq.func_id,
              cargo: rec.cargoFuncVent,
              hosp_id: hosp_id, // Utilizamos el hosp_id obtenido anteriormente
            };
            await insertarReqVentilatorio(reqData);
          } else {
            console.log(
              `>> Funcionario "${rec.funcionarioVent}" no encontrado. Se omitirá este registro de requerimiento ventilatorio.`
            );
          }
        } else {
          console.log(
            ">> No se especifica funcionarioVent en el registro de ReqVentilatorio. Registro omitido."
          );
        }
      }
    } else {
      console.log(
        ">> No se encontró la sección ReqVentilatorio o no es un array."
      );
    }
    console.log(">> PROCESO FINALIZADO CORRECTAMENTE.");
  } catch (error) {
    console.error(">> Error en el proceso general:", error);
  }
}

// Ejecutar la función principal
Principal();
