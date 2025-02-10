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
 * Si no se obtiene el rut en Resumen, se intenta obtener desde DatosPaciente.
 */
async function procesarPaciente(dataResumen, dataPaciente) {
  console.log(">> Procesando paciente...");

  const pac_rut =
    dataResumen && dataResumen.rut && dataResumen.rut.trim() !== ""
      ? dataResumen.rut.trim()
      : dataPaciente && dataPaciente.rut && dataPaciente.rut.trim() !== ""
      ? dataPaciente.rut.trim()
      : null;

  const pac_nombres =
    dataResumen &&
    dataResumen.nombresPac &&
    dataResumen.nombresPac.trim() !== ""
      ? dataResumen.nombresPac.trim()
      : dataPaciente &&
        dataPaciente.nombrePac &&
        dataPaciente.nombrePac.trim() !== ""
      ? dataPaciente.nombrePac.trim()
      : "";

  const pac_appat =
    dataResumen && dataResumen.apePatPac && dataResumen.apePatPac.trim() !== ""
      ? dataResumen.apePatPac.trim()
      : "";

  const pac_fono = "";

  const client = new Client(config);
  await client.connect();
  try {
    if (pac_rut) {
      const selectQuery = "SELECT pac_id FROM pacientes WHERE pac_rut = $1;";
      const resultSelect = await client.query(selectQuery, [pac_rut]);
      if (resultSelect.rowCount > 0) {
        console.log("Paciente ya existe en la BD.");
        return resultSelect.rows[0];
      }
    }

    console.log("Paciente no encontrado o sin rut, se procede a insertar.");
    const insertQuery = `
      INSERT INTO pacientes (pac_rut, pac_nombres, pac_appat, pac_fono)
      VALUES ($1, $2, $3, $4)
      RETURNING pac_id;
    `;
    const resultInsert = await client.query(insertQuery, [
      pac_rut,
      pac_nombres,
      pac_appat,
      pac_fono,
    ]);
    console.log("Paciente insertado en la BD.");
    return resultInsert.rows[0];
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

    let servicioValue = hospData.hosp_servicio;
    if (servicioValue === "(Sin Asignar...)" || !servicioValue) {
      servicioValue = null;
    } else if (typeof servicioValue === "string") {
      servicioValue = servicioValue.trim();
      if (/^\d+$/.test(servicioValue)) {
        servicioValue = parseInt(servicioValue, 10);
      } else {
        console.error(
          `El valor para hosp_servicio no es numérico: "${servicioValue}". Se asignará null.`
        );
        servicioValue = null;
      }
    }

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
 * Inserta múltiples registros en la tabla hospitalizacion_observaciones en una sola consulta.
 */
async function insertarHospitalizacionObservacionesBulk(obsArray) {
  if (!obsArray || obsArray.length === 0) {
    console.log(
      ">> No hay observaciones para insertar en hospitalizacion_observaciones."
    );
    return [];
  }
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
    console.log(">> Observaciones insertadas en masa:", result.rows);
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
 * Inserta múltiples registros en la tabla hospitalizacion_necesidades en una sola consulta.
 */
async function insertarHospitalizacionNecesidadesBulk(neceArray) {
  if (!neceArray || neceArray.length === 0) {
    console.log(
      ">> No hay registros para insertar en hospitalizacion_necesidades."
    );
    return [];
  }
  const client = new Client(config);
  await client.connect();
  try {
    let query = `INSERT INTO hospitalizacion_necesidades (hosp_id, hospn_fecha, hospn_observacion, hospn_func_id) VALUES `;
    const values = [];
    const placeholders = neceArray.map((nece, index) => {
      const baseIndex = index * 4;
      values.push(
        nece.hosp_id,
        nece.hospon_fecha,
        nece.hospon_observacion,
        nece.hospon_func_id
      );
      return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${
        baseIndex + 4
      })`;
    });
    query += placeholders.join(", ") + " RETURNING *;";
    const result = await client.query(query, values);
    console.log(">> Necesidades insertadas en masa:", result.rows);
    return result.rows;
  } catch (error) {
    console.error(
      "Error al insertar en hospitalizacion_necesidades en masa:",
      error
    );
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Inserta múltiples registros en la tabla hospitalizacion_registro en una sola consulta.
 */
async function insertarHospitalizacionRegistroBulk(registros) {
  if (!registros || registros.length === 0) {
    console.log(
      ">> No hay registros para insertar en hospitalizacion_registro."
    );
    return [];
  }
  const client = new Client(config);
  await client.connect();
  try {
    let query =
      "INSERT INTO hospitalizacion_registro (hreg_fecha, hest_id, hcon_id, hreg_func_id) VALUES ";
    const values = [];
    const placeholders = registros.map((reg, index) => {
      const baseIndex = index * 4;
      values.push(
        reg.hreg_fecha ?? new Date(),
        reg.hreg_estado ?? 1,
        reg.hreg_condicion ?? 1,
        reg.hreg_func_id
      );
      return `($${baseIndex + 1}, $${baseIndex + 2}, $${baseIndex + 3}, $${
        baseIndex + 4
      })`;
    });
    query += placeholders.join(", ") + " RETURNING *;";
    const result = await client.query(query, values);
    console.log(
      ">> Registros insertados en hospitalizacion_registro:",
      result.rows
    );
    return result.rows;
  } catch (error) {
    console.error("Error al insertar en hospitalizacion_registro:", error);
    throw error;
  } finally {
    await client.end();
  }
}

/**
 * Inserta un registro en la tabla censo_diario.
 */
async function insertarCensoDiario(censoData) {
  console.log(">> Insertando censo diario...");
  const client = new Client(config);
  await client.connect();
  try {
    const query = `
      INSERT INTO censo_diario (censo_fecha, censo_diario, func_id, hosp_id)
      VALUES ($1, $2, $3, $4)
      RETURNING censo_id;
    `;
    const values = [
      censoData.censo_fecha,
      censoData.censo_diario,
      censoData.func_id,
      censoData.hosp_id,
    ];
    const result = await client.query(query, values);
    console.log(">> Censo diario insertado:", result.rows[0]);
    return result.rows[0];
  } catch (error) {
    console.error("Error al insertar censo diario:", error);
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
  const diccionarioEstadoCovid = {
    1: "No Aplica",
    2: "Sospecha",
    3: "Positivo",
    4: "Negativo",
  };
  let estadoClave = Object.keys(diccionarioEstadoCovid).find(
    (key) => diccionarioEstadoCovid[key] === reqData.vent_estado
  );
  if (!estadoClave) {
    console.error("Error: Estado COVID no válido");
    throw new Error("Estado COVID no válido");
  }
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
      reqData.hosp_id,
    ];
    const result = await client.query(query, values);
    console.log(">> Requerimiento ventilatorio insertado:", result.rows[0]);
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
 * Recorre los primeros 300 JSON (sheets) obtenidos del Excel y procesa cada uno.
 * Se insertan los datos de paciente, hospitalización, observaciones, necesidades,
 * evolución de estado, requerimiento ventilatorio y censo diario.
 * Al final de cada iteración se muestra un log resumen con los datos insertados.
 */
async function Principal() {
  console.log(">> Iniciando proceso de carga masiva...");
  const dataExcels = JSON.parse(procesarExcels());
  // Para pruebas, se procesan solo los primeros 300 elementos
  const sheetsToProcess = dataExcels.slice(0, 300);

  for (let i = 0; i < sheetsToProcess.length; i++) {
    console.log(
      `\n>> Procesando sheet ${i + 1} de ${sheetsToProcess.length}...`
    );
    try {
      const datosExcelDesignado = sheetsToProcess[i].sheets;

      // 1. Procesar paciente (sección "Resumen" y "DatosPaciente")
      const paciente = await procesarPaciente(
        datosExcelDesignado.Resumen,
        datosExcelDesignado.DatosPaciente
      );
      if (!paciente) {
        console.warn(
          ">> Se omitirá el procesamiento de este sheet porque no se encontró el paciente."
        );
        continue;
      }
      console.log(">> Paciente procesado:", paciente);

      let hosp_id = null;
      let funcionarioAdmision = null;

      // 2. Procesar hospitalización usando "DatosPaciente"
      if (datosExcelDesignado.DatosPaciente) {
        const datosPaciente = datosExcelDesignado.DatosPaciente;
        if (
          datosPaciente.funcAdmision &&
          datosPaciente.funcAdmision.trim() !== ""
        ) {
          funcionarioAdmision = await procesarFuncionario(
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
              hosp_diag_cod:
                datosExcelDesignado.Resumen?.codDiagnostico || null,
              hosp_diagnostico:
                datosExcelDesignado.Resumen?.diagnostico || null,
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

      // 3. Procesar observaciones (HistObserv)
      if (datosExcelDesignado.HistObserv) {
        const observaciones = Array.isArray(datosExcelDesignado.HistObserv)
          ? datosExcelDesignado.HistObserv
          : [datosExcelDesignado.HistObserv];
        const obsData = observaciones.map((obs) => ({
          hosp_id: hosp_id,
          hospo_fecha: obs.historialHistObserv
            ? dayjs(obs.historialHistObserv).format("YYYY-MM-DD")
            : null,
          hospo_observacion: obs.evolucionHistObserv || null,
          hospo_func_id: funcionarioAdmision
            ? funcionarioAdmision.func_id
            : null,
        }));
        const insertedObservaciones =
          await insertarHospitalizacionObservacionesBulk(obsData);
        console.log(">> Observaciones insertadas:", insertedObservaciones);
      }

      // 4. Procesar necesidades (HistNecesidades)
      if (datosExcelDesignado.HistNecesidades) {
        const necesidades = Array.isArray(datosExcelDesignado.HistNecesidades)
          ? datosExcelDesignado.HistNecesidades
          : [datosExcelDesignado.HistNecesidades];
        const necData = necesidades.map((nec) => {
          let fecha = nec.fechaHistNec ? dayjs(nec.fechaHistNec) : null;
          return {
            hosp_id: hosp_id,
            hospon_fecha:
              fecha && fecha.isValid() ? fecha.format("YYYY-MM-DD") : null,
            hospon_observacion: nec.indicacionesHistNec || null,
            hospon_func_id: nec.hospon_func_id || null,
          };
        });
        const insertedNecesidades =
          await insertarHospitalizacionNecesidadesBulk(necData);
        console.log(">> Necesidades insertadas:", insertedNecesidades);
      }

      // 5. Procesar evolución de estado (hospitalizacion_registro)
      if (datosExcelDesignado.EvolucionEstado) {
        const evolucionEstado = Array.isArray(
          datosExcelDesignado.EvolucionEstado
        )
          ? datosExcelDesignado.EvolucionEstado
          : [datosExcelDesignado.EvolucionEstado];
        const registrosInsertados = await insertarHospitalizacionRegistroBulk(
          evolucionEstado
        );
        console.log(
          ">> Evolución de estado insertada en hospitalizacion_registro:",
          registrosInsertados
        );
      } else {
        console.warn(
          ">> No se encontraron datos en EvolucionEstado para hospitalizacion_registro."
        );
      }

      // 6. Procesar requerimiento ventilatorio (ReqVentilatorio)
      if (datosExcelDesignado.ReqVentilatorio) {
        const reqVent = Array.isArray(datosExcelDesignado.ReqVentilatorio)
          ? datosExcelDesignado.ReqVentilatorio
          : [datosExcelDesignado.ReqVentilatorio];
        for (let req of reqVent) {
          if (req.funcionarioVent && req.funcionarioVent.trim() !== "") {
            const funcionarioVent = await procesarFuncionario(
              req.funcionarioVent
            );
            if (funcionarioVent) {
              const reqData = {
                vent_fecha: req.fechaVent,
                vent_tipo: req.tipoReqVent,
                vent_estado: req.estadoCovidVent,
                func_id: funcionarioVent.func_id,
                hosp_id: hosp_id,
              };
              const insertedReq = await insertarReqVentilatorio(reqData);
              console.log(
                ">> Requerimiento ventilatorio insertado:",
                insertedReq
              );
            } else {
              console.warn(
                `>> Funcionario para ventilatorio "${req.funcionarioVent}" no encontrado. Registro omitido.`
              );
            }
          } else {
            console.warn(
              ">> No se especifica funcionarioVent en ReqVentilatorio. Registro omitido."
            );
          }
        }
      } else {
        console.warn(">> No se encontró la sección ReqVentilatorio.");
      }

      // 7. Procesar censo diario
      if (
        datosExcelDesignado.EvolucionEstado &&
        datosExcelDesignado.Resumen &&
        datosExcelDesignado.Resumen.categoria
      ) {
        console.log(">> Procesando censo diario...");
        let censo_fecha = null;
        const rawFechaCenso =
          datosExcelDesignado.EvolucionEstado["fechaEvoEst"];
        if (rawFechaCenso) {
          const parsedFecha = dayjs(rawFechaCenso);
          if (parsedFecha.isValid()) {
            censo_fecha = parsedFecha.format("YYYY-MM-DD HH:mm:ss");
          } else {
            console.warn(
              `La fecha "${rawFechaCenso}" de EvolucionEstado no es válida. Se asignará null.`
            );
          }
        } else {
          console.warn(
            "No se encontró la fecha en EvolucionEstado para el censo diario."
          );
        }
        const censo_diario = datosExcelDesignado.Resumen.categoria.trim();
        const func_id = funcionarioAdmision
          ? funcionarioAdmision.func_id
          : null;
        if (!hosp_id) {
          console.warn(
            "No se encontró hospitalización (hosp_id) para el censo diario. No se insertará el censo diario."
          );
        } else {
          const censoData = { censo_fecha, censo_diario, func_id, hosp_id };
          const resultadoCenso = await insertarCensoDiario(censoData);
          console.log(">> Censo diario insertado:", resultadoCenso);
        }
      } else {
        console.warn(
          ">> No se encontraron datos suficientes para procesar censo diario."
        );
      }

      // Log resumen de la hospitalización completa insertada en este sheet
      console.log(">> Resumen de datos insertados en este sheet:");
      console.log({
        sheet: i + 1,
        paciente: paciente,
        hospitalizacion: hosp_id
          ? { hosp_id }
          : "No se insertó hospitalización",
        funcionarioAdmision: funcionarioAdmision
          ? funcionarioAdmision
          : "No se insertó hospitalización",
      });
    } catch (error) {
      console.error(`>> Error procesando sheet ${i + 1}:`, error);
    }
  }
  console.log(">> PROCESO FINALIZADO CORRECTAMENTE.");
}

// Ejecutar la función principal
Principal();
