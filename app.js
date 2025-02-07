    // app.js
    const dayjs = require('dayjs');
    require('dayjs/locale/es'); // Importamos el idioma español
    const customParseFormat = require('dayjs/plugin/customParseFormat');
    dayjs.extend(customParseFormat);
    dayjs.locale('es'); // Usamos el español globalmente

    const { Client } = require('pg');
    const procesarExcels = require('./botCargaMasiva/bot');

    // Configuración de conexión a PostgreSQL
    const config = {
    host: "10.4.199.133",
    port: 5432,
    user: "postgres",
    password: "soporte010203",
    database: "mihis",
    };

    // Función para formatear la fecha (por ejemplo, de "DD/MM/YYYY" a "YYYY-MM-DD")
    const formatSimpleDate = (date, inputFormat = 'DD/MM/YYYY') => {
    const d = dayjs(date, inputFormat);
    return d.isValid() ? d.format('YYYY-MM-DD') : null;
    };

    // Ejemplo de función para consultar el paciente (se puede modificar para asignar null a datos faltantes)
    // Función para procesar el paciente a partir de los datos del JSON (objeto Resumen)
    // Función para procesar el paciente a partir de los datos del JSON (objeto Resumen)
    async function procesarPaciente(dataResumen) {
        const client = new Client(config);
        await client.connect();
        try {
        // Primero se consulta si existe el paciente, usando el RUT
        const selectQuery = 'SELECT pac_id FROM pacientes WHERE pac_rut = $1;';
        const resultSelect = await client.query(selectQuery, [dataResumen.rut]);
        
        if (resultSelect.rowCount > 0) {
            // El paciente ya existe, retornamos el registro
            return resultSelect.rows[0];
        } else {
            // Si no existe, se procede a insertar un nuevo paciente.
            // Se usan los datos que vienen en el objeto "Resumen".
            // Se asigna el apellido paterno (apePatPac) al campo pac_appat.
            const insertQuery = `
            INSERT INTO pacientes (pac_rut, pac_nombres, pac_appat, pac_fono)
            VALUES ($1, $2, $3, $4)
            RETURNING pac_id;
            `;
            const pac_rut = dataResumen.rut ? dataResumen.rut.trim() : null;
            const pac_nombres = dataResumen.nombresPac ? dataResumen.nombresPac.trim() : '';
            const pac_appat = dataResumen.apePatPac ? dataResumen.apePatPac.trim() : '';
            const pac_fono = ''; // No se encontró información telefónica en el JSON
    
            const resultInsert = await client.query(insertQuery, [pac_rut, pac_nombres, pac_appat, pac_fono]);
            return resultInsert.rows[0];
        }
        } catch (error) {
        throw error;
        } finally {
        await client.end();
        }
    }
    /**
     * ===============================================================
     * BLOQUE: Extracción e inserción de funcionarios desde el JSON
     * ===============================================================
     */

    /**
     * Función recursiva para recorrer cualquier objeto (u arreglo) y extraer
     * los valores de las propiedades cuyas claves comienzan con "func".
     * Se usan en un Set para evitar duplicados.
     */
    function extraerFuncionarios(obj) {
    const funcionarios = new Set();

    const recorrer = (valor) => {
        if (valor && typeof valor === 'object') {
        if (Array.isArray(valor)) {
            for (const item of valor) {
            recorrer(item);
            }
        } else {
            for (const key in valor) {
            if (Object.hasOwnProperty.call(valor, key)) {
                // Si la clave inicia con "func" y el valor es una cadena no vacía
                if (
                key.startsWith('func') &&
                typeof valor[key] === 'string' &&
                valor[key].trim() !== ''
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
     * Función que consulta si existe un funcionario (identificado por su "func_rut")
     * y, si no existe, lo inserta.  
     * Si algún dato (como el nombre, cargo, clave o fecha) no está definido, se le asigna null o cadena vacía.
     */
    async function procesarFuncionario(funcData) {
        // Extraemos y validamos el nombre (criterio para saber si es válido)
        const nombre = funcData.nombre ? funcData.nombre.trim() : null;
        if (!nombre) {
          console.log("Funcionario no válido, se asignará null.");
          return null;
        }
        // Extraemos los campos opcionales (si no existen, se asigna null)
        const cargo    = funcData.cargo    ? funcData.cargo.trim()    : null;
        const clave    = funcData.clave    ? funcData.clave.trim()    : null;
        const email    = funcData.email    ? funcData.email.trim()    : null;
        const servicio = funcData.servicio ? funcData.servicio.trim() : null;
        const ultimoLog = new Date();
      
        const client = new Client(config);
        await client.connect();
        try {
          // Se consulta si el funcionario ya existe (por nombre en este ejemplo)
          const selectQuery = `
            SELECT func_id, func_cargo, func_clave, func_email, func_servicio 
            FROM funcionario 
            WHERE func_nombre = $1;
          `;
          const resultSelect = await client.query(selectQuery, [nombre]);
          if (resultSelect.rowCount > 0) {
            // Ya existe: retornamos el registro completo (con los datos opcionales)
            return resultSelect.rows[0];
          } else {
            // Si no existe, se inserta el funcionario con todos los datos disponibles
            const insertQuery = `
              INSERT INTO funcionario (
                func_nombre, func_cargo, func_clave, func_email, func_servicio, func_ultimo_log
              ) VALUES ($1, $2, $3, $4, $5, $6)
              RETURNING func_id, func_cargo, func_clave, func_email, func_servicio;
            `;
            const resultInsert = await client.query(insertQuery, [
              nombre,
              cargo,
              clave,
              email,
              servicio,
              ultimoLog
            ]);
            return resultInsert.rows[0];
          }
        } catch (error) {
          // Si ocurre un error de duplicado, capturamos el error y realizamos una consulta para obtener el registro existente.
          if (error.code === '23505') { // 23505: duplicate key violation
            console.warn(`Duplicate detected for funcionario "${nombre}". Recuperando el registro existente.`);
            const selectQuery = `
              SELECT func_id, func_cargo, func_clave, func_email, func_servicio 
              FROM funcionario 
              WHERE func_nombre = $1;
            `;
            const resultSelect = await client.query(selectQuery, [nombre]);
            if (resultSelect.rowCount > 0) {
              return resultSelect.rows[0];
            }
          }
          // Si el error no es de duplicado o la consulta no devuelve nada, se lanza el error
          throw error;
        } finally {
          await client.end();
        }
      }
      

    /**
     * ===============================================================
     * FIN BLOQUE FUNCIONARIOS
     * ===============================================================
     */


      /* inicio consultar por el nombre del paciente */

      async function obtenerFuncionarioIdPorNombre(nombre) {
        if (!nombre || !nombre.trim()) {
          return null;
        }
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
            return result.rows[0].func_id;
          } else {
            console.log(`No se encontró funcionario con nombre similar a: ${nombre}`);
            return null;
          }
        } catch (error) {
          throw error;
        } finally {
          await client.end();
        }
      }

      /* fin consultar por el nombre del paciente */


      /*Inicio Insert Traslado_paciente*/

      async function insertarPacienteTraslado(trasladoData) {
        const client = new Client(config);
        await client.connect();
        console.log("Datos de la info de traslado:" + JSON.stringify(trasladoData));
        try {
          // Supongamos que ahora la tabla paciente_traslado incluye el campo ptras_pac_id
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
            trasladoData.ptras_cama_destino === "(Sin Asignar...)" ? 0 : trasladoData.ptras_cama_destino,
            trasladoData.ptras_func_id,
            trasladoData.hosp_id
          ];
          const result = await client.query(insertQuery, values);
          return result.rows[0];
        } catch (error) {
          throw error;
        } finally {
          await client.end();
        }
      }
      /* fin insert traslado paciente */



    /**
     * ===============================================================
     * NUEVO BLOQUE: Inserción en la tabla hospitalizacion
     * ===============================================================
     */
    async function insertarHospitalizacion(hospData) {
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
    
        // Verificamos si el servicio es un valor no numérico (por ejemplo, "(Sin Asignar...)")
        // y en ese caso asignamos null
        const servicioValue = (hospData.hosp_servicio === "(Sin Asignar...)") ? null : hospData.hosp_servicio;
    
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
        console.log("resultado de inerts hosp: ", result)
        return result.rows[0];
        } catch (error) {
        throw error;
        } finally {
        await client.end();
        }
    }
    /**
     * ===============================================================
     * FIN BLOQUE: Inserción en hospitalizacion
     * ===============================================================
     */

    // Función principal del bot que orquesta todo el proceso
  async function Principal() {
    try {
        // Se procesa el excel y se obtiene el objeto JSON
        const dataExcels = JSON.parse(procesarExcels());    
        // Seleccionamos la hoja que deseamos (en este ejemplo, la hoja del segundo excel)
        const datosExcelDesignado = dataExcels[9].sheets;

        console.log("Datos del excel:", datosExcelDesignado);

        // Construcción del objeto paciente usando coalescencia nula y optional chaining
        // 1. Consultamos y procesamos el paciente
        const paciente = await procesarPaciente(datosExcelDesignado.Resumen);
        console.log('Paciente procesado:', paciente);
        // 2. Extraemos los funcionarios que se encuentren en cualquier parte del JSON.
        //    Se buscarán todas las propiedades que comiencen con "func".
        const funcionariosEncontrados = extraerFuncionarios(datosExcelDesignado);
        console.log('Funcionarios encontrados en el JSON:', funcionariosEncontrados);

        // Para cada funcionario extraído, se consulta (o inserta) en la BD.
        for (const func of funcionariosEncontrados) {
        try {
            const funcionarioDB = await procesarFuncionario(func);
            console.log(`Funcionario procesado (${func}): ${JSON.stringify(funcionarioDB)}`);
        } catch (error) {
            console.error(`Error al procesar el funcionario "${func}":`, error);
        }
        }

        // 3. Insertar en hospitalizacion una vez completado el proceso de funcionarios.
        // Se asignan los valores usando los datos del JSON y el resultado de la consulta del paciente.
        // Para la fecha de ingreso se usará el campo "admision" de DatosPaciente, convertido a formato 'YYYY-MM-DD'
        const admisionRaw = datosExcelDesignado.DatosPaciente?.admision ?? null;
        const hosp_fecha_ing = admisionRaw ? formatSimpleDate(admisionRaw, 'DD/MM/YYYY') : null;

        // Para hosp_func_id, se usará el funcionario de admisión (por ejemplo, "funcAdmision" en DatosPaciente)
        const funcAdmision = datosExcelDesignado.DatosPaciente?.funcAdmision ?? null;
        const funcionarioAdmision = await procesarFuncionario(funcAdmision);
        const hosp_func_id = funcionarioAdmision ? funcionarioAdmision.func_id : null;

        // Se construye el objeto con los datos para la inserción
        const hospData = {
        hosp_fecha_ing,                                 // Fecha de admisión convertida
        hosp_pac_id: paciente.pac_id,                   // ID del paciente obtenido previamente
        hosp_func_id,                                  // ID del funcionario de admisión
        hosp_criticidad: datosExcelDesignado.Resumen?.categoria ?? null,        // Se asigna categoría (o criticidad) del resumen
        hosp_diag_cod: datosExcelDesignado.Resumen?.codDiagnostico ?? null,       // Código diagnóstico
        hosp_diagnostico: datosExcelDesignado.Resumen?.diagnostico ?? null,       // Diagnóstico
        //hosp_cod: datosExcelDesignado.Resumen?.ctaCorriente ?? null,              // Código (puede provenir de ctaCorriente)
        hosp_servicio: datosExcelDesignado.Resumen?.servicio ?? null              // Servicio o clasificación de cama
        };

        // Realizamos el insert en hospitalizacion
        const resultadoHospitalizacion = await insertarHospitalizacion(hospData);
        console.log("Hospitalización insertada:", resultadoHospitalizacion);

        // ---------------------
    // Nuevo bloque: Insertar en paciente_traslado
// ---------------------

// Se extrae la fecha para el traslado.
// En este ejemplo usamos el campo "admision" de DatosPaciente; ajusta según tus necesidades.
const trasladoFechaRaw = datosExcelDesignado.DatosPaciente?.admision ?? null;
const ptras_fecha = trasladoFechaRaw
  ? formatSimpleDate(trasladoFechaRaw, "DD/MM/YYYY")
  : null;

// Para este ejemplo se utilizan:
// - "ctaCorriente" del objeto Resumen para la cama de origen
// - "servicio" del objeto Resumen para la cama de destino
// Ajusta estos campos según la estructura real de tu JSON.
const ptras_cama_origen = datosExcelDesignado.Resumen?.ctaCorriente ?? null;
const ptras_cama_destino = datosExcelDesignado.Resumen?.servicio ?? null;

// Se obtiene el id del funcionario a partir de su nombre.
// En este ejemplo se usa "funcEgreso" de DatosPaciente para identificar al funcionario del traslado.
const nombreFuncionarioTraslado = datosExcelDesignado.DatosPaciente?.funcEgreso ?? null;
let ptras_func_id = null;
if (nombreFuncionarioTraslado) {
  ptras_func_id = await obtenerFuncionarioIdPorNombre(nombreFuncionarioTraslado);
}

// **Nuevo:** Obtener el id de la hospitalización, que es requerido por el trigger.
// Supongamos que este valor viene en DatosPaciente con la propiedad "hosp_id".
const hosp_id = datosExcelDesignado.DatosPaciente?.hosp_id ?? null;

// Construir objeto con los datos del traslado
const trasladoData = {
  ptras_fecha,
  ptras_cama_origen,
  ptras_cama_destino,
  ptras_func_id,  
  hosp_id: resultadoHospitalizacion.hosp_id,                  // Se agrega el hosp_id requerido para el trigger
};

// Realizar el insert en paciente_traslado
const resultadoTraslado = await insertarPacienteTraslado(trasladoData);
console.log("Paciente traslado insertado:", resultadoTraslado);


    } catch (error) {
        console.error('Error en el proceso:', error);
    }
    }

    // Ejecutamos la función principal
    Principal();
