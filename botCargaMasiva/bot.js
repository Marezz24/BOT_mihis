// bot.js

// ===================
// 1. Importar módulos
// ===================
const fs = require("fs");
const path = require("path");
const xlsx = require("xlsx");
const dayjs = require("dayjs");

// =============================
// 2. Función que procesa todos los archivos Excel y retorna el JSON resultante
// =============================
function procesarExcels() {
  // Arreglo para almacenar la información de cada Excel
  const excelsData = [];

  // ============================================
  // 3. Funciones auxiliares y de procesamiento
  // ============================================

  // Función para recorrer de forma recursiva un directorio (incluye subcarpetas)
  function walkDir(dir, callback) {
    fs.readdirSync(dir).forEach((file) => {
      const filepath = path.join(dir, file);
      const stat = fs.statSync(filepath);
      if (stat.isDirectory()) {
        walkDir(filepath, callback);
      } else {
        callback(filepath);
      }
    });
  }

  // Función para formatear fechas (por ejemplo, "2025-02-06")
  function formatDate(dateValue) {
    return dayjs(dateValue).format("YYYY-MM-DD");
  }

  // Funciones para extraer texto antes o después de un separador
  function extractBefore(text, separator) {
    const index = text.indexOf(separator);
    return index !== -1 ? text.substring(0, index).trim() : text.trim();
  }

  function extractAfter(text, separator) {
    const index = text.indexOf(separator);
    return index !== -1 ? text.substring(index + 1).trim() : "";
  }

  function separarNombreCompleto(nombreCompleto) {
    // Separamos la cadena por espacios
    const partes = nombreCompleto.trim().split(" ");

    // Verificamos que el arreglo tenga al menos 4 elementos
    if (partes.length < 4) {
      throw new Error(
        'El formato del nombre es incorrecto. Se espera: "ApellidoPaterno ApellidoMaterno Nombre1 Nombre2"'
      );
    }

    // Asignamos la primera parte al apellido paterno
    const apellidoPaterno = partes[0];
    // La segunda parte al apellido materno
    const apellidoMaterno = partes[1];
    // El resto se unen para formar los nombres
    const nombres = partes.slice(2).join(" ");

    return {
      apellidoPaterno,
      apellidoMaterno,
      nombres,
    };
  }

  // Función para procesar un archivo Excel y devolver un objeto con la información por hoja
  function processExcel(filePath) {
    const excelObj = { file: filePath, sheets: {} };
    try {
      const workbook = xlsx.readFile(filePath);

      // -------------------------------
      // HOJA "Resumen"
      // -------------------------------
      if (workbook.SheetNames.includes("Resumen")) {
        const sheet = workbook.Sheets["Resumen"];
        const ctaCorriente = sheet["B2"] ? sheet["B2"].v : null;
        const fecha = sheet["B3"] ? sheet["B3"].v : null;
        const fechaform = extractBefore(fecha, "-");
        const fechaIngreso = fechaform ? formatDate(fechaform) : null;
        const rut = sheet["B4"] ? sheet["B4"].v : null;
        const fechaN = sheet["B5"] ? sheet["B5"].v : null;
        const fechaNac = fechaN ? formatDate(fechaN) : null;
        const comuna = sheet["B6"] ? sheet["B6"].v : null;
        const ficha = sheet["B7"] ? sheet["B7"].v : null;
        const nomApes = sheet["B8"] ? sheet["B8"].v : null;
        const sepNomApe = separarNombreCompleto(nomApes);
        const nombresPac = sepNomApe.nombres;
        const apePatPac = sepNomApe.apellidoPaterno;
        const apeMatPac = sepNomApe.apellidoMaterno;
        const procedencia = sheet["B9"] ? sheet["B9"].v : null;
        const servicio = sheet["B10"] ? sheet["B10"].v : null;
        const categoria = sheet["B11"] ? sheet["B11"].v : null;
        const diagRaw = sheet["B12"] ? sheet["B12"].v : null;
        let codDiagnostico = null;
        let diagnostico = null;
        if (diagRaw) {
          codDiagnostico = extractBefore(diagRaw, " ");
          diagnostico = extractAfter(diagRaw, " ");
        }

        excelObj.sheets["Resumen"] = {
          ctaCorriente,
          fechaIngreso,
          rut,
          fechaNac,
          comuna,
          ficha,
          nombresPac,
          apePatPac,
          apeMatPac,
          procedencia,
          servicio,
          categoria,
          codDiagnostico,
          diagnostico,
        };
      } else {
        excelObj.sheets["Resumen"] = {
          error: 'La hoja "Resumen" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "EvolucionCategoria"
      // -------------------------------
      if (workbook.SheetNames.includes("EvolucionCategoria")) {
        const sheet = workbook.Sheets["EvolucionCategoria"];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1, range: 3 });
        excelObj.sheets["EvolucionCategoria"] = data
          .map((row) => ({
            fechaEvo: row[0] ? formatDate(row[0]) : null, // Columna A
            categoriaRDEvo: row[1] || null, // Columna B
            nomFuncionarioEvo: row[2] || null, // Columna C
          }))
          .filter(
            (row) => row.fechaEvo || row.categoriaRDEvo || row.nomFuncionarioEvo
          ); // Filtramos filas vacías
      } else {
        excelObj.sheets["EvolucionCategoria"] = {
          error: 'La hoja "EvolucionCategoria" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "ReqVentilatorio"
      // -------------------------------
      if (workbook.SheetNames.includes("ReqVentilatorio")) {
        const sheet = workbook.Sheets["ReqVentilatorio"];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1, range: 2 });
        excelObj.sheets["ReqVentilatorio"] = data.map((row) => {
          let funcionarioVent = null;
          let cargoFuncVent = null;
          if (row[3]) {
            funcionarioVent = extractBefore(row[3], "/");
            cargoFuncVent = extractAfter(row[3], "/");
          }
          return {
            fechaVent: row[0] ? formatDate(row[0]) : null,
            tipoReqVent: row[1] || null,
            estadoCovidVent: row[2] || null,
            funcionarioVent,
            cargoFuncVent,
          };
        });
      } else {
        excelObj.sheets["ReqVentilatorio"] = {
          error: 'La hoja "ReqVentilatorio" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "EvolucionEstado"
      // -------------------------------
      if (workbook.SheetNames.includes("EvolucionEstado")) {
        const sheet = workbook.Sheets["EvolucionEstado"];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1, range: 2 });
        excelObj.sheets["EvolucionEstado"] = data.map((row) => ({
          fechaEvoEst: row[0] ? formatDate(extractBefore(row[0], " ")) : null,
          estadoPacEvoEst: row[1] || null,
          condicionPacEvoEst: row[2] || null,
          FuncEvoEst: row[3] || null,
        }));
      } else {
        excelObj.sheets["EvolucionEstado"] = {
          error: 'La hoja "EvolucionEstado" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "AltasCanceladas"
      // -------------------------------
      if (workbook.SheetNames.includes("AltasCanceladas")) {
        const sheet = workbook.Sheets["AltasCanceladas"];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1, range: 3 });
        excelObj.sheets["AltasCanceladas"] = data.map((row) => ({
          fechaAltCan: row[0] || null,
          tipoAltCan: row[1] || null,
          funcAltCan: row[2] || null,
        }));
      } else {
        excelObj.sheets["AltasCanceladas"] = {
          error: 'La hoja "AltasCanceladas" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "Traslados"
      // -------------------------------
      if (workbook.SheetNames.includes("Traslados")) {
        const sheet = workbook.Sheets["Traslados"];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1, range: 3 });
        excelObj.sheets["Traslados"] = data.map((row) => ({
          fechaAsigTras: row[0] ? extractBefore(row[0], " ") : null,
          origenTras: extractBefore(row[1], "-") || null,
          camaTras: row[2] || null,
          clasificacion_camaTras: row[3] ? extractBefore(row[3], "-") : null,
          numCamaTras: row[4] || null,
          diasHospTras: row[5] || null,
          funcTras: row[6] || null,
        }));
      } else {
        excelObj.sheets["Traslados"] = {
          error: 'La hoja "Traslados" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "HistObserv"
      // -------------------------------
      if (workbook.SheetNames.includes("HistObserv")) {
        const sheet = workbook.Sheets["HistObserv"];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1, range: 3 });
        excelObj.sheets["HistObserv"] = data.map((row) => ({
          historialHistObserv: row[0] ? extractBefore(row[0], " ") : null,
          evolucionHistObserv: row[1] || null,
        }));
      } else {
        excelObj.sheets["HistObserv"] = {
          error: 'La hoja "HistObserv" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "HistNecesidades"
      // -------------------------------
      if (workbook.SheetNames.includes("HistNecesidades")) {
        const sheet = workbook.Sheets["HistNecesidades"];
        const data = xlsx.utils.sheet_to_json(sheet, { header: 1, range: 3 });
        excelObj.sheets["HistNecesidades"] = data.map((row) => ({
          fechaHistNec: row[0] ? formatDate(extractBefore(row[0], " ")) : null,
          indicacionesHistNec: row[1] || null,
          accionHistNec: row[2] || null,
        }));
      } else {
        excelObj.sheets["HistNecesidades"] = {
          error: 'La hoja "HistNecesidades" no se encontró.',
        };
      }

      // -------------------------------
      // HOJA "DatosPaciente"
      // -------------------------------
      if (workbook.SheetNames.includes("DatosPaciente")) {
        const sheet = workbook.Sheets["DatosPaciente"];
        const rut = sheet["B1"] ? sheet["B1"].v : null;
        const nombrePac = sheet["B2"] ? sheet["B2"].v : null;
        const adm = sheet["B3"] ? sheet["B3"].v : null;
        const admf = extractBefore(adm, " ");
        const admision = admf;
        const hos = sheet["B4"] ? sheet["B4"].v : null;
        const hosp = extractBefore(hos, " ");
        const hospitalizacion = hosp;
        const egreso = sheet["B5"] ? sheet["B5"].v : null;
        const alergias = sheet["D1"] ? sheet["D1"].v : null;
        const rutMedico = extractAfter(alergias, ":");
        const medico = sheet["D2"] ? sheet["D2"].v : null;
        const funcAdmision = sheet["D3"] ? sheet["D3"].v : null;
        const funcEgreso = sheet["D5"] ? sheet["D5"].v : null;

        excelObj.sheets["DatosPaciente"] = {
          rut,
          nombrePac,
          admision,
          hospitalizacion,
          egreso,
          alergias,
          rutMedico,
          medico,
          funcAdmision,
          funcEgreso,
        };
      } else {
        excelObj.sheets["DatosPaciente"] = {
          error: 'La hoja "DatosPaciente" no se encontró.',
        };
      }
    } catch (error) {
      excelObj.error = `Error procesando el archivo: ${error.message}`;
    }
    // Retornamos el objeto sin convertirlo a cadena
    return excelObj;
  }

  // ============================================
  // 4. Recorrer la carpeta "data" y procesar cada archivo Excel
  // ============================================
  const dataDir = path.join(__dirname, "data");
  walkDir(dataDir, (filePath) => {
    // Procesa solo archivos .xlsx (evitando archivos temporales que comiencen con "~$")
    if (
      filePath.endsWith(".xlsx") &&
      !path.basename(filePath).startsWith("~$")
    ) {
      const excelData = processExcel(filePath);
      excelsData.push(excelData);
    }
  });

  // ============================================
  // 5. Retornar el JSON resultante (se convierte el arreglo completo a JSON)
  // ============================================
  return JSON.stringify(excelsData, null, 2);
}

// Exportar la función para que pueda ser llamada desde principal.js
module.exports = procesarExcels;
