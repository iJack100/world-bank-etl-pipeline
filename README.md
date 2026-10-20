# ETL de indicadores del Banco Mundial

Proyecto de portafolio construido el **20 de octubre de 2026**. Extrae respuestas
JSON oficiales, normaliza y valida observaciones y las carga en SQLite mediante
SQL con clave única y actualización por conflicto. Genera un reporte CSV por país
y año y conserva trazabilidad y métricas de calidad por ejecución.

## Alcance y datos

- Países: Ecuador (EC) y Colombia (CO).
- Periodo: 2019–2024, ambos inclusive.
- Indicadores: PIB en USD corrientes (NY.GDP.MKTP.CD), población total
  (SP.POP.TOTL) e inflación anual al consumidor (FP.CPI.TOTL.ZG).
- Cobertura esperada: 2 países × 6 años × 3 indicadores = **36 observaciones**.
- Fuente: API pública del Banco Mundial, World Development Indicators.
- Snapshots capturados el **2026-10-20**, entre **21:34:08 y 21:34:09 UTC**.
  Las respuestas incluidas declaran última actualización de fuente
  2026-10-08. Cada URL y fecha exacta de descarga figura en
  data/raw/sources.json. Los JSON originales se conservan sin transformar.

URLs oficiales utilizadas:

- [PIB](https://api.worldbank.org/v2/country/EC;CO/indicator/NY.GDP.MKTP.CD?date=2019:2024&format=json&per_page=1000)
- [Población](https://api.worldbank.org/v2/country/EC;CO/indicator/SP.POP.TOTL?date=2019:2024&format=json&per_page=1000)
- [Inflación](https://api.worldbank.org/v2/country/EC;CO/indicator/FP.CPI.TOTL.ZG?date=2019:2024&format=json&per_page=1000)

## Ejecutar en Windows

Requiere Python 3.10 o posterior. Utiliza únicamente la biblioteca estándar:
no hay paquetes que instalar ni servicios externos que configurar.

Abre PowerShell en esta carpeta y ejecuta:

    python pipeline.py
    python -m unittest discover -s tests -v

El comando por defecto trabaja **sin conexión** con los snapshots incluidos.
Se puede usar py -3 en lugar de python si ese es el lanzador instalado.
La ruta de salida es relativa al propio proyecto; no depende de la carpeta desde
la que se llama al script.

Opciones:

    python pipeline.py --output-dir .\outputs_nuevos
    python pipeline.py --raw-dir .\data\raw --output-dir .\outputs
    python pipeline.py --refresh

La opción --refresh requiere internet y vuelve a descargar los tres indicadores.
Valida la estructura y la integridad de la paginación antes de reemplazar los
archivos raw. La API puede revisar datos históricos; el resultado actualizado
podría diferir del snapshot incluido. Para conservar evidencia, copia antes data/raw.

## Flujo y controles

1. **Extracción:** lectura de los JSON raw y su manifiesto; comprobación de
   indicador, URL, fecha y respuesta completa en una sola página.
2. **Transformación:** esquema largo con país, año, indicador y valor numérico.
   Se verifican claves, tipos, países e ISO3, rango temporal, valores finitos,
   población positiva y entera y PIB positivo. La inflación negativa es válida.
3. **Calidad:** los valores nulos, registros inválidos y claves repetidas se
   rechazan y conservan con su motivo. Ante duplicados de la misma clave se
   acepta la primera observación y se rechazan las posteriores. No se imputan
   valores faltantes. Se informa cobertura faltante frente a las 36 claves esperadas.
4. **Carga:** SQLite con claves foráneas y clave primaria compuesta
   (country_code, year, indicator_code). El upsert inserta claves nuevas,
   actualiza valores modificados y deja intactos los valores iguales.
5. **Salida:** CSV por país/año, con los tres indicadores, cobertura y
   PIB por habitante calculado como PIB/población, redondeado a dos decimales.

La idempotencia se refiere a la tabla de observaciones: repetir la misma entrada
no duplica ni modifica sus valores. Cada ejecución sí crea una nueva entrada de
auditoría. El proceso no elimina observaciones que desaparezcan de un snapshot:
la cobertura de la entrada actual se mide por separado del total del almacén.

## Archivos de salida

| Archivo | Contenido |
| --- | --- |
| outputs/indicadores.sqlite | Datos, ejecuciones, fuentes y controles |
| outputs/indicadores_por_pais_anio.csv | Reporte de 12 combinaciones país/año con cobertura completa |
| outputs/quality_report.json | Resumen de la última ejecución, conteos, estado, fuentes y SHA-256 |
| outputs/rechazos_run_N.csv | Registros rechazados por ejecución; solo cabecera si no hay rechazos |

Tablas SQLite:

- countries, indicators: dimensiones.
- observations: observaciones normalizadas, primera carga y última modificación.
- pipeline_runs: inicio, fin, modo, estado y conteos por ejecución; los fallos
  de extracción quedan registrados.
- run_sources: URL, fecha de descarga, SHA-256, actualización de fuente y filas.
- quality_metrics, quality_rejections: controles y cuarentena.

Se comprueban las identidades:
entrada = aceptadas + rechazadas; y
aceptadas = insertadas + actualizadas + sin cambios.
El estado completed_with_warnings indica rechazos o cobertura incompleta;
failed indica un error que impide completar la ejecución.

## Pruebas

La suite unittest contiene siete pruebas: idempotencia real sin red, cuarentena
de siete clases de registros inválidos/duplicados, actualización de una sola
observación, conciliación entre CSV/SQLite/métricas, fallo por paginación
incompleta, cobertura faltante sin imputación y reglas de población/inflación.
Usa copias temporales dentro del proyecto y no altera los snapshots originales.

Verificación realizada el 20 de octubre de 2026: **7 de 7 pruebas superadas**.
Se ejecutó además el comando principal dos veces sobre la salida incluida:
la primera ejecución insertó 36 observaciones; la segunda insertó 0, actualizó 0
y dejó 36 sin cambios. Ambas terminaron sin rechazos ni claves esperadas
faltantes. La base incluida conserva el historial de ejecuciones y 36 observaciones;
el reporte contiene 12 filas de datos, además de su cabecera.

## Límites

Es una demostración local de pequeño volumen, no un servicio en producción.
No incluye nube, orquestador, carga distribuida ni garantía de disponibilidad.
No valida cifras fiscales ni certifica la exactitud económica de la fuente.
Los valores se almacenan como números de coma flotante; los cálculos derivados
son descriptivos y no contables. El PIB por habitante calculado no se presenta
como una descarga del indicador oficial de PIB per cápita. El reporte refleja
las últimas observaciones cargadas y no constituye asesoría económica.
