# Evidencia de ejecución

Fecha: 20 de octubre de 2026. Python estándar, ejecución local sin conexión
sobre los tres JSON oficiales incluidos en data/raw.

## Comando principal

    python pipeline.py

Se ejecutó dos veces. Resultados observados:

| Medida | Ejecución 1 | Ejecución 2 |
| --- | ---: | ---: |
| Estado | completed | completed |
| Filas de entrada | 36 | 36 |
| Aceptadas | 36 | 36 |
| Rechazadas | 0 | 0 |
| Insertadas | 36 | 0 |
| Actualizadas | 0 | 0 |
| Sin cambios | 0 | 36 |
| Observaciones en SQLite | 36 | 36 |
| Claves esperadas faltantes | 0 | 0 |
| Filas del reporte CSV | 12 | 12 |

La tabla pipeline_runs conserva los resultados de ambas ejecuciones.
run_sources conserva las tres fuentes por cada ejecución, con sus SHA-256.
La tercera ejecución mantuvo 36 observaciones, sin inserciones, cambios ni rechazos. quality_report.json refleja esta última ejecución. Las 7 pruebas de este proyecto pasaron; VALIDACION.json conserva el resumen.

## Pruebas automatizadas

    python -m unittest discover -s tests -v

Resultado final: **Ran 7 tests — OK**.

1. Una respuesta paginada incompleta provoca fallo registrado y no carga datos.
2. Siete registros inválidos/duplicados se separan de las 36 observaciones válidas.
3. La falta de una observación genera cobertura incompleta y un campo CSV vacío.
4. Poblaciones inválidas se rechazan y se admite inflación negativa.
5. Dos cargas de los snapshots reales mantienen 36 observaciones sin duplicarlas.
6. CSV, datos SQL y métricas de calidad concilian.
7. Modificar un valor actualiza únicamente esa observación y conserva su primera carga.

Los datos defectuosos utilizados en las pruebas son copias sintéticas y
temporales. No se mezclan con la base ni los snapshots de entrega.
