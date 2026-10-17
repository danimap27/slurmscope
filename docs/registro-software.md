# Cómo inscribir SlurmScope en el Registro de la Propiedad Intelectual

Guía de trámite para esta obra concreta. Verificado en octubre de 2026 contra las fuentes
oficiales que se citan al final; conviene reconfirmar tasas y formularios antes de enviar,
porque cambian.

## 0. Antes de nada: qué protege el registro y qué no

El programa de ordenador está protegido por derechos de autor **desde el momento de su
creación**, sin necesidad de registro (arts. 95 y siguientes del texto refundido de la Ley
de Propiedad Intelectual). La inscripción registral no crea el derecho: lo **publica** y
sirve como **presunción, salvo prueba en contra, de que el derecho existe y pertenece a su
titular** en la forma que consta en el asiento.

O sea: registrarlo no es obligatorio, pero es la prueba cualificada de autoría y de fecha
que se pide cuando hay que acreditar un mérito (por ejemplo, el ítem de transferencia
científica del baremo de una plaza) o cuando alguien discute la titularidad.

## 1. Decidir la vía

| Vía | Cuándo tiene sentido |
|---|---|
| **Registro Territorial de Andalucía** (procedimiento código **297** de la Junta) | Es la vía natural si resides en Sevilla. Tramita la inscripción inicial. Formulario normalizado: `003149D`. |
| **Registro General de la Propiedad Intelectual** (Ministerio de Cultura, sede electrónica) | Alternativa telemática. Válida igualmente: el registro es único en todo el territorio nacional. |
| **Depósito con sello de tiempo** (Software Heritage, Zenodo con DOI, notario) | No sustituye al registro, pero es barato, inmediato y publica fecha y autoría. Útil además como mérito de acceso abierto. Recomendable hacerlo **además**, no en lugar de. |

En cualquiera de las vías el sujeto es **una persona física** (el autor), no la universidad,
siempre que la obra no se haya creado en el marco de una relación laboral o por encargo.
Confirma este punto antes de firmar si en algún momento has usado tiempo o medios de la UPO:
la OTRI de la UPO gestiona la propiedad intelectual e industrial y puede aclararte la
titularidad.

## 2. Documentación que hay que presentar

Requisitos específicos para **programas de ordenador** según el Ministerio de Cultura:

1. **La totalidad del código fuente** en formato digital cuyo contenido pueda ser
   examinado por el registro.
2. **El ejecutable** del programa. Si no se presenta, hay que **justificar esa
   circunstancia** en la memoria.
3. Una **memoria** que contenga: breve descripción del programa, lenguaje de programación,
   entorno operativo, **diagrama de flujo** y **listado de ficheros**.

Además, con carácter general: solicitud normalizada, identificación del autor, título y
naturaleza de la obra, y justificante del pago de la tasa.

Todo esto ya está preparado en esta obra:

| Requisito | Dónde está |
|---|---|
| Código fuente completo | `registro/codigo-fuente-slurmscope-1.0.0.txt` |
| Justificación de ausencia de ejecutable | `docs/memoria-tecnica.md` §7 (lenguaje interpretado, distribución como paquete Python) |
| Memoria descriptiva | `docs/memoria-tecnica.md` |
| Diagrama de flujo | `docs/memoria-tecnica.md` §4.1 |
| Listado de ficheros | `registro/listado-ficheros.txt` y §11 de la memoria |
| Identificación del autor y licencia | `pyproject.toml`, `LICENSE`, §1 de la memoria |

Genera o regenera ese paquete con:

```bash
bash scripts/preparar-registro.sh
```

Produce `registro/` con el código fuente unificado, el listado de ficheros, un
`MANIFEST.sha256` con la huella de todo (integridad del depósito) y un `.tar.gz` con el
conjunto.

## 3. Sobre el ejecutable

SlurmScope se distribuye como paquete de Python y se ejecuta con el intérprete del sistema
(`python3 -m slurmscope.cli`). No hay binario compilado que depositar, y la propia
normativa contempla el caso: se justifica en la memoria y se aporta el resultado de la
batería de pruebas como evidencia de funcionamiento (`make test`, 80 casos, resultado
`OK`). Si el registro pidiera un artefacto ejecutable, se puede adjuntar un `zipapp`
autocontenido:

```bash
python3 -m zipapp slurmscope -o slurmscope.pyz -p "/usr/bin/env python3"
```

## 4. Pasos concretos (vía andaluza)

1. **Reunir el paquete**: `bash scripts/preparar-registro.sh`.
2. **Descargar el formulario** `003149D` (Solicitud de inscripción inicial, Registro
   Territorial de la Propiedad Intelectual de Andalucía) desde el catálogo de
   procedimientos de la Junta, código 297.
3. **Cumplimentarlo** con los datos del autor y de la obra: título «SlurmScope», versión,
   naturaleza «programa de ordenador», y la fecha de creación.
4. **Pagar la tasa** y guardar el justificante. La tasa estatal de propiedad intelectual se
   regula en el artículo 20 de la Ley 66/1997, con la redacción de la disposición adicional
   quinta de la Ley 22/2021; muchas comunidades aplican bonificaciones o exenciones
   (por ejemplo, para jóvenes), así que convérsalo con la propia unidad tramitadora.
5. **Presentar** por sede electrónica (con certificado o Cl@ve) o presencialmente en
   cualquier oficina de asistencia en materia de registros. Se puede presentar el código en
   soporte digital.
6. **Esperar la resolución**: el plazo de resolución es de **6 meses**. Si no resuelve en
   plazo, se entiende estimada por silencio administrativo positivo.
7. **Guardar** el certificado de inscripción con su número de asiento: es lo que se aporta
   como mérito.

## 5. Después del registro

- **Fecha y autoría**: usa el número de asiento en el CV y en la solicitud de acreditación
  o en el baremo de la plaza.
- **Divulgación**: publicar el código en abierto (GitHub + un DOI de Zenodo) suma en los
  apartados de transferencia y de recursos educativos en abierto, y no impide la
  inscripción.
- **Versiones**: el registro cubre la versión depositada. Si más adelante publicas cambios
  sustanciales, valora una inscripción sucesiva.
- **Cambio de titularidad**: si en algún momento lo cedes o licencias, ese acto también es
  inscribible.

## 6. Fuentes

- Ministerio de Cultura, «Programas de ordenador» (requisitos específicos del depósito).
- Ministerio de Cultura, sede electrónica del Registro de la Propiedad Intelectual.
- Real Decreto 611/2023, de 11 de julio, por el que se aprueba el Reglamento del Registro
  de la Propiedad Intelectual.
- Junta de Andalucía, procedimiento 297 «Inscripción registral de derechos de Propiedad
  Intelectual» (Consejería de Cultura y Deporte).
- UPO, OTRI, «Propiedad Intelectual e Industrial».
