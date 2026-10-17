#!/usr/bin/env bash
# preparar-registro.sh — genera el paquete de depósito para inscribir la obra en el
# Registro de la Propiedad Intelectual.
#
# Produce, en registro/:
#   codigo-fuente-<obra>-<version>.txt   la totalidad del código fuente en un solo fichero
#   listado-ficheros.txt                 listado de ficheros con líneas y huella SHA-256
#   MANIFEST.sha256                      huellas de todo el paquete (integridad del depósito)
#   <obra>-<version>.tar.gz              el paquete completo comprimido
# además de la memoria técnica, el README, la licencia y los metadatos del paquete.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

OBRA="slurmscope"
VERSION="$(python3 -c "import re,pathlib;print(re.search(r'version\s*=\s*\"([^\"]+)\"', pathlib.Path('pyproject.toml').read_text()).group(1))")"
OUT="registro"
BUNDLE="$OUT/codigo-fuente-$OBRA-$VERSION.txt"

rm -rf "$OUT"
mkdir -p "$OUT"

echo "==> Recopilando código fuente (v$VERSION)"
: > "$BUNDLE"
{
  echo "================================================================================"
  echo "OBRA:    $OBRA v$VERSION"
  echo "AUTOR:   Daniel Martín Pérez"
  echo "DEPÓSITO: totalidad del código fuente para inscripción registral"
  echo "FECHA DE GENERACIÓN: $(date --iso-8601=seconds)"
  echo "================================================================================"
  echo
} >> "$BUNDLE"

mapfile -t SOURCES < <(find slurmscope tests examples -type f -name "*.py" | sort)
for f in "${SOURCES[@]}"; do
  {
    echo "--------------------------------------------------------------------------------"
    echo "FICHERO: $f"
    echo "--------------------------------------------------------------------------------"
    cat "$f"
    echo
  } >> "$BUNDLE"
done
echo "    $BUNDLE  ($(wc -l < "$BUNDLE") líneas)"

echo "==> Listado de ficheros (requisito del registro)"
{
  printf '%-46s %8s  %s\n' "FICHERO" "LÍNEAS" "SHA-256"
  printf '%-46s %8s  %s\n' "----------------------------------------------" "--------" "$(printf '%.64s' '----------------------------------------------------------------')"
  for f in "${SOURCES[@]}" pyproject.toml README.md LICENSE Makefile; do
    [ -f "$f" ] || continue
    printf '%-46s %8s  %s\n' "$f" "$(wc -l < "$f")" "$(sha256sum "$f" | cut -d' ' -f1)"
  done
  printf '%-46s %8s  %s\n' "docs/memoria-tecnica.md" "$(wc -l < docs/memoria-tecnica.md)" "$(sha256sum docs/memoria-tecnica.md | cut -d' ' -f1)"
  printf '%-46s %8s  %s\n' "docs/registro-software.md" "$(wc -l < docs/registro-software.md 2>/dev/null || echo 0)" "$(sha256sum docs/registro-software.md 2>/dev/null | cut -d' ' -f1 || echo '-')"
  echo
  echo "Total de ficheros de código: ${#SOURCES[@]}"
  echo "Líneas de código:  $(cat "${SOURCES[@]}" | wc -l)"
} > "$OUT/listado-ficheros.txt"
echo "    $OUT/listado-ficheros.txt"

echo "==> Copiando documentación del depósito"
cp README.md LICENSE pyproject.toml Makefile "$OUT/"
cp docs/memoria-tecnica.md "$OUT/"
[ -f docs/registro-software.md ] && cp docs/registro-software.md "$OUT/"

echo "==> Calculando huellas del paquete"
( cd "$OUT" && find . -type f ! -name "MANIFEST.sha256" ! -name "*.tar.gz" -print0 \
    | sort -z | xargs -0 sha256sum > MANIFEST.sha256 )
echo "    $OUT/MANIFEST.sha256  ($(wc -l < "$OUT/MANIFEST.sha256") ficheros)"

echo "==> Empaquetando"
tar -czf "$OUT/$OBRA-$VERSION.tar.gz" -C "$OUT" \
  $(cd "$OUT" && ls | grep -v '\.tar\.gz$')
echo "    $OUT/$OBRA-$VERSION.tar.gz  ($(du -h "$OUT/$OBRA-$VERSION.tar.gz" | cut -f1))"

echo
echo "Paquete de depósito listo en $OUT/"
ls -la "$OUT"
