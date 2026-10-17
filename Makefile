.PHONY: help test demo audit lint stats registro clean

help:
	@echo "make test      — ejecuta la batería de pruebas"
	@echo "make demo      — ciclo completo sobre el clúster simulado"
	@echo "make audit     — compilación, imports sin usar y métricas"
	@echo "make lint      — solo la auditoría (falla si hay hallazgos)"
	@echo "make stats     — solo las métricas de la obra"
	@echo "make registro  — genera el paquete de depósito para el Registro"

test:
	PYTHONPATH=. python3 -m unittest discover -s tests -t tests -v

demo:
	bash examples/demo.sh

audit:
	python3 scripts/audit.py

lint:
	python3 scripts/audit.py --check

stats:
	python3 scripts/audit.py --stats

registro:
	bash scripts/preparar-registro.sh

clean:
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	find . -name "*.pyc" -delete
