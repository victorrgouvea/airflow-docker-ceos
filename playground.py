from dags.airflow_utils.utils.normalizer import normalize_column_names
import json

denormalized_schema = [
    "CHAVE DE ACESSO",
    "MODELO",
    "SÉRIE",
    "NÚMERO",
    "NATUREZA DA OPERAÇÃO",
    "DATA EMISSÃO",
    "EVENTO MAIS RECENTE",
    "DATA/HORA EVENTO MAIS RECENTE",
    "CPF/CNPJ Emitente",
    "RAZÃO SOCIAL EMITENTE",
    "INSCRIÇÃO ESTADUAL EMITENTE",
    "UF EMITENTE",
    "MUNICÍPIO EMITENTE",
    "CNPJ DESTINATÁRIO",
    "NOME DESTINATÁRIO",
    "UF DESTINATÁRIO",
    "INDICADOR IE DESTINATÁRIO",
    "DESTINO DA OPERAÇÃO",
    "CONSUMIDOR FINAL",
    "PRESENÇA DO COMPRADOR",
    "VALOR NOTA FISCAL"
]

normalized = normalize_column_names(denormalized_schema)
print(json.dumps(normalized,ensure_ascii=False,indent=4))
