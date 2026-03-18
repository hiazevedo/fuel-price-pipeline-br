from databricks.connect import DatabricksSession

def get_spark():
    # Usa o perfil padrão de ~/.databrickscfg (configurado via Databricks CLI)
    # Não armazene host/token hardcoded aqui
    return DatabricksSession.builder \
        .serverless(True) \
        .getOrCreate()
