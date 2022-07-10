schema = {
    "type": "object",
    "required": ["NU_INSCRICAO", "NU_ANO"],
    "properties": {
        "NU_INSCRICAO": {"type": "integer"},
        "NU_ANO": {"type": "integer"},
        "TP_FAIXA_ETARIA": {"type": "integer"},
        "TP_SEXO": {"type": "string"},
        "TP_ESTADO_CIVIL": {"type": "integer"},
        "TP_COR_RACA": {"type": "integer"},
        "TP_NACIONALIDADE": {"type": "integer"},
        "TP_ST_CONCLUSAO": {"type": "integer"},
        "TP_ANO_CONCLUIU": {"type": "integer"},
        "TP_ESCOLA": {"type": "integer"},
        "TP_ENSINO": {"type": "integer"},
        "IN_TREINEIRO": {"type": "integer"},
        "NO_MUNICIPIO_PROVA": {"type": "string"},
        "SG_UF_PROVA": {"type": "string"},
        "NU_NOTA_CN": {"type": "number"},
        "NU_NOTA_CH": {"type": "number"},
        "NU_NOTA_LC": {"type": "number"},
        "NU_NOTA_MT": {"type": "number"},
        "TP_LINGUA": {"type": "integer"},
        "TP_STATUS_REDACAO": {"type": "number"},
        "NU_NOTA_COMP1": {"type": "number"},
        "NU_NOTA_COMP2": {"type": "number"},
        "NU_NOTA_COMP3": {"type": "number"},
        "NU_NOTA_COMP4": {"type": "number"},
        "NU_NOTA_COMP5": {"type": "number"},
        "NU_NOTA_REDACAO": {"type": "number"}
    }
  }

final_columns = ["NUM_INSCRICAO", "ANO", "FAIXA_ETARIA", "SEXO", "ESTADO_CIVIL", "COR_RACA", "NACIONALIDADE", "EM_STATUS", 
                 "EM_ANO_CONCLUSAO", "TIPO_ESCOLA", "TIPO_ENSINO", "TREINEIRO", "MUNICIPIO_PROVA", "UF_PROVA", "NOTA_CN",
                 "NOTA_CH", "NOTA_LC", "NOTA_MT", "LINGUA_EST", "STATUS_REDACAO", "NOTA_COMP1", "NOTA_COMP2", "NOTA_COMP3", "NOTA_COMP4",
                 "NOTA_COMP5", "NOTA_REDACAO"]

columns = ["NU_INSCRICAO","NU_ANO","TP_FAIXA_ETARIA","TP_SEXO","TP_ESTADO_CIVIL","TP_COR_RACA","TP_NACIONALIDADE","TP_ST_CONCLUSAO",
           "TP_ANO_CONCLUIU","TP_ESCOLA","TP_ENSINO","IN_TREINEIRO", "NO_MUNICIPIO_PROVA", "SG_UF_PROVA","NU_NOTA_CN", "NU_NOTA_CH", 
           "NU_NOTA_LC", "NU_NOTA_MT", "TP_LINGUA", "TP_STATUS_REDACAO", "NU_NOTA_COMP1", "NU_NOTA_COMP2", "NU_NOTA_COMP3",
            "NU_NOTA_COMP4", "NU_NOTA_COMP5", "NU_NOTA_REDACAO"]