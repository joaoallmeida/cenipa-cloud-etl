extract_event = {
    "pipeline": [
        {
            "step": "extract",
            "tables": [
                "aeronave",
                "ocorrencia_tipo",
                "ocorrencia",
                "recomendacao",
                "fator_contribuinte"
            ]
        }
    ]
}

refine_event = {
    "pipeline": [
        {
            "step": "refined",
            "date_ref": None,
            "tables": [
                "aeronave",
                "ocorrencia_tipo",
                "ocorrencia",
                "recomendacao",
                "fator_contribuinte"
            ]
        }
    ]
}

load_event =  {
    "pipeline": [
        {
            "step": "load",
            "date_ref": None,
            "create_ddl": True,
            "tables": [
                "dim_aeronave",
                "dim_ocorrencia_tipo",
                "dim_recomendacao",
                "dim_fator_contribuinte",
                "fat_ocorrencia",
            ]
        }
    ]
}

event = {
    "pipeline": [
        {
            "step": "extract",
            "tables": [
                "aeronave",
                "ocorrencia_tipo",
                "ocorrencia",
                "recomendacao",
                "fator_contribuinte"
            ]
        },
        {
            "step": "refined",
            "date_ref": None,
            "tables": [
                "aeronave",
                "ocorrencia_tipo",
                "ocorrencia",
                "recomendacao",
                "fator_contribuinte"
            ]
        },
        {
            "step": "load",
            "date_ref": None,
            "create_ddl": True,
            "tables": [
                "dim_aeronave",
                "dim_ocorrencia_tipo",
                "dim_recomendacao",
                "dim_fator_contribuinte",
                "fat_ocorrencia",
            ]
        }
    ]
}