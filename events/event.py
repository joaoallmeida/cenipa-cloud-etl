extract_event = {
    "pipeline": [
        {
            "step": "extract",
            "tables": [
                "aeronave",
                "ocorrencia_tipo",
                "ocorrencia",
                "recomendacao"
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
                "recomendacao"
            ]
        }
    ]
}

load_event =  {
    "pipeline": [
        {
            "step": "load",
            "date_ref": None,
            "tables": [
                "dim_aeronave",
                "dim_ocorrencia_tipo",
                "dim_recomendacao",
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
                "recomendacao"
            ]
        },
        {
            "step": "refined",
            "date_ref": None,
            "tables": [
                "aeronave",
                "ocorrencia_tipo",
                "ocorrencia",
                "recomendacao"
            ]
        },
        {
            "step": "load",
            "date_ref": None,
            "tables": [
                "dim_aeronave",
                "dim_ocorrencia_tipo",
                "dim_recomendacao",
                "fat_ocorrencia"
            ]
        }
    ]
}