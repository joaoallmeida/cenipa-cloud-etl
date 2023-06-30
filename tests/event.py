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
            "tables": [
                "aeronave",
                "ocorrencia_tipo",
                "ocorrencia",
                "recomendacao"
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
        }
    ]
}