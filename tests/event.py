extract_event =    {
    "step": "extract",
    "tables": [
        "aeronave",
        "ocorrencia_tipo",
        "ocorrencia",
        "recomendacao"
    ]
}

refine_event = {
    "step": "refined",
    "date_ref": None,
    "tables": [
        "aeronave",
        "ocorrencia_tipo",
        "ocorrencia",
        "recomendacao"
    ]
}

load_event =  {
    "step": "load",
    "tables": [
        "dim_aeronave",
        "dim_ocorrencia_tipo",
        "fat_ocorrencia",
        "dim_recomendacao"
    ]
}