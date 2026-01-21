# services/config.py

import os
import json

def get_secret(path: list[str], default=None):
    """
    Busca segredo em:
    1️⃣ Variável de ambiente
       - aceita JSON (string) ou valor simples
    2️⃣ Streamlit secrets (se disponível)
    """

    env_key = "_".join(path).upper()

    # 1️⃣ ENV
    raw = os.getenv(env_key)
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            return raw

    # 2️⃣ Streamlit
    try:
        import streamlit as st
        ref = st.secrets
        for key in path:
            ref = ref[key]
        return ref
    except Exception:
        return default
