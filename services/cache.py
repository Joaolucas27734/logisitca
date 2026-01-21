
# services/cache.py

try:
    import streamlit as st
    cache = st.cache_data
except Exception:
    def cache(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
