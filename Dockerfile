FROM quay.io/astronomer/astro-runtime:12.4.0

RUN python -m venv BlueMar_venv && \
    . BlueMar_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate