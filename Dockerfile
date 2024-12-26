FROM quay.io/astronomer/astro-runtime:12.6.0

# install dbt into a virtual environment
RUN python -m venv dbt_venv && . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-core dbt-postgres && deactivate