import pandas as pd

from sqlalchemy import create_engine
import yaml
from pathlib import Path

from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2

def configure_pandas_display():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 200)
    pd.set_option('display.precision', 2)

# Avoid error from stock psycopg2 trying to set 'standard_conforming_strings' setting
class _RedshiftPostgresDialect(PGDialect_psycopg2):
    def _set_backslash_escapes(self, connection):
        self._backslash_escapes = 'off'

def get_redshift_engine(profile_name, target_name):
    registry.register("redshift_custom", __name__, "_RedshiftPostgresDialect")

    # Load from dbt profiles.yml
    with open(Path.home() / '.dbt' / 'profiles.yml') as f:
        profiles = yaml.safe_load(f)

    p = profiles[profile_name]['outputs'][target_name]
    db_url = f"redshift_custom://{p['user']}:{p['password']}@{p['host']}:5439/{p['dbname']}"
    engine = create_engine(db_url, connect_args={'sslmode': 'require'})
    return engine
