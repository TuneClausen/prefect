# prefect

# deploy from git example
Opretter temp fil, lader prefect importere script fra åbent gitrepo
```
cat > /tmp/deploy_bootstrap.py << 'EOF'
from prefect import flow
flow.from_source(
    source='https://github.com/TuneClausen/prefect.git',
    entrypoint='bootstrap_geodkv.py:bootstrap_geodkv',
).deploy(name='bootstrap-geodkv', work_pool_name='default-pool')
EOF

sudo -iu prefect /opt/prefect/venv/bin/python /tmp/deploy_bootstrap.py
```
