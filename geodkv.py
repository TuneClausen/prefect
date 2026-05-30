
from prefect import flow
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.blocks.system import Secret

database_block = SqlAlchemyConnector.load("pg-db-auth")
with database_block:
    ...
from prefect.blocks.system import Secret

Secret(value="sk-1234567890").save("daf-geodkv-api", overwrite=True)

secret_block = Secret.load("BLOCK_NAME")

# Access the stored secret
secret_block.get()

from prefect import flow
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.blocks.system import Secret

@flow(log_prints=True)
def geodkv_flow():
    db = SqlAlchemyConnector.load("pg-db-auth")
    api_key = Secret.load("daf-geodkv-api").get()
    
    with db as connection:
        # her gør du noget med connection og api_key
        print(f"Connected. Key starter med {api_key[:4]}...")
