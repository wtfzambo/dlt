import dlt, os
import requests
# Create a dlt pipeline that will load
# chess player data to the DuckDB destination

os.environ['DATA_WRITER__FILE_MAX_ITEMS'] = "40"

pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='duckdb',
    dataset_name='player_data'
)

global offset
offset = 0

@dlt.resource(name="items", write_disposition="replace", primary_key="id", merge_key="NA")
def load_items():
    # will produce 3 jobs for the main table with 40 items each
    # 6 jobs for the sub_items
    # 3 jobs for the sub_sub_items
    global offset
    for _, index in enumerate(range(offset, offset+120), 1):
        yield {
            "id": index,
            "name": f"item {index}",
            "sub_items": [{
                "id": index + 1000,
                "name": f"sub item {index + 1000}"
            },{
                "id": index + 2000,
                "name": f"sub item {index + 2000}",
                "sub_sub_items": [{
                    "id": index + 3000,
                    "name": f"sub item {index + 3000}",
                }]
            }]
            }

# Extract, normalize, and load the data
info = pipeline.run([load_items], table_name='player')
print(pipeline.last_trace)

print("=====================================")

@dlt.resource(name="items", write_disposition="replace", primary_key="id", merge_key="NA")
def load_items():
    # will produce 3 jobs for the main table with 40 items each
    # 6 jobs for the sub_items
    # 3 jobs for the sub_sub_items
    global offset
    for _, index in enumerate(range(offset, offset+120), 1):
        yield from []

# Extract, normalize, and load the data
info = pipeline.run([load_items], table_name='player')
print(pipeline.last_trace)
