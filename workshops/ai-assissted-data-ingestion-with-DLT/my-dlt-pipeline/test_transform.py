import dlt

def transform_books(item):
    # item is the dict from the API
    result = []
    for key, value in item.items():
        if isinstance(value, dict):
            value["isbn"] = key
            result.append(value)
    return result

@dlt.resource
def dummy_source():
    yield {"ISBN:0451526538": {"bib_key": "ISBN:0451526538", "title": "Tom Sawyer"}, "ISBN:0261103253": {"bib_key": "ISBN:0261103253", "title": "LOTR"}}

pipeline = dlt.pipeline(pipeline_name="test_transform", destination="duckdb")
# test map
res = dummy_source()
res.add_yield_map(transform_books)

pipeline.run(res)
