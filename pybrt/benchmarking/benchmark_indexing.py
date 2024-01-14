import polars
import pybrt
from time import time
import pandas as pd
import logging
import eland
import elasticsearch
import warnings
from elasticsearch.exceptions import ElasticsearchWarning
import fire
import tantivy

warnings.simplefilter('ignore', ElasticsearchWarning)


class Timing:

    run_times = {}

    @staticmethod
    def timer_func(func):
        # This function shows the execution time of
        # the function object passed
        def wrap_func(*args, **kwargs):
            t1 = time()
            result = func(*args, **kwargs)
            t2 = time()
            elapsed = t2 - t1
            print(
                f'Function {func.__name__!r} executed in {(elapsed):.4f}s')
            Timing.run_times[func.__name__] = elapsed
            return result
        return wrap_func


@Timing.timer_func
def index_polars_pybrt(index_name, df):
    pybrt.initialize_index(index_name, "repo", [
        "tasks", "true_tasks", "dependencies"])
    print("Indexing...")
    pybrt.index_polars_dataframe("benchmarking", df)
    print("Searching...")
    pybrt.search("benchmarking", "object detection")


@Timing.timer_func
def query_pybrt(index_name, query):
    results = pybrt.search(index_name, query)
    print(f"Found {len(results)} results for query '{query}'")


@Timing.timer_func
def load_polars(path):
    return polars.read_csv(path).drop_nulls()


def pybrt_benchmark(path, query):
    print("loading df using polars")
    df = load_polars(path)

    index_name = "benchmarking"
    index_polars_pybrt("benchmarking", df)
    query_pybrt("benchmarking", query)


@Timing.timer_func
def index_pandas_eland(index_name, df):
    print("Indexing...")
    es_client = elasticsearch.Elasticsearch(hosts="http://localhost:9200")

    mapping = {col: "text" for col in df.columns}
    eland_df = eland.pandas_to_eland(
        df, es_dest_index="benchmark", es_client=es_client, es_if_exists="replace", es_type_overrides=mapping)
    return eland_df


@Timing.timer_func
def query_eland(edf, query):
    results = edf.es_match(
        query, columns=list(edf.columns))
    print(f"Found {len(results)} results for query '{query}'")


@Timing.timer_func
def load_pandas(path):
    return pd.read_csv(path).dropna()


def clean_pandas(df, invalid_cols=["", "Unnamed: 0"]):
    for col in invalid_cols:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df


def eland_benchmark(path, query):
    df = clean_pandas(load_pandas(path))
    index_name = "benchmarking"
    eland_df = index_pandas_eland(index_name, df)
    query_eland(eland_df, query)


@Timing.timer_func
def index_tantivy(indexname, df):
    schema_builder = tantivy.SchemaBuilder()
    for col in df.columns:
        schema_builder.add_text_field(col, stored=True)
    schema = schema_builder.build()
    index = tantivy.Index(schema)
    writer = index.writer()
    for i, row in df.iterrows():
        doc_elems = {col: [row[col]] for col in df.columns}
        doc = tantivy.Document(doc_id=i, **doc_elems)
        writer.add_document(doc)
    return index


@Timing.timer_func
def query_tantivy(index, query):
    parsed_query = index.parse_query(query)
    results = index.searcher().search(parsed_query).hits
    print(f"Found {len(results)} results for query '{query}'")


def tantivy_py_benchmark(path, query):
    df = clean_pandas(load_pandas(path))
    index_name = "benchmarking"
    tantivy_index = index_tantivy(index_name, df)
    query_tantivy(tantivy_index, query)


def log_time_relative(pybrt_time, other_name, other_function):
    other_time = Timing.run_times[other_function]
    time_ratio = (
        other_time /
        pybrt_time
    )
    print(
        f"pybrt indexing is {time_ratio:.2f}x faster than {other_name}")


def log_time_comparison():
    pybrt_time = Timing.run_times["index_polars_pybrt"]
    if "index_pandas_eland" in Timing.run_times.keys():
        log_time_relative(pybrt_time, "eland", "index_pandas_eland")
    if "index_tantivy" in Timing.run_times.keys():
        log_time_relative(pybrt_time, "tantivy", "index_tantivy")


def main(names, query="object detection", data_path="data/search_example.csv"):
    print("#" * 50)
    print("pybrt")
    print("#" * 50)
    pybrt_benchmark(data_path, query)
    print()

    for name in names.split(","):
        if name == "eland":
            print("#" * 50)
            print("eland")
            print("#" * 50)
            eland_benchmark(data_path, query)
            print()
        elif name == "tantivy-py":
            print("#" * 50)
            print("tantivy")
            print("#" * 50)
            tantivy_py_benchmark(data_path, query)
            print()
        else:
            raise ValueError(f"Unknown benchmark name {name}")
    log_time_comparison()


if __name__ == "__main__":

    fire.Fire(main)
