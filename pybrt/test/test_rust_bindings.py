import pytest
import pybrt
import polars


@pytest.fixture
def tantivity_index():
    pybrt.initialize_index("foo", "title", ["body"])


@pytest.fixture
def polars_df():
    df = polars.DataFrame(
        {
            "title": ["a", "b", "c"],
            "body": ["a text", "another text", "something"],
        }
    )
    return df


def test_initialization(tantivity_index):
    assert True


def test_indexing(tantivity_index):
    pybrt.index_document("foo", {"title": "a title", "text": "a text"})


def test_indexing_with_polars(tantivity_index, polars_df):
    pybrt.index_polars_dataframe("foo", polars_df)
    query = "text"
    results = pybrt.search("foo", query)
    assert set(results) == {"a", "b"}
