from gridflow.downloader import QueryHandler


def test_build_query():
    query_handler = QueryHandler()
    base_url = "https://esgf-node.llnl.gov/esg-search/search"
    params = {"project": "CMIP6", "variable": "tas"}
    query = query_handler.build_query(base_url, params)
    expected = (
        f"{base_url}?type=File&format=application%2Fsolr%2Bjson&limit=1000&distrib=true"
        "&project=CMIP6&variable=tas"
    )
    assert query == expected