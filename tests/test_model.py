import pytest

from stac_fastapi_pgstac_pair_search.models import PairSearchRequest


def test_model_validation():
    # Basic valid case
    valid_request = {
        "filter_expr": {
            "op": "and",
            "args": [
                {"op": "=", "args": [{"property": "first.id"}, "item1"]},
                {"op": "=", "args": [{"property": "second.id"}, "item2"]},
            ],
        }
    }
    PairSearchRequest.model_validate(valid_request)

    # Invalid case: missing filter_expr
    PairSearchRequest.model_validate({})

    # Invalid case: wrong type for filter_expr
    with pytest.raises(ValueError):
        PairSearchRequest.model_validate({"filter_expr": '"eo:cloud_cover" =< 80'})

    # Edge case: empty filter_expr
    empty_filter_request = {"filter_expr": {}}
    PairSearchRequest.model_validate(empty_filter_request)
