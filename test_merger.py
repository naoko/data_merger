"""
see test discovery rule
https://pytest.org/latest/goodpractises.html#test-discovery
"""
import pytest

from merger import MetaDataExtractorStrategy, FileMerger
from merger import class_factory


@pytest.mark.parametrize("extractor, document", [
    ('YmlMetaExtractor', """
                              a: ["x", "y", "z"]
                              b:
                                c: 3
                                d: 4"""),
    ('JsonMetaExtractor', """{"a": ["x", "y", "z"],"b": {"c": 3, "d": 4}}"""),
])
def test_json_extractor_factory(extractor, document):
    extractor = MetaDataExtractorStrategy.data_extractor_factory(extractor)
    params = extractor(document).params
    assert params == {'a': ['x', 'y', 'z'], 'b': {'c': 3, 'd': 4}}

def test_meta_data_class_factory():
    attributes = {"a": [1, 2, 3]}
    new_class = class_factory("NewClass", attributes)
    cls = new_class()
    assert cls.a == [1, 2, 3]
