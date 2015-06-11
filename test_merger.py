"""
see test discovery rule
https://pytest.org/latest/goodpractises.html#test-discovery
"""
from merger import MetaDataExtractorStrategy, FileMerger
from merger import class_factory


def test_yml_extractor_factory():
    document = """
      a: ["x", "y", "z"]
      b:
        c: 3
        d: 4
    """
    extractor = MetaDataExtractorStrategy.data_extractor_factory('YmlMetaExtractor')
    parms = extractor(document).params
    assert parms == {'a': ['x', 'y', 'z'], 'b': {'c': 3, 'd': 4}}

def test_meta_data_class_factory():
    attributes = {"a": [1, 2, 3]}
    new_class = class_factory("NewClass", attributes)
    cls = new_class()
    assert cls.a == [1, 2, 3]
