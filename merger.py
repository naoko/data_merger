import gzip
import os
import logging
import json

from yaml import load
import pandas as pd

import merger_exceptions


logger = logging.getLogger(__name__)


METADATA_TYPE = ('YmlMetaExtractor', 'JsonMetaExtractor', )

EXTRACTOR_CLASS_REGISTRY = {}
"""list of extractor classes"""

REQUIRED_KEYS = ('mapping_files', 'base_path', 'columns', 'base_file', 'out_path', )

def register_class(extractor_class):
    """

    """
    EXTRACTOR_CLASS_REGISTRY[extractor_class.__name__] = extractor_class
    return extractor_class


class MetaDataExtractorStrategy(object):
    """

    """
    def __init__(self, document):
        self.document = document

    def params(self):
        raise NotImplementedError

    @staticmethod
    def data_extractor_factory(metadata_type):
        if metadata_type not in METADATA_TYPE:
            raise TypeError("{} is not supported".format(metadata_type))
        return EXTRACTOR_CLASS_REGISTRY[metadata_type]

@register_class
class YmlMetaExtractor(MetaDataExtractorStrategy):
    """

    """
    def __init__(self, *args, **kwargs):
        super(YmlMetaExtractor, self).__init__(*args, **kwargs)

    @property
    def params(self):
        return load(self.document)


@register_class
class JsonMetaExtractor(MetaDataExtractorStrategy):
    """

    """
    def __init__(self, *args, **kwargs):
        super(JsonMetaExtractor, self).__init__(*args, **kwargs)

    @property
    def params(self):
        print self.document
        return json.loads(self.document)


def class_factory(cls_name, dict_attrs):
    """
    creates class dynamically for more readable dictionary value access
    :param cls_name: class name
    :param dict_attrs: key value pair for class attributes
    :return: class
    """
    return type(cls_name, (object, ), dict_attrs)


class FileMerger(object):
    """

    """
    def __init__(self, metadata):
        assert set(metadata.keys()).issubset(set(REQUIRED_KEYS)), "{} is not subset of {}".format(
            metadata.keys(), REQUIRED_KEYS)
        self.metadata = self._metadata(metadata)

    @staticmethod
    def _metadata(metadata):
        return class_factory("meta_data", metadata)

    @staticmethod
    def compress(file_path):
        outfilename = "{}.gz".format(file_path)
        # read file in binary mode
        with open(file_path, 'rb') as file_in:
            with gzip.open(outfilename, 'wb') as file_out:
                # write output
                file_out.writelines(file_in)

    def dataframe_from_csv(self, file_path, column=None):
        df = pd.read_csv(file_path)
        if column:
            df = df[column]
        return df

    def dataframe_merge(self, main_df, data_df, key, value):
        main_df = pd.merge(main_df, data_df, how='outer', left_on=[key], right_on=[key])
        self.metadata.columns.append(value)
        self.metadata.columns.pop(self.metadata.columns.index(key))
        return main_df[self.metadata.columns]

    def merge(self):
        main_df = self.dataframe_from_csv(
            os.path.join(self.metadata.base_path, self.metadata.base_file), column=self.metadata.columns)

        for file_type, meta in self.metadata.mapping_files.iteritems():
            logger.info("processing {}".format(file_type))
            try:
                file_path = os.path.join(self.metadata.base_path, meta["file_name"])
                data_df = self.dataframe_from_csv(file_path)
            except IOError as exc:
                logger.exception(msg=exc.message)
                raise merger_exceptions.MergerFileIOException(file_path, exc)
            main_df = self.dataframe_merge(main_df, data_df, meta["key"], meta["value"])

        logger.info("head: {}".format(main_df.head()))
        logger.info("tail: {}".format(main_df.tail()))
        logger.info("exporting....")
        main_df.to_csv(self.metadata.out_path)

        logger.info("compressing....")
        self.compress(self.metadata.out_path)


def main():
    # TODO: meta_data_file_path etc should be posted from REST request
    meta_data_file_path = "data/file_meta.yaml"
    extractor = MetaDataExtractorStrategy.data_extractor_factory('YmlMetaExtractor')
    params = extractor(open(meta_data_file_path, "rw")).params

    fm = FileMerger(metadata=params)
    fm.merge()


if __name__ == "__main__":
    main()
