import gzip
import os
import logging
import json

from yaml import load
import pandas as pd

import merger_exceptions


logger = logging.getLogger(__name__)


METADATA_TYPE = ('YmlMetaExtractor', 'JsonMetaExtractor', )
FILE_TYPE = ('CsvLoader', )

EXTRACTOR_CLASS_REGISTRY = {}
"""list of extractor classes"""

DATA_LOADER_CLASS_REGISTRY = {}
"""list of data loader classes"""

REQUIRED_KEYS = ('mapping_files', 'base_path', 'columns', 'base_file', 'out_path', )


def extractor_registry(extractor_class):
    """

    """
    EXTRACTOR_CLASS_REGISTRY[extractor_class.__name__] = extractor_class
    return extractor_class


def loader_registry(loader_class):
    """

    """
    DATA_LOADER_CLASS_REGISTRY[loader_class.__name__] = loader_class
    return loader_class


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

@extractor_registry
class YmlMetaExtractor(MetaDataExtractorStrategy):
    """

    """
    def __init__(self, *args, **kwargs):
        super(YmlMetaExtractor, self).__init__(*args, **kwargs)

    @property
    def params(self):
        return load(self.document)


@extractor_registry
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


class DataLoaderStrategy(object):
    """

    """
    def __init__(self, file_path, columns=None):
        self.file_path = file_path
        self.columns = columns

    def load(self):
        raise NotImplementedError

    def export(self, df, out_path):
        """
        convert df to original file type save to out_path
        """
        raise NotImplementedError

    @staticmethod
    def data_extractor_factory(file_type):
        if file_type not in FILE_TYPE:
            raise TypeError("{} is not supported".format(file_type))
        return DATA_LOADER_CLASS_REGISTRY[file_type]


@loader_registry
class CsvLoader(DataLoaderStrategy):
    """
    convert from csv to data frame
    """
    def __init__(self, *args, **kwargs):
        super(CsvLoader, self).__init__(*args, **kwargs)

    def load(self):
        df = pd.read_csv(self.file_path)
        if self.columns:
            df = df[self.columns]
        return df

    def export(self, df, out_path):
        df.to_csv(out_path)


@loader_registry
class ExcelLoader(DataLoaderStrategy):
    """
    convert from excel to data frame
    """
    def __init__(self, *args, **kwargs):
        super(ExcelLoader, self).__init__(*args, **kwargs)

    def load(self):
        xls = pd.ExcelFile(self.file_path)
        df = xls.parse('Sheet1', index_col=self.columns, na_values=['NA'])
        return df

    def export(self, df, out_path):
        df.to_excel(out_path, sheet_name='Sheet1', engine='xlsxwriter')


class DataFrame(object):
    """
    this should be converted to strategy pattern so that it can take
    various file format such as Excel and csv
    """

    def __init__(self, loader, metadata):
        self._df = None
        self.loader = loader
        self.metadata = metadata

    @property
    def df(self):
        if self._df is None:
            self._df = self.loader.load()
        return self._df

    @df.setter
    def df(self, value):
        self._df = value

    @staticmethod
    def _data_frame_from_csv(file_path, column=None):
        df = pd.read_csv(file_path)
        if column:
            df = df[column]
        return df

    def data_frame_merge(self, data_df, key, value):
        merged_df = pd.merge(self.df, data_df, how='outer', left_on=[key], right_on=[key])
        self.metadata.columns.append(value)
        self.metadata.columns.pop(self.metadata.columns.index(key))
        self.df = merged_df[self.metadata.columns]

    def export(self):
        self.loader.export(self.df, self.metadata.out_path)


class FileMerger(object):
    """

    """
    def __init__(self, metadata, file_type):
        assert set(metadata.keys()).issubset(set(REQUIRED_KEYS)), "{} is not subset of {}".format(
            metadata.keys(), REQUIRED_KEYS)
        self.metadata = self._metadata(metadata)
        self.file_type = file_type

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

    def merge(self):
        loader_class = DataLoaderStrategy.data_extractor_factory(self.file_type)

        loader = loader_class(
            os.path.join(self.metadata.base_path, self.metadata.base_file),
            columns=self.metadata.columns)

        df_handler = DataFrame(
            loader,
            metadata=self.metadata,
        )

        for file_type, meta in self.metadata.mapping_files.iteritems():
            msg = "processing {}".format(file_type)
            print msg
            logger.info(msg)
            try:
                file_path = os.path.join(self.metadata.base_path, meta["file_name"])
                loader = loader_class(file_path)
                support_df_handler = DataFrame(loader, metadata=self.metadata)
            except IOError as exc:
                logger.exception(msg=exc.message)
                raise merger_exceptions.MergerFileIOException(file_path, exc)
            df_handler.data_frame_merge(support_df_handler.df, meta["key"], meta["value"])

        df_handler.export()

        logger.info("compressing....")
        self.compress(self.metadata.out_path)


def main():
    # TODO: meta_data_file_path etc should be posted from REST request
    meta_data_file_path = "data/file_meta.yaml"
    extractor = MetaDataExtractorStrategy.data_extractor_factory('YmlMetaExtractor')
    params = extractor(open(meta_data_file_path, "rw")).params

    fm = FileMerger(metadata=params, file_type='CsvLoader')
    fm.merge()


if __name__ == "__main__":
    main()
