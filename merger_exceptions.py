class MergerFileIOException(Exception):
    """
    custom file IO exception
    """
    message = "File Merger was unable to read {}. {}"

    def __init__(self, file_path, exc):
        self.errors = self.message.format(file_path, exc)

    def __str__(self):
        return repr(self.errors)
