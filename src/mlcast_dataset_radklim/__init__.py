import importlib.metadata

try:
    __version__ = importlib.metadata.version("mlcast-dataset-radklim")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
