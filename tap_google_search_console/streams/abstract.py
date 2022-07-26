from abc import abstractmethod,ABC
from singer.metadata import get_standard_metadata,to_map,write

class BaseStream(ABC):
    """
    Base class representing generic stream methods and meta-attributes
    """
    @property
    @abstractmethod
    def replication_method(self) -> str:
        """
        Defines the sync mode of a stream
        """

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """
        Defines the sync mode of a stream
        """

    @property
    @abstractmethod
    def replication_key(self) -> str:
        """
        Defines the replication key for incremental sync mode of a stream
        """

    @property
    @abstractmethod
    def valid_replication_keys(self) -> tuple[str,str]:
        """
        Defines the replication key for incremental sync mode of a stream
        """

    @property
    @abstractmethod
    def key_properties(self) -> tuple[str,str]:
        """
        List of key properties for stream
        """

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """
        The unique identifier for the stream. This is allowed to be different from the name of the stream in order to allow for sources that have duplicate stream names.
        """

    @abstractmethod
    def get_records(self,):
        """
        TODO: Add Documentation
        """

    @abstractmethod
    def sync(self,):
        """
        TODO: Add Documentation
        """

    def __init__(self,client=None) -> None:
        self.client = client

    @classmethod
    def get_metadata(cls,schema) -> dict[str,str]:
        """
        Returns a `dict` for generating stream metadata
        """
        metadata = get_standard_metadata(**{
            "schema":schema,
            "key_properties":cls.key_properties,
            "valid_replication_keys":cls.valid_replication_keys,
            "replication_method":cls.replication_method or cls.forced_replication_method
            })
        
        if cls.replication_key is not None:
            metadata = write(metadata, (), 'replication-key', cls.replication_key)
        if cls.valid_replication_keys is not None:
            for replication_key in cls.valid_replication_keys:
                metadata = write(metadata,("properties", replication_key),"inclusion","automatic")
        return metadata

class IncremetalStream(BaseStream):
    """
    Base Class for Incremental Stream
    """

# ReplicationMethod.÷
    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"



class FullTableStream(BaseStream):
    """
    Base Class for Incremental Stream
    """

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_key = None