from typing import TYPE_CHECKING, Literal

from dlt.common.configuration import configspec
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.runners.configuration import PoolRunnerConfiguration, TPoolType
from dlt.common.storages import LoadStorageConfiguration, NormalizeStorageConfiguration, SchemaStorageConfiguration

TSchemaUpdateMode = Literal["update-schema", "freeze-and-filter", "freeze-and-raise", "freeze-and-discard"]


@configspec
class NormalizeConfiguration(PoolRunnerConfiguration):
    pool_type: TPoolType = "process"
    destination_capabilities: DestinationCapabilitiesContext = None  # injectable
    schema_update_mode: TSchemaUpdateMode = "update-schema"
    _schema_storage_config: SchemaStorageConfiguration
    _normalize_storage_config: NormalizeStorageConfiguration
    _load_storage_config: LoadStorageConfiguration

    if TYPE_CHECKING:
        def __init__(
            self,
            pool_type: TPoolType = "process",
            workers: int = None,
            _schema_storage_config: SchemaStorageConfiguration = None,
            _normalize_storage_config: NormalizeStorageConfiguration = None,
            _load_storage_config: LoadStorageConfiguration = None
        ) -> None:
            ...
