from typing import Protocol
class ObjectDelivery(Protocol):
    def copy_batch(self, manifest_uri: str, run_id: str) -> dict: ...
    def verify(self, receipt: dict) -> bool: ...
class AzureDelivery:
    def copy_batch(self, manifest_uri, run_id):
        raise NotImplementedError('Implement Azure copy polling and checksum verification before enabling')
class GCSDelivery:
    def copy_batch(self, manifest_uri, run_id):
        raise NotImplementedError('Implement GCS rewrite-token loop and CRC32C verification before enabling')
