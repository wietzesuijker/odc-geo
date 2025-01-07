"""
Multipart upload interface.

Defines the `MultiPartUploadBase` class for implementing multipart upload functionality.
This interface standardises methods for initiating, uploading, and finalising
multipart uploads across storage backends.
"""

from abc import ABC, abstractmethod
from typing import Any, Union, TYPE_CHECKING

if TYPE_CHECKING:
    # pylint: disable=import-outside-toplevel,import-error
    import dask.bag


class MultiPartUploadBase(ABC):
    """Abstract base class for multipart upload."""

    @abstractmethod
    def initiate(self, **kwargs) -> str:
        """Initiate a multipart upload and return an identifier."""

    @abstractmethod
    def write_part(self, part: int, data: bytes) -> dict[str, Any]:
        """Upload a single part."""

    @abstractmethod
    def finalise(self, parts: list[dict[str, Any]]) -> str:
        """Finalise the upload with a list of parts."""

    @abstractmethod
    def cancel(self, other: str = ""):
        """Cancel the multipart upload."""

    @property
    @abstractmethod
    def url(self) -> str:
        """Return the URL of the upload target."""

    @property
    @abstractmethod
    def started(self) -> bool:
        """Check if the multipart upload has been initiated."""

    @abstractmethod
    def writer(self, kw: dict[str, Any], *, client: Any = None) -> Any:
        """
        Return a Dask-compatible writer for multipart uploads.

        :param kw: Additional parameters for the writer.
        :param client: Dask client for distributed execution.
        """

    @abstractmethod
    def upload(
        self,
        chunks: Union["dask.bag.Bag", list["dask.bag.Bag"]],
        *,
        mk_header: Any = None,
        mk_footer: Any = None,
        user_kw: dict[str, Any] | None = None,
        writes_per_chunk: int = 1,
        spill_sz: int = 20 * (1 << 20),
        client: Any = None,
        **kw,
    ) -> Any:
        """
        Orchestrate the upload process with multipart uploads.

        :param chunks: Dask bag of chunks to upload.
        :param mk_header: Function to create header data.
        :param mk_footer: Function to create footer data.
        :param user_kw: User-provided metadata for the upload.
        :param writes_per_chunk: Number of writes per chunk.
        :param spill_sz: Spill size for buffering data.
        :param client: Dask client for distributed execution.
        :return: A Dask delayed object representing the finalised upload.
        """
