# odc/geo/cog/_mpu.py
"""
Multi-part upload as a graph processing primitive.
Immutable MPUChunk implementation.
"""
# pylint: disable=too-many-lines

from __future__ import annotations
import logging
from functools import partial
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Optional,
    Protocol,
    Union,
)

# Dask imports
from dask.distributed import get_client, Client
from dask.base import tokenize
from dask.delayed import delayed, Delayed

if TYPE_CHECKING:
    import dask.bag
    from dask.distributed import Future

__all__ = [
    "SomeData",
    "PartsWriter",
    "MPUChunk",
    "mpu_write",
]

logger = logging.getLogger("odc.geo.cog._mpu")

SomeData = Union[bytes, bytearray]


class PartsWriter(Protocol):
    """Protocol defining the interface for a storage-specific part writer."""

    def __call__(self, part: int, data: SomeData) -> dict[str, Any]: ...
    def finalise(self, parts: list[dict[str, Any]]) -> Any: ...
    @property
    def min_write_sz(self) -> int: ...
    @property
    def max_write_sz(self) -> int: ...
    @property
    def min_part(self) -> int: ...
    @property
    def max_part(self) -> int: ...


# pylint: disable=too-many-instance-attributes
class MPUChunk:
    """
    Represents state during multi-part upload processing.
    Designed to be immutable: methods return new instances with updated state.
    """

    __slots__ = (
        "nextPartId",  # Internal attribute name
        "write_credits",
        "data",
        "left_data",
        "parts",
        "observed",
        "is_final",
        "lhs_keep",
    )

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        partId: int,  # Parameter name for __init__
        write_credits: int,
        data: Optional[bytearray] = None,
        left_data: Optional[bytearray] = None,
        parts: Optional[list[dict[str, Any]]] = None,
        observed: Optional[list[tuple[int, Any]]] = None,
        is_final: bool = False,
        lhs_keep: int = 0,
    ) -> None:
        self.nextPartId = partId  # Assign parameter to attribute
        self.write_credits = write_credits
        self.data = data if data is not None else bytearray()
        self.left_data = left_data if left_data is not None else bytearray()
        self.parts: list[dict[str, Any]] = parts if parts is not None else []
        self.observed: list[tuple[int, Any]] = observed if observed is not None else []
        self.is_final = is_final
        self.lhs_keep = lhs_keep
        assert data is None or observed is not None

    def _clone(self, **kwargs) -> "MPUChunk":
        """Helper to create a new instance with updated fields."""
        slot_values = {s: getattr(self, s) for s in self.__slots__}
        slot_values.update(kwargs)

        # **FIX:** Map internal attribute name to __init__ parameter name
        if "nextPartId" in slot_values:
            slot_values["partId"] = slot_values.pop("nextPartId")

        # Ensure mutable types are copied if not explicitly replaced by kwargs
        for field in ["data", "left_data", "parts", "observed"]:
            if field not in kwargs and field in slot_values:
                current_val = slot_values.get(field)  # Use .get for safety
                if isinstance(current_val, (bytearray, list)):
                    slot_values[field] = current_val[:]

        # Remove potential kwargs that are not __init__ parameters
        # (although with slots this shouldn't happen unless kwargs match slot names)
        valid_init_args = {"partId", "write_credits", "data", "left_data", "parts", "observed", "is_final", "lhs_keep"}
        init_kwargs = {k: v for k, v in slot_values.items() if k in valid_init_args}

        return MPUChunk(**init_kwargs)  # Call __init__ with corrected args

    def __dask_tokenize__(self):
        return (
            "MPUChunk",
            self.nextPartId,
            self.write_credits,
            len(self.data),
            hash(bytes(self.data)),
            len(self.left_data),
            hash(bytes(self.left_data)),
            tuple(map(tuple, map(sorted, map(dict.items, self.parts)))),
            self.observed,
            self.is_final,
            self.lhs_keep,
        )

    def __repr__(self) -> str:
        parts_summary = f"parts[{len(self.parts)}]" if self.parts else ""
        left_summary = f"left_data={len(self.left_data)}" if self.left_data else ""
        final_marker = " final" if self.is_final else ""
        attrs = filter(
            None,
            [
                f"nextPartId={self.nextPartId}",
                f"#credits={self.write_credits}",
                f"cache={len(self.data)}",
                left_summary,
                parts_summary,
                final_marker,
            ],
        )
        return f"MPUChunk({', '.join(attrs)})"

    def append(self, data: SomeData, chunk_id: Any = None) -> "MPUChunk":
        """Return a new MPUChunk with data appended to the buffer."""
        new_data = self.data + data
        new_observed = self.observed + [(len(data), chunk_id)]
        return self._clone(data=new_data, observed=new_observed)

    @property
    def started_write(self) -> bool:
        """Check if any parts have been written."""
        return bool(self.parts)

    @staticmethod
    def merge(
        lhs: "MPUChunk",
        rhs: "MPUChunk",
        write: Optional[PartsWriter] = None,  # Kept for signature, but unused
    ) -> "MPUChunk":
        """
        Merge two MPUChunk objects (lhs processed before rhs). Functional style.
        Returns a *new* merged chunk. Combines buffers correctly.
        """
        del write

        if not rhs.started_write:
            assert not rhs.left_data and not rhs.parts
            return MPUChunk(  # Direct call uses partId parameter name correctly
                lhs.nextPartId,
                lhs.write_credits + rhs.write_credits,
                lhs.data + rhs.data,
                lhs.left_data[:],
                lhs.parts[:],
                lhs.observed + rhs.observed,
                rhs.is_final,
                lhs.lhs_keep,
            )

        new_left_data = lhs.left_data + lhs.data + rhs.left_data

        return MPUChunk(  # Direct call uses partId parameter name correctly
            rhs.nextPartId,
            rhs.write_credits,
            rhs.data[:],
            new_left_data,
            lhs.parts + rhs.parts,
            lhs.observed + rhs.observed,
            rhs.is_final,
            lhs.lhs_keep,
        )

    # pylint: disable=too-many-statements, too-many-branches
    def flush_rhs(self, write: Optional[PartsWriter], extra_data: Optional[bytearray] = None) -> "MPUChunk":
        """
        Flushes internal buffer (`.data` + `extra_data`) if conditions are met.
        Returns a *new* MPUChunk instance reflecting the state after the flush attempt.
        Handles multi-part writes and lhs_keep separation.
        """
        if write is None:
            combined_len = len(self.data) + (len(extra_data) if extra_data else 0)
            if combined_len > 0:
                raise RuntimeError("Flush required but no writer provided")
            return self

        data_to_flush = self.data[:]  # Copy current buffer
        if extra_data:
            data_to_flush += extra_data

        writeable_len = len(data_to_flush)
        first_write = not self.started_write

        if first_write and self.lhs_keep > 0:
            writeable_len = max(0, writeable_len - self.lhs_keep)

        should_flush = (self.is_final and writeable_len > 0) or (
            not self.is_final and writeable_len >= write.min_write_sz
        )

        if not self.is_final and self.write_credits < 1:
            should_flush = False

        if not should_flush:
            # Return new state with combined data moved to .data
            return self._clone(data=data_to_flush, left_data=self.left_data[:])

        # --- Proceed with Flush - Calculate new state ---
        new_data_buffer = bytearray()
        new_left_data_buffer = self.left_data[:]
        new_parts = self.parts[:]
        new_next_part_id = self.nextPartId
        new_credits = self.write_credits
        max_chunk_size = write.max_write_sz
        current_write_data = data_to_flush

        if first_write and self.lhs_keep > 0:
            if len(current_write_data) < self.lhs_keep:
                new_left_data_buffer += current_write_data
                # Return state with data moved to left_data, nothing flushed
                return self._clone(data=new_data_buffer, left_data=new_left_data_buffer)

            if new_left_data_buffer:
                logger.warning("flush_rhs: Overwriting non-empty left_data.")
            new_left_data_buffer = bytearray(current_write_data[: self.lhs_keep])
            current_write_data = current_write_data[self.lhs_keep :]

        offset = 0
        total_size_to_write = len(current_write_data)
        while offset < total_size_to_write:
            if new_credits < 1:
                new_data_buffer = bytearray(current_write_data[offset:])
                logger.error("flush_rhs: Ran out of credits at part %d.", new_next_part_id)
                # Return state *before* the failed write attempt
                return MPUChunk(
                    new_next_part_id,
                    new_credits,
                    new_data_buffer,
                    new_left_data_buffer,
                    new_parts,
                    self.observed[:],
                    self.is_final,
                    self.lhs_keep,
                )

            if not write.min_part <= new_next_part_id <= write.max_part:
                new_data_buffer = bytearray(current_write_data[offset:])
                # Return state before invalid write attempt
                return MPUChunk(
                    new_next_part_id,
                    new_credits,
                    new_data_buffer,
                    new_left_data_buffer,
                    new_parts,
                    self.observed[:],
                    self.is_final,
                    self.lhs_keep,
                )
                # raise ValueError(f"flush_rhs: Part ID {new_next_part_id} out of range")

            chunk_size = min(total_size_to_write - offset, max_chunk_size)
            chunk_data = current_write_data[offset : offset + chunk_size]

            try:
                part = write(new_next_part_id, bytes(chunk_data))
            except Exception as e:
                logger.error("flush_rhs: Writer failed part %d: %s", new_next_part_id, e, exc_info=True)
                new_data_buffer = bytearray(current_write_data[offset:])
                # Return state before failed write attempt
                return MPUChunk(
                    new_next_part_id,
                    new_credits,
                    new_data_buffer,
                    new_left_data_buffer,
                    new_parts,
                    self.observed[:],
                    self.is_final,
                    self.lhs_keep,
                )
                # raise RuntimeError(f"Writer failed for part {new_next_part_id}") from e

            if part.get("PartNumber") != new_next_part_id:
                logger.warning(
                    "PartNumber mismatch part %d: Writer returned %s", new_next_part_id, part.get("PartNumber")
                )

            new_parts.append(part)
            new_next_part_id += 1
            new_credits -= 1
            offset += chunk_size

        return MPUChunk(
            new_next_part_id,
            new_credits,
            new_data_buffer,
            new_left_data_buffer,
            new_parts,
            self.observed[:],
            self.is_final,
            self.lhs_keep,
        )

    def flush(
        self,
        write: PartsWriter,
        leftPartId: Optional[int] = None,
        finalise: bool = True,
    ) -> tuple["MPUChunk", Any]:
        """
        Flush remaining data and optionally finalize. Returns new state and result.
        Handles left_data flushing correctly based on finalise flag.
        """
        current_state: MPUChunk = self
        result = None

        if not current_state.started_write:
            if current_state.left_data:
                logger.warning("flush: Non-empty left_data when !started_write.")
            partId = current_state.nextPartId if leftPartId is None else leftPartId
            new_parts = current_state.parts[:]
            new_next_part_id = current_state.nextPartId
            new_credits = current_state.write_credits
            if current_state.data:
                part_info = write(partId, current_state.data)
                new_parts.append(part_info)
                new_next_part_id += 1
                new_credits -= 1
            # Create new state with cleared buffer
            current_state = self._clone(
                data=bytearray(), parts=new_parts, nextPartId=new_next_part_id, write_credits=new_credits
            )
        else:
            # Flush main buffer first if data exists
            if current_state.data:
                original_final_flag = current_state.is_final
                # Create a temporary state to pass to flush_rhs
                temp_state = current_state._clone(is_final=finalise or current_state.is_final)
                current_state = temp_state.flush_rhs(write)  # Update state with result of flush
                current_state.is_final = original_final_flag  # Restore original flag

            # Flush left_data conditionally from the potentially updated state
            if current_state.left_data:
                if finalise or len(current_state.left_data) >= write.min_write_sz:
                    partId = write.min_part if leftPartId is None else leftPartId
                    part_info = write(partId, current_state.left_data)
                    new_parts = current_state.parts[:]
                    new_parts.insert(0, part_info)
                    # Create new state with cleared left_data
                    current_state = current_state._clone(left_data=bytearray(), parts=new_parts)

        if finalise:
            result = write.finalise(current_state.parts)

        return current_state, result  # Return the final state and result

    def maybe_write(self, write: PartsWriter, spill_sz: int) -> "MPUChunk":
        """
        Conditionally write a part if enough data accumulated. Returns new state.
        """
        rhs_keep = 0 if self.is_final else write.min_write_sz
        lhs_keep = 0 if self.started_write else self.lhs_keep
        parts_to_keep = 0 if self.is_final else 1

        if self.write_credits - parts_to_keep < 1:
            return self

        bytes_available = len(self.data) - rhs_keep - lhs_keep
        if bytes_available < spill_sz:
            return self

        write_chunk_size = min(bytes_available, write.max_write_sz)
        if write_chunk_size <= 0:
            return self

        # --- Prepare state for the new chunk ---
        new_data = self.data[:]
        new_left_data = self.left_data[:]
        new_parts = self.parts[:]

        spill_data: bytearray
        if lhs_keep == 0:
            spill_data = new_data[:write_chunk_size]
            new_data = new_data[write_chunk_size:]
        else:
            spill_data = new_data[lhs_keep : lhs_keep + write_chunk_size]
            if new_left_data:
                logger.warning("maybe_write: Overwriting existing left_data.")
            new_left_data = new_data[:lhs_keep]
            new_data = new_data[lhs_keep + write_chunk_size :]

        if not write.min_part <= self.nextPartId <= write.max_part:
            raise ValueError(f"maybe_write: Next Part ID {self.nextPartId} out of range")

        try:
            part_info = write(self.nextPartId, bytes(spill_data))
        except Exception as e:
            logger.error("maybe_write: Writer failed part %d: %s", self.nextPartId, e, exc_info=True)
            raise RuntimeError(f"Writer failed during maybe_write for part {self.nextPartId}") from e

        new_parts.append(part_info)
        new_nextPartId = self.nextPartId + 1
        new_write_credits = self.write_credits - 1

        return MPUChunk(  # Return new state
            new_nextPartId,
            new_write_credits,
            new_data,
            new_left_data,
            new_parts,
            self.observed[:],
            self.is_final,
            self.lhs_keep,
        )

    @staticmethod
    def gen_bunch(
        partId: int, n: int, *, writes_per_chunk: int = 1, mark_final: bool = False, lhs_keep: int = 0
    ) -> Iterator["MPUChunk"]:
        """Generate initial MPUChunk states with consistent lhs_keep."""
        for idx in range(n):
            is_final = mark_final and idx == (n - 1)
            yield MPUChunk(
                partId + idx * writes_per_chunk,
                writes_per_chunk,
                is_final=is_final,
                lhs_keep=lhs_keep,  # Consistent lhs_keep
            )

    @staticmethod
    def from_dask_bag(
        partId: int,
        chunks: "dask.bag.Bag",
        *,
        writes_per_chunk: int = 1,
        mark_final: bool = False,
        lhs_keep: int = 0,
        write: Optional[PartsWriter] = None,
        spill_sz: int = 0,
        split_every: int = 4,
    ) -> "dask.bag.Item":
        """Create a Dask Bag computation graph for MPU processing."""
        # pylint: disable=import-outside-toplevel
        import dask.bag

        mpus = dask.bag.from_sequence(
            list(
                MPUChunk.gen_bunch(
                    partId,
                    chunks.npartitions,
                    writes_per_chunk=writes_per_chunk,
                    mark_final=mark_final,
                    lhs_keep=lhs_keep,
                )
            ),
            npartitions=chunks.npartitions,
        )

        mpus = dask.bag.map_partitions(
            _mpu_append_chunks_op,
            mpus,
            chunks,
            write=write,
            spill_sz=spill_sz,
            token="mpu.append",
        )

        return mpus.fold(
            partial(_merge_and_spill_op, write=write, spill_sz=spill_sz),
            split_every=split_every,
        )

    @staticmethod
    def collate_substreams(
        substreams: list["dask.bag.Item"], *, write: Optional[PartsWriter] = None, spill_sz: int = 0
    ) -> "dask.bag.Item":
        """Merge multiple MPUChunk Items sequentially using a delayed task."""
        # pylint: disable=import-outside-toplevel
        import dask.bag

        if not substreams:
            raise ValueError("Cannot collate an empty list of substreams.")

        return dask.bag.Item.from_delayed(
            delayed(_mpu_collate_op)(substreams, pure=False, write=write, spill_sz=spill_sz)
        )


# pylint: disable=too-many-arguments
def mpu_write(
    chunks: Union[dask.bag.Bag, list[dask.bag.Bag]],
    write: Optional[PartsWriter] = None,
    *,
    mk_header: Any = None,
    mk_footer: Any = None,
    user_kw: Optional[dict[str, Any]] = None,
    writes_per_chunk: int = 1,
    spill_sz: int = 20 * (1 << 20),
    dask_name_prefix: str = "mpufinalise",
    client: Any = None,
) -> Delayed:
    """
    Constructs the main Dask graph for multi-part upload.
    """
    # pylint: disable=too-many-locals

    if not isinstance(chunks, list):
        chunks = [chunks]

    min_part = write.min_part if write else 1
    lhs_keep = write.min_write_sz if write and spill_sz > 0 else 0

    partId = min_part
    dss: list["dask.bag.Item"] = []
    for idx, ch_bag in enumerate(chunks):
        # pylint: disable=import-outside-toplevel
        import dask.bag

        if not isinstance(ch_bag, dask.bag.Bag):
            raise TypeError(f"Input item {idx} is not a dask.bag.Bag")
        sub = MPUChunk.from_dask_bag(
            partId,
            ch_bag,
            writes_per_chunk=writes_per_chunk,
            lhs_keep=lhs_keep,  # Pass consistent lhs_keep
            spill_sz=spill_sz,
            mark_final=(mk_footer is None and idx == len(chunks) - 1),
            write=write,
        )
        dss.append(sub)
        partId += ch_bag.npartitions * writes_per_chunk

    data_substream = dss[0] if len(dss) == 1 else MPUChunk.collate_substreams(dss, write=write, spill_sz=spill_sz)

    token_data = (write, mk_header, mk_footer, user_kw, spill_sz, data_substream)
    tk = tokenize(*token_data)
    name = f"{dask_name_prefix}-{tk}"

    finalizer_user_kw = user_kw or {}
    if client is not None and "client" not in finalizer_user_kw:
        finalizer_user_kw = {**finalizer_user_kw, "client": client}

    return delayed(_finalizer_dask_op, name=name, pure=False)(
        data_substream,
        write=write,
        mk_header=mk_header,
        mk_footer=mk_footer,
        user_kw=finalizer_user_kw,
        # final_task_timeout=1200, # Pass timeout if needed
    )


# --- Helper functions for Dask operations ---


def _mpu_collate_op(substreams: list[MPUChunk], *, write: Optional[PartsWriter] = None, spill_sz: int = 0) -> MPUChunk:
    """Dask delayed function: Sequentially merge MPUChunk results (immutable style)."""
    if not substreams:
        raise ValueError("Received empty list of substreams to collate.")
    root = substreams[0]
    for rhs in substreams[1:]:
        root = MPUChunk.merge(root, rhs, write=None)
        if write and spill_sz > 0:
            root = root.maybe_write(write, spill_sz)  # Use returned state
    return root


def _mpu_append_chunks_op(
    mpus: Iterable[MPUChunk],
    chunks: Iterable[tuple[bytes, Any]],
    write: Optional[PartsWriter] = None,
    spill_sz: int = 0,
) -> list[MPUChunk]:
    """Dask map_partitions op: Append data chunks, maybe spill (immutable style)."""
    try:
        mpu = next(iter(mpus))
    except StopIteration as exc:
        raise ValueError("MPUChunk state missing in map_partitions.") from exc

    for data, chunk_id in chunks:
        mpu = mpu.append(data, chunk_id)  # Update mpu with returned state
        if write is not None and spill_sz > 0:
            mpu = mpu.maybe_write(write, spill_sz)  # Update mpu with returned state

    return [mpu]


def _merge_and_spill_op(
    lhs: MPUChunk, rhs: MPUChunk, write: Optional[PartsWriter] = None, spill_sz: int = 0
) -> MPUChunk:
    """Dask fold op: Merge two MPUChunks, maybe spill (immutable style)."""
    merged_chunk = MPUChunk.merge(lhs, rhs, write=None)
    if write is not None and spill_sz > 0:
        merged_chunk = merged_chunk.maybe_write(write, spill_sz)  # Update state
    return merged_chunk


# pylint: disable=too-many-locals, too-many-statements, too-many-branches
def _finalizer_dask_op(
    data_substream: "MPUChunk",
    *,
    write: Optional["PartsWriter"] = None,
    mk_header: Any = None,
    mk_footer: Any = None,
    user_kw: Optional[dict[str, Any]] = None,
    final_task_timeout: int = 1200,  # Increased timeout
    # pylint: disable=unused-argument
    dask_key_name: Optional[str] = None,
):
    """
    Dask graph node: Final step processing header/footer and submitting
    the actual remote flush/commit task.
    """
    user_kw = user_kw or {}
    final_chunk = data_substream

    # --- Compute and Merge Header/Footer ---
    try:
        hdr_bytes, footer_bytes = [
            None if op is None else op(final_chunk.observed, **user_kw) for op in [mk_header, mk_footer]
        ]
        if footer_bytes:
            if not isinstance(footer_bytes, (bytes, bytearray)):
                raise TypeError("mk_footer must return bytes/bytearray")
            final_chunk = final_chunk.append(footer_bytes)  # Update reference
        if hdr_bytes:
            if not isinstance(hdr_bytes, (bytes, bytearray)):
                raise TypeError("mk_header must return bytes/bytearray")
            header_part_id = write.min_part if write else 1
            hdr_chunk = MPUChunk(header_part_id, 1, lhs_keep=final_chunk.lhs_keep)
            hdr_chunk = hdr_chunk.append(hdr_bytes)
            final_chunk = MPUChunk.merge(hdr_chunk, final_chunk, write=None)  # Update reference
    except Exception as e:
        logger.error("Error processing header/footer: %s", e, exc_info=True)
        raise RuntimeError("Failed during header/footer processing.") from e
    # --- End Header/Footer ---

    if write is None:
        return final_chunk

    # --- Submit Final Remote Task ---
    client: Optional[Client] = None
    try:
        client = get_client()
    except ValueError as exc:
        raise RuntimeError("Dask client not found in _finalizer_dask_op") from exc
    except Exception as exc:
        raise RuntimeError("Failed to get Dask client.") from exc

    future: Optional[Future] = None
    try:
        future = client.submit(_remote_final_flush, final_chunk, write, pure=False)
        logger.info("Submitted remote final flush task: %s", future.key)
    except Exception as exc:
        raise RuntimeError("Dask task submission failed.") from exc
    # --- End Submit ---

    # --- Wait for Result ---
    final_upload_result = None
    try:
        logger.info("Waiting for remote final task %s (timeout=%ds)...", future.key, final_task_timeout)
        final_upload_result = future.result(timeout=final_task_timeout)
        logger.info("Remote final task %s COMPLETED successfully.", future.key)
    except FuturesTimeoutError:
        logger.error("Timed out waiting for remote final task %s after %ds.", future.key, final_task_timeout)
        try:
            future.cancel(asynchronous=True)
            logger.warning("Cancellation requested for task: %s", future.key)
        except Exception as cancel_e:
            logger.error("Error cancelling task %s: %s", future.key, cancel_e)
        raise
    except Exception as exc:
        logger.error("Remote final task %s failed: %s", future.key, exc, exc_info=True)
        raise
    finally:
        if future:
            future.release()
    # --- End Wait ---

    return final_upload_result


def _remote_final_flush(chunk_state: MPUChunk, writer_obj: PartsWriter):
    """Function executed remotely on worker to perform final flush/commit."""
    worker_logger = logging.getLogger(__name__)
    worker_logger.info("Executing remote final flush/commit task for %s state: %s", type(writer_obj), chunk_state)
    try:
        # flush returns (new_state, result)
        _final_state, final_result = chunk_state.flush(writer_obj, leftPartId=None, finalise=True)
        worker_logger.info("Remote final flush/commit task COMPLETED. Result: %s", final_result)
        return final_result
    except Exception as worker_e:
        worker_logger.error("Error during remote final flush/commit task: %s", worker_e, exc_info=True)
        raise


def get_mpu_kwargs(
    mk_header=None, mk_footer=None, user_kw=None, writes_per_chunk=1, spill_sz=20 * (1 << 20), client=None
) -> dict:
    """Helper to construct shared keyword arguments for MPU functions."""
    return {
        "mk_header": mk_header,
        "mk_footer": mk_footer,
        "user_kw": user_kw,
        "writes_per_chunk": writes_per_chunk,
        "spill_sz": spill_sz,
        "client": client,
    }


# pylint: disable=too-many-arguments
def mpu_upload(
    chunks: Union[dask.bag.Bag, list[dask.bag.Bag]],
    *,
    writer: Any,  # Expects writer *factory method*
    dask_name_prefix: str,
    mk_header: Any = None,
    mk_footer: Any = None,
    user_kw: Optional[dict[str, Any]] = None,
    writes_per_chunk: int = 1,
    spill_sz: int = 20 * (1 << 20),
    client: Any = None,
    **writer_factory_kw: Any,
) -> Delayed:
    """
    High-level function to set up and execute MPU using Dask Bags.
    Separates MPU parameters from writer factory parameters.
    """
    factory_call_kw = {"kw": writer_factory_kw, "client": client}
    write_instance: Optional[PartsWriter] = None

    if spill_sz > 0:
        try:
            write_instance = writer(**factory_call_kw)
        except TypeError as e:
            logger.error("TypeError calling writer factory %s with %s: %s", writer, factory_call_kw, e)
            raise TypeError(f"Writer factory {writer} incompatible with arguments.") from e
        except Exception as e:
            logger.error("Failed to instantiate writer callable from %s: %s", writer, e, exc_info=True)
            raise RuntimeError("Writer factory instantiation failed.") from e

    return mpu_write(
        chunks,
        write_instance,
        mk_header=mk_header,
        mk_footer=mk_footer,
        user_kw=user_kw,
        writes_per_chunk=writes_per_chunk,
        spill_sz=spill_sz,
        dask_name_prefix=dask_name_prefix,
        client=client,
    )
