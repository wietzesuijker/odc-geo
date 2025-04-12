# odc/geo/cog/_mpu.py
"""
Multi-part upload as a graph processing primitive.
IMMUTABLE MPUChunk implementation focused on correctness with Dask.
Fixes _clone TypeError and _write_part AttributeError by inlining write logic.
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
    Tuple,
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
    Represents state during multi-part upload processing. Immutable design:
    methods return new instances with updated state.
    """

    __slots__ = (
        "nextPartId",
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
        partId: int,  # Init parameter name
        write_credits: int,
        data: Optional[bytearray] = None,
        left_data: Optional[bytearray] = None,
        parts: Optional[list[dict[str, Any]]] = None,
        observed: Optional[list[tuple[int, Any]]] = None,
        is_final: bool = False,
        lhs_keep: int = 0,
    ) -> None:
        self.nextPartId = partId  # Internal attribute name
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
        slot_values = {s: getattr(self, s) for s in self.__slots__ if hasattr(self, s)}
        slot_values.update(kwargs)
        # Map internal attribute name to __init__ parameter name
        if "nextPartId" in slot_values:
            slot_values["partId"] = slot_values.pop("nextPartId")
        # Ensure mutable types are copied if not explicitly replaced by kwargs
        for field in ["data", "left_data", "parts", "observed"]:
            if field not in kwargs and field in slot_values:
                current_val = slot_values.get(field)
                if isinstance(current_val, (bytearray, list)):
                    slot_values[field] = current_val[:]  # Shallow copy
        valid_init_args = {"partId", "write_credits", "data", "left_data", "parts", "observed", "is_final", "lhs_keep"}
        init_kwargs = {k: v for k, v in slot_values.items() if k in valid_init_args}
        return MPUChunk(**init_kwargs)

    def __dask_tokenize__(self):
        # Token based on immutable fields and summaries of mutable ones
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
        """Return NEW chunk with data appended."""
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
        write: Optional[PartsWriter] = None,  # Unused
    ) -> "MPUChunk":
        """Merge two chunks (functional style). Returns NEW chunk."""
        del write
        if not rhs.started_write:
            assert not rhs.left_data and not rhs.parts
            # Combine buffers, keeping lhs.left_data separate if present
            return MPUChunk(
                lhs.nextPartId,
                lhs.write_credits + rhs.write_credits,
                lhs.data + rhs.data,  # Combine main buffers
                lhs.left_data[:],  # Keep lhs leftover
                lhs.parts[:],
                lhs.observed + rhs.observed,
                rhs.is_final,
                lhs.lhs_keep,
            )

        # If rhs wrote, combine LHS leftover/buffer + RHS leftover -> new leftover
        new_left_data = lhs.left_data + lhs.data + rhs.left_data
        return MPUChunk(
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
    def flush(
        self,
        write: PartsWriter,
        leftPartId: Optional[int] = None,
        finalise: bool = True,
    ) -> Tuple["MPUChunk", Any]:
        """
        Flush all remaining data and optionally finalize. Returns new state and result.
        Handles left_data correctly. Immutable. Inlines write logic.
        """
        current_state: MPUChunk = self
        result = None
        new_parts = current_state.parts[:]
        new_next_part_id = current_state.nextPartId
        new_credits = current_state.write_credits
        new_left_data = current_state.left_data[:]
        new_data = current_state.data[:]

        # 1. Handle left_data (header/lhs_keep)
        if new_left_data:
            if finalise or len(new_left_data) >= write.min_write_sz:
                partId_for_left = write.min_part if leftPartId is None else leftPartId
                if any(p.get("PartNumber") == partId_for_left for p in new_parts):
                    logger.warning("Flush: left_data Part ID %d might clash!", partId_for_left)

                # --- Inlined write logic for left_data ---
                data_to_write = bytes(new_left_data)
                part_id = partId_for_left
                if new_credits < 1:
                    raise RuntimeError(f"Insufficient credits: left part {part_id}")
                if not write.min_part <= part_id <= write.max_part:
                    raise ValueError(f"Part ID {part_id} out of range")
                if len(data_to_write) > write.max_write_sz:
                    logger.warning("Writing oversized left_data part %d", part_id)
                try:
                    part_info = write(part_id, data_to_write)
                except Exception as e:
                    raise RuntimeError(f"Writer failed for left_data part {part_id}") from e
                if part_info.get("PartNumber") != part_id:
                    logger.warning("PartNumber mismatch %d", part_id)
                # --- End Inlined write ---

                new_parts.insert(0, part_info)
                new_left_data = bytearray()  # Clear buffer for new state
                if partId_for_left >= new_next_part_id:
                    new_next_part_id = part_id + 1
                new_credits -= 1

        # 2. Handle main data buffer (potentially multiple parts)
        if new_data:
            data_to_flush = new_data[:]
            offset = 0
            total_size_to_write = len(data_to_flush)
            max_chunk_size = write.max_write_sz

            while offset < total_size_to_write:
                # --- Inlined write logic for main data buffer parts ---
                part_id = new_next_part_id
                if new_credits < 1:
                    raise RuntimeError(f"Insufficient credits: part {part_id}")
                if not write.min_part <= part_id <= write.max_part:
                    raise ValueError(f"Part ID {part_id} out of range")

                chunk_size = min(total_size_to_write - offset, max_chunk_size)
                chunk_data = data_to_flush[offset : offset + chunk_size]

                if len(chunk_data) > write.max_write_sz:
                    logger.warning("Writing oversized data part %d", part_id)

                try:
                    part_info = write(part_id, bytes(chunk_data))
                except Exception as e:
                    raise RuntimeError(f"Writer failed for data part {part_id}") from e
                if part_info.get("PartNumber") != part_id:
                    logger.warning("PartNumber mismatch %d", part_id)
                # --- End Inlined write ---

                new_parts.append(part_info)
                new_next_part_id = part_id + 1
                new_credits -= 1
                offset += chunk_size

            new_data = bytearray()  # Clear buffer after writing all parts

        # 3. Create final state object reflecting flushed buffers
        final_state = MPUChunk(
            new_next_part_id,
            new_credits,
            new_data,
            new_left_data,
            new_parts,
            current_state.observed[:],
            finalise or current_state.is_final,
            current_state.lhs_keep,
        )

        # 4. Finalise if requested
        if finalise:
            result = write.finalise(final_state.parts)

        return final_state, result

    def maybe_write(self, write: PartsWriter, spill_sz: int) -> "MPUChunk":
        """
        Conditionally write a part if enough data accumulated. Returns new state. Immutable.
        Uses inlined write logic. Respects lhs_keep correctly. Writes one part max.
        """
        rhs_keep = 0 if self.is_final else write.min_write_sz
        lhs_keep = 0 if self.started_write else self.lhs_keep
        parts_to_keep = 0 if self.is_final else 1

        if self.write_credits - parts_to_keep < 1:
            return self  # Cannot write

        # Determine spillable data amount
        data_to_consider = self.data
        is_first_write_attempt = not self.started_write
        if is_first_write_attempt and lhs_keep > 0:
            if len(self.data) <= lhs_keep:
                return self
            data_to_consider = self.data[lhs_keep:]

        bytes_available_for_spill = len(data_to_consider) - rhs_keep
        if bytes_available_for_spill < spill_sz:
            return self

        # Determine data to write and new state buffers
        data_to_write: bytes
        new_data_after_write: bytearray
        new_left_data: bytearray
        part_id_to_use = self.nextPartId

        if is_first_write_attempt and lhs_keep > 0:
            # Write lhs_keep + first part of spillable data together
            write_chunk_size = min(len(data_to_consider), write.max_write_sz)
            data_to_write = bytes(self.data[: lhs_keep + write_chunk_size])
            new_left_data = bytearray()  # Keep data is being written
            new_data_after_write = self.data[lhs_keep + write_chunk_size :]
        else:
            # Write from the main buffer, respecting max size and rhs_keep
            write_chunk_size = min(len(self.data) - rhs_keep, write.max_write_sz)
            if write_chunk_size <= 0:
                return self  # Safety check
            data_to_write = bytes(self.data[:write_chunk_size])
            new_data_after_write = self.data[write_chunk_size:]
            new_left_data = self.left_data[:]  # Keep existing left_data

        # --- Perform the Write (Inlined Logic) ---
        if self.write_credits < 1:
            raise RuntimeError(f"Insufficient credits: part {part_id_to_use}")
        if not write.min_part <= part_id_to_use <= write.max_part:
            raise ValueError(f"Part ID {part_id_to_use} out of range")
        if len(data_to_write) > write.max_write_sz:
            logger.warning("maybe_write: Writing oversized part %d", part_id_to_use)

        try:
            part_info = write(part_id_to_use, data_to_write)
        except Exception as e:
            logger.error("maybe_write: Writer failed part %d: %s", part_id_to_use, e, exc_info=True)
            raise RuntimeError(f"Writer failed during maybe_write for part {part_id_to_use}") from e
        if part_info.get("PartNumber") != part_id_to_use:
            logger.warning("PartNumber mismatch %d", part_id_to_use)
        # --- End Inlined Write ---

        new_parts = self.parts + [part_info]
        new_next_id = part_id_to_use + 1
        new_credits = self.write_credits - 1

        # Return new state
        return self._clone(
            data=new_data_after_write,
            left_data=new_left_data,
            parts=new_parts,
            nextPartId=new_next_id,
            write_credits=new_credits,
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
            token="mpu.append",  # Pass spill_sz
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
    spill_sz: int = 0,  # Default spill_sz=0
    dask_name_prefix: str = "mpufinalise",
    client: Any = None,
) -> Delayed:
    """
    Constructs the main Dask graph for MPU (immutable style).
    """
    # pylint: disable=too-many-locals

    if not isinstance(chunks, list):
        chunks = [chunks]

    min_part = write.min_part if write else 1
    lhs_keep = write.min_write_sz if write and mk_header else 0

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
            lhs_keep=lhs_keep,  # Consistent lhs_keep
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
    )


# --- Helper functions for Dask operations ---


def _mpu_collate_op(substreams: list[MPUChunk], *, write: Optional[PartsWriter] = None, spill_sz: int = 0) -> MPUChunk:
    """Dask delayed function: Sequentially merge MPUChunk results (immutable style)."""
    if not substreams:
        raise ValueError("Cannot collate empty list.")
    root = substreams[0]
    for rhs in substreams[1:]:
        root = MPUChunk.merge(root, rhs, write=None)
        if write and spill_sz > 0:
            root = root.maybe_write(write, spill_sz)
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
        raise ValueError("MPU state missing") from exc

    for data, chunk_id in chunks:
        mpu = mpu.append(data, chunk_id)  # Update mpu state
        if write is not None and spill_sz > 0:
            mpu = mpu.maybe_write(write, spill_sz)  # Update mpu state

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
    final_task_timeout: int = 1200,
    # pylint: disable=unused-argument
    dask_key_name: Optional[str] = None,
):
    """
    Dask graph node: Final step processing header/footer and submitting remote task.
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
                raise TypeError("footer")
            final_chunk = final_chunk.append(footer_bytes)  # Update reference
        if hdr_bytes:
            if not isinstance(hdr_bytes, (bytes, bytearray)):
                raise TypeError("header")
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
        raise RuntimeError("Dask client not found") from exc
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
            logger.warning("Cancellation requested: %s", future.key)
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
    """Executed remotely to perform final flush/commit."""
    worker_logger = logging.getLogger(__name__)
    worker_logger.info("Executing remote final flush for %s state: %s", type(writer_obj), chunk_state)
    try:
        # Flush returns (final_state, result)
        _final_state, final_result = chunk_state.flush(writer_obj, finalise=True)
        worker_logger.info("Remote final flush COMPLETED. Result: %s", final_result)
        return final_result
    except Exception as worker_e:
        worker_logger.error("Error during remote final flush: %s", worker_e, exc_info=True)
        raise


def get_mpu_kwargs(
    mk_header=None, mk_footer=None, user_kw=None, writes_per_chunk=1, spill_sz=0, client=None  # Default spill_sz=0
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
    spill_sz: int = 0,  # Default spill_sz=0
    client: Any = None,
    **writer_factory_kw: Any,
) -> Delayed:
    """
    High-level function for MPU graph (immutable, corrected logic).
    """
    factory_call_kw = {"kw": writer_factory_kw, "client": client}
    write_instance: Optional[PartsWriter] = None

    # Instantiate writer if writing will actually occur
    if mk_header or mk_footer or chunks:
        try:
            write_instance = writer(**factory_call_kw)
        except TypeError as e:
            raise TypeError(f"Writer factory {writer} incompatible with arguments.") from e
        except Exception as e:
            raise RuntimeError("Writer factory instantiation failed.") from e

    return mpu_write(
        chunks,
        write_instance,
        mk_header=mk_header,
        mk_footer=mk_footer,
        user_kw=user_kw,
        writes_per_chunk=writes_per_chunk,
        spill_sz=spill_sz,  # Pass spill_sz
        dask_name_prefix=dask_name_prefix,
        client=client,
    )
