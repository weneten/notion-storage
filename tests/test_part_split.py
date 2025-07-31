import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import hashlib
import types

from uploader.streaming_uploader import NotionStreamingUploader


def byte_generator(data: bytes, chunk_size: int):
    """Yield data in chunks of ``chunk_size`` bytes."""
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def collect_stream(stream: types.GeneratorType):
    return b"".join(chunk for chunk in stream)


def test_part_stream_exact_size():
    data = b"0123456789abcdef"  # 16 bytes
    gen = byte_generator(data, 4)

    part = NotionStreamingUploader._PartStream(gen, 10, b"", None)
    collected = collect_stream(part)

    remaining = b"".join(gen)
    leftover = part.get_leftover() + remaining

    assert collected == data[:10]
    assert leftover == data[10:]
    assert part.get_bytes_sent() == 10
    assert part.get_part_hash() == hashlib.sha512(data[:10]).hexdigest()


def test_part_stream_leftover_across_parts():
    data = b"abcdefghijklmnopqrst"  # 20 bytes
    gen = byte_generator(data, 3)

    part1 = NotionStreamingUploader._PartStream(gen, 8, b"", None)
    bytes1 = collect_stream(part1)
    leftover1 = part1.get_leftover()

    part2 = NotionStreamingUploader._PartStream(gen, 8, leftover1, None)
    bytes2 = collect_stream(part2)
    leftover2 = part2.get_leftover()

    remaining = b"".join(gen)
    reconstructed = bytes1 + bytes2 + leftover2 + remaining
    assert reconstructed == data

