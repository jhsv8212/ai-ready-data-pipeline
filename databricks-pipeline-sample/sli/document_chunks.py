from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

# =============================================================================
# Silver Layer: 문서 파싱 + 오버랩 청킹 (Overlap Chunking)
# =============================================================================
# 1. ai_parse_document()로 PDF/문서에서 텍스트 추출
# 2. 커스텀 UDF로 오버랩 청킹 수행
# =============================================================================

# 청킹 설정
CHUNK_SIZE = 1000  # 각 청크의 문자 수
CHUNK_OVERLAP = 200  # 청크 간 겹치는 문자 수


@F.udf(
    returnType=ArrayType(
        StructType(
            [
                StructField("chunk_index", IntegerType(), False),
                StructField("chunk_text", StringType(), False),
                StructField("start_offset", IntegerType(), False),
                StructField("end_offset", IntegerType(), False),
            ]
        )
    )
)
def overlap_chunk(text, chunk_size, chunk_overlap):
    """오버랩 청킹: 지정된 크기와 겹침으로 텍스트를 분할합니다."""
    if text is None or len(text.strip()) == 0:
        return []

    chunks = []
    start = 0
    idx = 0

    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunk_text = text[start:end]

        if chunk_text.strip():
            chunks.append(
                {
                    "chunk_index": idx,
                    "chunk_text": chunk_text,
                    "start_offset": start,
                    "end_offset": end,
                }
            )
            idx += 1

        start += chunk_size - chunk_overlap

    return chunks


@dp.materialized_view(
    name="parsed_documents",
    comment="Silver: 마크다운 문서에서 추출된 텍스트",
    private=True,
)
def parsed_documents():
    return (
        spark.read.table("raw_documents")
        .withColumn(
            "full_text",
            F.col("content").cast("STRING"),
        )
        .select(
            F.col("path").alias("source_path"),
            F.element_at(F.split(F.col("path"), "/"), -1).alias("file_name"),
            F.col("modificationTime").alias("file_modified_at"),
            "full_text",
        )
        .filter(F.col("full_text").isNotNull() & (F.length(F.col("full_text")) > 0))
    )


@dp.materialized_view(
    name="document_chunks",
    comment="Silver: 오버랩 청킹이 적용된 문서 청크 (chunk_size=1000, overlap=200)",
)
def document_chunks():
    return (
        spark.read.table("parsed_documents")
        .withColumn(
            "chunks",
            overlap_chunk(
                F.col("full_text"),
                F.lit(CHUNK_SIZE),
                F.lit(CHUNK_OVERLAP),
            ),
        )
        .select(
            "source_path",
            "file_name",
            "file_modified_at",
            F.explode("chunks").alias("chunk"),
        )
        .select(
            "source_path",
            "file_name",
            "file_modified_at",
            F.col("chunk.chunk_index").alias("chunk_index"),
            F.col("chunk.chunk_text").alias("chunk_text"),
            F.col("chunk.start_offset").alias("start_offset"),
            F.col("chunk.end_offset").alias("end_offset"),
            F.expr(
                "md5(concat(source_path, '::', chunk.chunk_index))"
            ).alias("chunk_id"),
        )
    )
