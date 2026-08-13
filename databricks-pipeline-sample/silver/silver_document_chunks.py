"""Silver Layer: 문서 요소별 텍스트 청킹 (Document Chunking)

입력: silver_documents (Streaming Table)
출력: silver_document_chunks (Streaming Table)
  - document_id: 문서 고유 ID
  - source_file_name: 원본 파일명
  - element_type: 요소 유형 (text, table, figure 등)
  - element_page: 요소가 위치한 페이지 번호
  - element_idx: 원본 요소 인덱스
  - chunk_idx: 요소 내 청크 순번
  - chunk_content: 청크 텍스트 내용
  - chunk_type: 청킹 방식 (overlap / single)
  - chunked_at: 청킹 처리 시각

청킹 전략:
  - CHUNK_SIZE(500자) 이하 요소: 분할 없이 단일 청크
  - CHUNK_SIZE 초과 요소: CHUNK_OVERLAP(100자) 오버랩으로 고정 크기 분할
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import config


@dp.table(
    comment="문서 요소별 텍스트 청킹 (Silver Layer) - RAG 벡터검색용",
    table_properties={"delta.enableChangeDataFeed": "true"},
    schema="""
        document_id STRING NOT NULL,
        source_file_name STRING,
        element_type STRING,
        element_page INT,
        element_idx INT,
        chunk_idx INT,
        chunk_content STRING,
        chunk_type STRING,
        chunked_at TIMESTAMP,
        CONSTRAINT fk_chunks_document FOREIGN KEY (document_id) REFERENCES silver_documents(document_id)
    """,
)
def silver_document_chunks():
    chunk_size = config.CHUNK_SIZE
    chunk_overlap = config.CHUNK_OVERLAP
    step = chunk_size - chunk_overlap  # 슬라이딩 윈도우 이동 단위 (400자)

    df = spark.readStream.table("silver_documents")

    # 1. parsed_content에서 elements 배열 추출 및 posexplode
    df = df.withColumn(
        "_elements",
        F.expr("try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)"),
    )
    df = df.select(
        "document_id",
        "source_file_name",
        F.posexplode("_elements").alias("element_idx", "_element"),
    )

    # 2. 요소 메타데이터 추출
    df = (
        df.withColumn("element_type", F.expr("try_cast(_element:type AS STRING)"))
        .withColumn("element_page", F.expr("try_cast(_element:page AS INT)"))
        .withColumn(
            "_raw_content",
            F.expr(
                "coalesce(try_cast(_element:content AS STRING), "
                "try_cast(_element:description AS STRING))"
            ),
        )
    )

    # 빈 컨텐츠 제거
    df = df.filter(
        F.col("_raw_content").isNotNull()
        & (F.length(F.trim(F.col("_raw_content"))) > 0)
    )

    # 3. HTML 태그 정리 (table 요소 등)
    df = (
        df.withColumn("_raw_content", F.regexp_replace("_raw_content", "</tr>", "\n"))
        .withColumn("_raw_content", F.regexp_replace("_raw_content", "</t[dh]>", " | "))
        .withColumn("_raw_content", F.regexp_replace("_raw_content", "<[^>]+>", ""))
        .withColumn(
            "_raw_content", F.regexp_replace("_raw_content", " \\| *(?=\\n|$)", "")
        )
        .withColumn("_raw_content", F.trim("_raw_content"))
    )

    # 4. 오버랩 청킹: 텍스트 길이에 따라 chunk 시작 위치 배열 생성
    df = df.withColumn("_text_len", F.length("_raw_content"))
    df = df.withColumn(
        "_chunk_starts",
        F.expr(f"sequence(0, greatest(_text_len - 1, 0), {step})"),
    )

    # 5. explode로 요소당 다수의 청크 행 생성
    df = df.select(
        "document_id",
        "source_file_name",
        "element_type",
        "element_page",
        "element_idx",
        "_raw_content",
        "_text_len",
        F.posexplode("_chunk_starts").alias("chunk_idx", "_start"),
    )

    # 6. substr로 청크 텍스트 추출 (SQL substr은 1-indexed)
    df = df.withColumn(
        "chunk_content",
        F.trim(F.substr(F.col("_raw_content"), F.col("_start") + 1, F.lit(chunk_size))),
    )

    # 빈 청크 제거
    df = df.filter(F.length("chunk_content") > 0)

    # 7. 메타데이터 추가
    df = df.withColumn(
        "chunk_type",
        F.when(F.col("_text_len") <= chunk_size, F.lit("single")).otherwise(
            F.lit("overlap")
        ),
    )
    df = df.withColumn("chunked_at", F.current_timestamp())

    return df.select(
        "document_id",
        "source_file_name",
        "element_type",
        "element_page",
        "element_idx",
        "chunk_idx",
        "chunk_content",
        "chunk_type",
        "chunked_at",
    )
