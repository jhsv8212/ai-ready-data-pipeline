"""Silver Layer: 텍스트 청킹 (Document Chunking)

입력: {카테고리명}_silver_documents (Streaming Table)
출력: {카테고리명}_silver_document_chunks (Streaming Table, 카테고리별로 동적 생성)
  - config.get_category_list()로 스캔한 카테고리마다 별도 테이블을 생성한다.
  - document_id: 문서 고유 ID
  - source_file_name: 원본 파일명
  - element_type: 요소 유형 (MD 전환 후 항상 "text" - PDF 복귀 시 table/figure 등 다양화)
  - element_page: 요소가 위치한 페이지 번호 (MD 전환 후 항상 NULL)
  - element_idx: 원본 요소 인덱스 (MD 전환 후 항상 0 - 문서 전체를 단일 요소로 취급)
  - chunk_idx: 요소 내 청크 순번
  - chunk_content: 청크 텍스트 내용
  - chunk_type: 청킹 방식 (overlap / single)
  - chunked_at: 청킹 처리 시각

청킹 전략:
  - CHUNK_SIZE(500자) 이하: 분할 없이 단일 청크
  - CHUNK_SIZE 초과: CHUNK_OVERLAP(100자) 오버랩으로 고정 크기 분할 (overlap_chunk UDF)

참고: 원래 ai_parse_document() 결과의 요소(text/table/figure)별로 청킹했으나, MD 파일
전환으로 요소 구분이 사라져 문서 전체(full_text)를 단일 요소로 청킹합니다. 기존 요소 추출
로직은 삭제하지 않고 주석 처리로 남겨두었습니다.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

import config


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


def _generate_silver_document_chunks(category: str):
    """카테고리 하나에 대한 {category}_silver_document_chunks 테이블을 정의한다."""

    @dp.table(
        name=f"dev_haesung.silver.{category}_silver_document_chunks",
        comment=f"'{category}' 카테고리 문서 텍스트 오버랩 청킹 (Silver Layer) - RAG 벡터검색용, overlap_chunk UDF 사용",
        table_properties={"delta.enableChangeDataFeed": "true"},
        schema=f"""
            document_id STRING NOT NULL,
            source_file_name STRING,
            element_type STRING,
            element_page INT,
            element_idx INT,
            chunk_idx INT,
            chunk_content STRING,
            chunk_type STRING,
            chunked_at TIMESTAMP,
            CONSTRAINT `fk_{category}_chunks_document` FOREIGN KEY (document_id) REFERENCES dev_haesung.silver.{category}_silver_documents(document_id)
        """,
    )
    def silver_document_chunks():
        chunk_size = config.CHUNK_SIZE
        chunk_overlap = config.CHUNK_OVERLAP

        df = spark.readStream.table(f"dev_haesung.silver.{category}_silver_documents")

        # --- 기존 ai_parse_document elements 기반 요소 추출 (PDF 전용) - MD 전환으로 비활성화 ---
        # # 1. parsed_content에서 elements 배열 추출 및 posexplode
        # df = df.withColumn(
        #     "_elements",
        #     F.expr("try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)"),
        # )
        # df = df.select(
        #     "document_id",
        #     "source_file_name",
        #     F.posexplode("_elements").alias("element_idx", "_element"),
        # )
        #
        # # 2. 요소 메타데이터 추출
        # df = (
        #     df.withColumn("element_type", F.expr("try_cast(_element:type AS STRING)"))
        #     .withColumn("element_page", F.expr("try_cast(_element:page AS INT)"))
        #     .withColumn(
        #         "_raw_content",
        #         F.expr(
        #             "coalesce(try_cast(_element:content AS STRING), "
        #             "try_cast(_element:description AS STRING))"
        #         ),
        #     )
        # )
        #
        # # 빈 컨텐츠 제거
        # df = df.filter(
        #     F.col("_raw_content").isNotNull()
        #     & (F.length(F.trim(F.col("_raw_content"))) > 0)
        # )
        #
        # # 3. HTML 태그 정리 (table 요소 등)
        # df = (
        #     df.withColumn("_raw_content", F.regexp_replace("_raw_content", "</tr>", "\n"))
        #     .withColumn("_raw_content", F.regexp_replace("_raw_content", "</t[dh]>", " | "))
        #     .withColumn("_raw_content", F.regexp_replace("_raw_content", "<[^>]+>", ""))
        #     .withColumn(
        #         "_raw_content", F.regexp_replace("_raw_content", " \\| *(?=\\n|$)", "")
        #     )
        #     .withColumn("_raw_content", F.trim("_raw_content"))
        # )
        # --- 여기까지 ---

        # 1. MD 파일은 ai_parse_document 요소 구분이 없으므로, 문서 전체(full_text)를
        #    단일 요소로 취급 (element_type="text", element_page=NULL, element_idx=0)
        df = df.select(
            "document_id",
            "source_file_name",
            F.lit("text").alias("element_type"),
            F.lit(None).cast("int").alias("element_page"),
            F.lit(0).alias("element_idx"),
            F.col("full_text").alias("_raw_content"),
        )
        df = df.filter(
            F.col("_raw_content").isNotNull()
            & (F.length(F.trim(F.col("_raw_content"))) > 0)
        )

        # --- 기존 SQL 기반 청킹 로직 (sequence + substr) - overlap_chunk UDF로 대체 ---
        # # 4. 오버랩 청킹: 텍스트 길이에 따라 chunk 시작 위치 배열 생성
        # df = df.withColumn("_text_len", F.length("_raw_content"))
        # df = df.withColumn(
        #     "_chunk_starts",
        #     F.expr(f"sequence(0, greatest(_text_len - 1, 0), {step})"),
        # )
        #
        # # 5. explode로 요소당 다수의 청크 행 생성
        # df = df.select(
        #     "document_id",
        #     "source_file_name",
        #     "element_type",
        #     "element_page",
        #     "element_idx",
        #     "_raw_content",
        #     "_text_len",
        #     F.posexplode("_chunk_starts").alias("chunk_idx", "_start"),
        # )
        #
        # # 6. substr로 청크 텍스트 추출 (SQL substr은 1-indexed)
        # df = df.withColumn(
        #     "chunk_content",
        #     F.trim(F.substr(F.col("_raw_content"), F.col("_start") + 1, F.lit(chunk_size))),
        # )
        #
        # # 빈 청크 제거
        # df = df.filter(F.length("chunk_content") > 0)
        #
        # # 7. 메타데이터 추가
        # df = df.withColumn(
        #     "chunk_type",
        #     F.when(F.col("_text_len") <= chunk_size, F.lit("single")).otherwise(
        #         F.lit("overlap")
        #     ),
        # )
        # df = df.withColumn("chunked_at", F.current_timestamp())
        # --- 여기까지 ---

        # 4. 오버랩 청킹 (overlap_chunk UDF, config.CHUNK_SIZE / config.CHUNK_OVERLAP 사용)
        df = df.withColumn("_text_len", F.length("_raw_content"))
        df = df.withColumn(
            "_chunks",
            overlap_chunk(
                F.col("_raw_content"),
                F.lit(chunk_size),
                F.lit(chunk_overlap),
            ),
        )

        # 5. explode로 요소당 다수의 청크 행 생성
        df = df.select(
            "document_id",
            "source_file_name",
            "element_type",
            "element_page",
            "element_idx",
            "_text_len",
            F.explode("_chunks").alias("_chunk"),
        )
        df = df.withColumn("chunk_idx", F.col("_chunk.chunk_index"))
        df = df.withColumn("chunk_content", F.trim(F.col("_chunk.chunk_text")))

        # 빈 청크 제거
        df = df.filter(F.length("chunk_content") > 0)

        # 6. 메타데이터 추가
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

    return silver_document_chunks


for _category in config.get_category_list():
    _generate_silver_document_chunks(_category)
