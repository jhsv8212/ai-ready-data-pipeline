"""Gold Layer: 문서 요소 타입별 청킹 전략을 적용합니다.

입력: silver_documents (Materialized View)
출력: gold_document_chunks (Materialized View)
  - element_type별 차별화된 청킹:
    - text: ai_query 시멘틱 청킹 (의미 단위 분할)
    - table: 앞뒤 문맥(title, 페이지) 포함한 청크
    - figure: title + description + image_ref 구조화 JSON 청크
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import config

# 청킹 설정값은 config.py에서 관리
CHUNKING_LLM_MODEL = config.CHUNKING_LLM_MODEL
CHUNKING_INPUT_MAX_CHARS = config.CHUNKING_INPUT_MAX_CHARS
CHUNKING_MAX_TOKENS = config.CHUNKING_MAX_TOKENS
CHUNKING_PROMPT = config.CHUNKING_PROMPT


@dp.materialized_view(
    comment="문서 요소 타입별 청킹 전략 적용 (Gold Layer) - 텍스트/표/이미지 분리 처리"
)
def gold_document_chunks():
    # silver_documents에서 elements 배열을 explode하여 개별 요소로 전개
    elements_df = (
        spark.read.table("silver_documents")
        .select(
            "document_id",
            "source_file_name",
            F.posexplode(
                F.expr("try_cast(parsed_content:document:elements AS ARRAY<VARIANT>)")
            ).alias("element_idx", "element"),
        )
        .withColumn("element_type", F.expr("try_cast(element:type AS STRING)"))
        .withColumn("element_content", F.expr("try_cast(element:content AS STRING)"))
        .withColumn("element_title", F.expr("try_cast(element:title AS STRING)"))
        .withColumn("element_description", F.expr("try_cast(element:description AS STRING)"))
        .withColumn("element_page", F.expr("try_cast(element:page AS INT)"))
        # 빈 콘텐츠 제외
        .filter(
            F.col("element_content").isNotNull()
            & (F.length(F.trim("element_content")) > 0)
        )
    )

    # =========================================================================
    # 1. 텍스트 요소: ai_query 시멘틱 청킹 (의미 단위 분할)
    # =========================================================================
    text_chunks = (
        elements_df
        .filter(
            (F.col("element_type") != "figure")
            & (F.col("element_type") != "table")
        )
        .withColumn(
            "chunk_content",
            F.expr(f"""
                ai_query(
                    '{CHUNKING_LLM_MODEL}',
                    '{CHUNKING_PROMPT}' || substr(element_content, 1, {CHUNKING_INPUT_MAX_CHARS}),
                    modelParameters => named_struct('max_tokens', {CHUNKING_MAX_TOKENS})
                )
            """),
        )
        .withColumn("chunk_type", F.lit("semantic"))
        .select(
            "document_id",
            "source_file_name",
            F.lit("text").alias("element_type"),
            "element_page",
            "element_idx",
            "chunk_content",
            "chunk_type",
        )
    )

    # =========================================================================
    # 2. 표 요소: 앞뒤 문맥(title, 페이지 번호) 포함 청크
    # =========================================================================
    table_chunks = (
        elements_df
        .filter(F.col("element_type") == "table")
        # HTML 태그를 | 구분 텍스트로 변환
        .withColumn("table_text", F.regexp_replace("element_content", "</tr>", "\n"))
        .withColumn("table_text", F.regexp_replace("table_text", "</t[dh]>", " | "))
        .withColumn("table_text", F.regexp_replace("table_text", "<[^>]+>", ""))
        .withColumn("table_text", F.regexp_replace("table_text", " \\| *(?=\\n|$)", ""))
        .withColumn("table_text", F.trim("table_text"))
        # 앞뒤 문맥을 포함한 청크 생성
        .withColumn(
            "chunk_content",
            F.concat(
                F.lit("[표 제목] "),
                F.coalesce(F.col("element_title"), F.lit("제목 없음")),
                F.lit("\n[페이지] "),
                F.coalesce(F.col("element_page").cast("string"), F.lit("N/A")),
                F.lit("\n[표 내용]\n"),
                F.col("table_text"),
            ),
        )
        .withColumn("chunk_type", F.lit("table_with_context"))
        .select(
            "document_id",
            "source_file_name",
            F.lit("table").alias("element_type"),
            "element_page",
            "element_idx",
            "chunk_content",
            "chunk_type",
        )
    )

    # =========================================================================
    # 3. 이미지 요소: title + description + image_ref 구조화 JSON
    # =========================================================================
    figure_chunks = (
        elements_df
        .filter(F.col("element_type") == "figure")
        .withColumn(
            "chunk_content",
            F.to_json(
                F.struct(
                    F.coalesce(F.col("element_title"), F.lit("")).alias("title"),
                    F.coalesce(
                        F.col("element_description"),
                        F.col("element_content"),
                        F.lit(""),
                    ).alias("description"),
                    F.concat(
                        F.lit("page_"),
                        F.coalesce(F.col("element_page").cast("string"), F.lit("unknown")),
                        F.lit("_figure_"),
                        F.col("element_idx").cast("string"),
                    ).alias("image_ref"),
                )
            ),
        )
        .withColumn("chunk_type", F.lit("structured_figure"))
        .select(
            "document_id",
            "source_file_name",
            F.lit("figure").alias("element_type"),
            "element_page",
            "element_idx",
            "chunk_content",
            "chunk_type",
        )
    )

    # =========================================================================
    # 세 타입 결과를 하나로 합침
    # =========================================================================
    return (
        text_chunks
        .unionByName(table_chunks)
        .unionByName(figure_chunks)
        .withColumn("chunked_at", F.current_timestamp())
    )
