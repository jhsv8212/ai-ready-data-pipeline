"""Silver Layer: bronze_documents(MD 파일)에서 텍스트를 추출·정제합니다.

입력: {카테고리명}_bronze_documents (Streaming Table)
출력: {카테고리명}_silver_documents (Streaming Table, 카테고리별로 동적 생성)
  - config.get_category_list()로 스캔한 카테고리마다 별도 테이블을 생성한다.
  - full_text: MD 파일 원문 텍스트 (content를 STRING으로 디코딩)
  - figure_descriptions / page_count / parsed_content: MD 전환으로 비활성 (항상 NULL)

참고: 원래 PDF + ai_parse_document() 기반으로 개발되었으나, 현재는 MD 파일이 업로드되는 것을
전제로 ai_parse_document() 호출 없이 content를 바로 텍스트로 사용합니다. 추후 PDF로 다시
바뀔 수 있어 기존 로직은 삭제하지 않고 주석 처리로 남겨두었습니다.
"""
import sys
sys.path.insert(0, "/Workspace/Shared/rag_document_processing_pipeline_f237c77c/transformations")

from pyspark import pipelines as dp
from pyspark.sql import functions as F
import config


def _generate_silver_documents(category: str):
    """카테고리 하나에 대한 {category}_silver_documents 테이블을 정의한다."""

    @dp.table(
        name=f"silver.`{category}_silver_documents`",
        comment=f"'{category}' 카테고리 MD 파일 텍스트를 추출·정제한 데이터 (Silver Layer) - ai_parse_document() 미사용",
        schema=f"""
            document_id STRING NOT NULL,
            source_file_name STRING,
            source_file STRING,
            full_text STRING,
            figure_descriptions STRING,
            page_count INT,
            parsed_content VARIANT,
            file_size_bytes BIGINT,
            file_modified_at TIMESTAMP,
            ingested_at TIMESTAMP,
            processed_at TIMESTAMP,
            silver_layer STRING,
            CONSTRAINT `pk_{category}_silver_documents` PRIMARY KEY (document_id)
        """,
    )
    def silver_documents():
        # --- 기존 PDF + ai_parse_document() 기반 파싱 - MD 파일 전환으로 비활성화 ---
        # return (
        #     spark.readStream.table(f"bronze.{category}_bronze_documents")
        #     # ai_parse_document v2: PDF 바이너리를 구조화된 VARIANT로 출력
        #     # 반환값에는 pages, elements(텍스트/테이블/그림), metadata 등이 포함됨
        #     .withColumn(
        #         "parsed_content",
        #         F.expr(f"ai_parse_document(content, map('version', '{config.AI_PARSE_DOCUMENT_VERSION}'))"),
        #     )
        #     # 문서 내 텍스트 요소만 추출 (figure 타입 제외하여 깨끗한 평문 생성)
        #     .withColumn(
        #         "full_text",
        #         F.expr("""
        #             concat_ws(chr(10) || chr(10),
        #                 transform(
        #                     filter(
        #                         try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
        #                         element -> try_cast(element:type AS STRING) != 'figure'
        #                                    AND try_cast(element:content AS STRING) IS NOT NULL
        #                                    AND length(trim(try_cast(element:content AS STRING))) > 0
        #                     ),
        #                     element -> try_cast(element:content AS STRING)
        #                 )
        #             )
        #         """),
        #     )
        #     # figure 타입 요소의 AI 생성 설명 추출 (이미지/차트 내용 파악)
        #     # descriptionElementTypes 기본값 '*'에 의해 자동 생성됨
        #     .withColumn(
        #         "figure_descriptions",
        #         F.expr("""
        #             concat_ws(chr(10) || chr(10),
        #                 transform(
        #                     filter(
        #                         try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
        #                         element -> try_cast(element:type AS STRING) = 'figure'
        #                                    AND (
        #                                        (try_cast(element:description AS STRING) IS NOT NULL
        #                                         AND length(trim(try_cast(element:description AS STRING))) > 0)
        #                                        OR
        #                                        (try_cast(element:content AS STRING) IS NOT NULL
        #                                         AND length(trim(try_cast(element:content AS STRING))) > 0)
        #                                    )
        #                     ),
        #                     element -> coalesce(
        #                         try_cast(element:description AS STRING),
        #                         try_cast(element:content AS STRING)
        #                     )
        #                 )
        #             )
        #         """),
        #     )
        #     # HTML 테이블 태그를 | 구분 텍스트로 변환 (병합셀 구조 보존)
        #     # ai_parse_document가 table 요소를 HTML(rowspan/colspan 포함)로 반환하므로
        #     # 태그를 제거하면서 행/열 구조를 읽기 쉬운 텍스트로 변환
        #     .withColumn("full_text", F.regexp_replace("full_text", "</tr>", "\n"))
        #     .withColumn("full_text", F.regexp_replace("full_text", "</t[dh]>", " | "))
        #     .withColumn("full_text", F.regexp_replace("full_text", "<[^>]+>", ""))
        #     .withColumn("full_text", F.regexp_replace("full_text", " \\| *(?=\\n|$)", ""))
        #     # 연속 공백/빈 행 정리
        #     .withColumn("full_text", F.regexp_replace("full_text", "\\n{3,}", "\n\n"))
        #     .withColumn("full_text", F.trim("full_text"))
        #     # 페이지 배열 크기로 총 페이지 수 산출
        #     .withColumn(
        #         "page_count",
        #         F.expr("size(try_cast(parsed_content:document:pages AS ARRAY<VARIANT>))"),
        #     )
        #     # 파싱 실패 여부 확인 (null이면 정상)
        #     .withColumn("error_status", F.expr("try_cast(parsed_content:error_status AS STRING)"))
        #     .withColumn("processed_at", F.current_timestamp())
        #     .withColumn("silver_layer", F.lit("silver"))
        #     # 파싱 오류가 발생한 문서 제외
        #     .filter(F.col("error_status").isNull())
        #     .select(
        #         "document_id",
        #         "source_file_name",
        #         "source_file",
        #         "full_text",              # 텍스트 (figure 제외, Gold에서 ai_query등에 활용)
        #         "figure_descriptions",    # 그림/차트 AI 설명 (이미지 내용 요약)
        #         "page_count",
        #         "parsed_content",         # 원본 VARIANT (ai_extract 등 추가 분석에 활용 가능)
        #         "file_size_bytes",
        #         "file_modified_at",
        #         "ingested_at",
        #         "processed_at",
        #         "silver_layer",
        #     )
        # )
        # --- 여기까지 ---

        # MD 파일: ai_parse_document() 없이 content를 바로 텍스트로 디코딩
        return (
            spark.readStream.table(f"bronze.`{category}_bronze_documents`")
            .withColumn("full_text", F.col("content").cast("STRING"))
            # 연속 공백/빈 행 정리
            .withColumn("full_text", F.regexp_replace("full_text", "\\n{3,}", "\n\n"))
            .withColumn("full_text", F.trim("full_text"))
            # PDF 전용 필드 - MD에는 해당 없음 (스키마 호환을 위해 NULL 유지)
            .withColumn("figure_descriptions", F.lit(None).cast("string"))
            .withColumn("page_count", F.lit(None).cast("int"))
            .withColumn("parsed_content", F.expr("CAST(NULL AS VARIANT)"))
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("silver_layer", F.lit("silver"))
            # 빈 텍스트 문서 제외
            .filter(F.col("full_text").isNotNull() & (F.length(F.col("full_text")) > 0))
            .select(
                "document_id",
                "source_file_name",
                "source_file",
                "full_text",
                "figure_descriptions",
                "page_count",
                "parsed_content",
                "file_size_bytes",
                "file_modified_at",
                "ingested_at",
                "processed_at",
                "silver_layer",
            )
        )

    return silver_documents


for _category in config.get_category_list():
    _generate_silver_documents(_category)
