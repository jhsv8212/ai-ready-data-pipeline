"""Gold Layer: 문서별 AI 요약 및 메타데이터를 생성합니다.

입력: {카테고리명}_silver_documents (Streaming Table)
출력: {카테고리명}_gold_document_ai_summary (Materialized View, 카테고리별로 동적 생성)
  - config.get_category_list()로 스캔한 카테고리마다 별도 테이블을 생성한다.
  - extraction_method: 추출 방식 (예: "LLM 추출 - 청크 본문 기반")
  - summary: LLM이 생성한 한국어 요약 (3~5문장)
  - keywords: AI 추출 키워드 태그 목록 (ARRAY<STRING>)
  - metadata: JSON 형태 메타데이터 (VARIANT)
    - doc_category, insurance_product_type, related_systems,
      sensitivity_level, regulation_reference, model_name,
      model_version, confidence_score, pipeline_run_id
  - generated_at: 요약 생성 시각

요약 방식:
  - ai_query: Databricks 내장 ai_query() 함수 사용 (현재 활성)
  - openai: 외부 OpenAI API 호출 (추후 API 키 발급 후 사용 예정)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Column

import config


# =============================================================================
# 요약 방식 1: Databricks ai_query() (현재 활성)
# =============================================================================


def summarize_with_ai_query() -> Column:
    """
    Databricks 내장 ai_query() SQL 함수를 사용하여 문서를 요약합니다.

    - 모델: config.LLM_MODEL_NAME
    - 입력: full_text 앞 2500자 + figure descriptions 앞 500자
    - 출력: 한국어 3~5문장 요약

    Returns:
        Column: ai_summary 컨텐츠가 담긴 Spark Column
    """
    return F.expr(f"""
        ai_query(
            '{config.LLM_MODEL_NAME}',
            '다음 보험 문서를 한국어로 3~5문장으로 요약하세요. 핵심 보장 내용, 주요 조건, 대상 독자를 포함하세요. 그림/차트 설명이 있으면 해당 내용도 반영하세요.\n\n'
                || '[문서 텍스트]\n' || substr(full_text, 1, {config.AI_INPUT_MAX_CHARS})
                || CASE WHEN concat_ws(chr(10),
                       transform(
                           filter(
                               try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
                               el -> try_cast(el:type AS STRING) = 'figure'
                                     AND coalesce(
                                         try_cast(el:description AS STRING),
                                         try_cast(el:content AS STRING)
                                     ) IS NOT NULL
                           ),
                           el -> coalesce(
                               try_cast(el:description AS STRING),
                               try_cast(el:content AS STRING)
                           )
                       )
                   ) != ''
                       THEN '\n\n[그림/차트 설명]\n' || substr(
                           concat_ws(chr(10),
                               transform(
                                   filter(
                                       try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
                                       el -> try_cast(el:type AS STRING) = 'figure'
                                             AND coalesce(
                                                 try_cast(el:description AS STRING),
                                                 try_cast(el:content AS STRING)
                                             ) IS NOT NULL
                                   ),
                                   el -> coalesce(
                                       try_cast(el:description AS STRING),
                                       try_cast(el:content AS STRING)
                                   )
                               )
                           ), 1, 500)
                       ELSE '' END,
            modelParameters => named_struct('max_tokens', {config.AI_MAX_TOKENS})
        )
    """)


# =============================================================================
# 요약 방식 2: OpenAI 외부 API (추후 API 키 발급 후 활성화 예정)
# =============================================================================

# --- 아래 코드는 OpenAI API 키 발급 후 주석을 해제하여 사용합니다. ---
# --- 사용 시 config.py에 OPENAI_API_KEY, OPENAI_MODEL_NAME 설정 필요 ---
#
# import pandas as pd
# from pyspark.sql.functions import pandas_udf
# from pyspark.sql.types import StringType
# import openai  # pip install openai
#
#
# @pandas_udf(StringType())
# def summarize_with_openai_udf(texts: pd.Series) -> pd.Series:
#     """
#     OpenAI API를 호출하여 문서를 요약합니다.
#
#     pandas_udf를 사용하여 배치 단위로 API를 호출합니다.
#     Databricks Secrets에서 API 키를 관리하는 것을 권장합니다.
#
#     Args:
#         texts: 요약할 문서 텍스트 Series
#
#     Returns:
#         pd.Series: 요약 결과 Series
#     """
#     api_key = spark.conf.get("openai_api_key", "")
#     client = openai.OpenAI(api_key=api_key)
#
#     results = []
#     for text in texts:
#         if text is None or len(text.strip()) == 0:
#             results.append(None)
#             continue
#         try:
#             response = client.chat.completions.create(
#                 model=config.OPENAI_MODEL_NAME,
#                 messages=[
#                     {
#                         "role": "system",
#                         "content": (
#                             "당신은 보험 문서 요약 전문가입니다. "
#                             "한국어로 3~5문장으로 요약하세요. "
#                             "핵심 보장 내용, 주요 조건, 대상 독자를 포함하세요."
#                         ),
#                     },
#                     {
#                         "role": "user",
#                         "content": text[:config.AI_INPUT_MAX_CHARS],
#                     },
#                 ],
#                 max_tokens=config.OPENAI_MAX_TOKENS,
#                 temperature=0.3,
#             )
#             results.append(response.choices[0].message.content)
#         except Exception as e:
#             results.append(f"[OpenAI 요약 실패: {str(e)}]")
#
#     return pd.Series(results)
#
#
# def summarize_with_openai(df):
#     """
#     OpenAI API 기반 요약 컨텐츠를 DataFrame에 추가합니다.
#
#     Args:
#         df: silver_documents DataFrame (full_text, figure_descriptions 컬럼 필요)
#
#     Returns:
#         Column: ai_summary 컨텐츠가 담긴 Spark Column
#     """
#     combined_text = F.concat(
#         F.substring("full_text", 1, config.AI_INPUT_MAX_CHARS),
#         F.when(
#             F.col("figure_descriptions").isNotNull() & (F.length("figure_descriptions") > 0),
#             F.concat(F.lit("\n\n[그림/차트 설명]\n"), F.substring("figure_descriptions", 1, 500)),
#         ).otherwise(F.lit("")),
#     )
#     return summarize_with_openai_udf(combined_text)


# =============================================================================
# 공통 유틸: AI 응답 코드펜스 제거
# =============================================================================


def _strip_code_fence(col: Column) -> Column:
    """AI 응답 Column에서 마크다운 코드펜스(```)를 제거합니다."""
    fence_open = r'^\s*\x60{3}[a-z]*\s*'
    fence_close = r'\x60{3}\s*\Z'
    return F.regexp_replace(
        F.regexp_replace(col, fence_open, ''),
        fence_close, ''
    )


# =============================================================================
# 파이프라인 정의
# =============================================================================


def _generate_gold_document_ai_summary(category: str):
    """카테고리 하나에 대한 {category}_gold_document_ai_summary 테이블을 정의한다."""

    @dp.table(
        name=f"dev_haesung.gold.{category}_gold_document_ai_summary",
        comment=f"'{category}' 카테고리 문서별 AI 요약 및 메타데이터 (Gold Layer) - 생명보험 도메인",
        schema=f"""
            document_id STRING NOT NULL,
            source_file_name STRING,
            extraction_method STRING,
            summary STRING,
            keywords ARRAY<STRING>,
            metadata VARIANT,
            generated_at TIMESTAMP,
            CONSTRAINT `fk_{category}_summary_document` FOREIGN KEY (document_id) REFERENCES dev_haesung.silver.{category}_silver_documents(document_id)
        """,
    )
    def gold_document_ai_summary():
        df = spark.readStream.table(f"dev_haesung.silver.{category}_silver_documents")

        # --- AI 요약 ---
        df = df.withColumn("summary", summarize_with_ai_query())

        # --- 키워드 추출 (AI 응답 코드펜스 제거 후 파싱) ---
        df = df.withColumn(
            "_raw_keywords",
            F.expr(f"""
                ai_query(
                    '{config.LLM_MODEL_NAME}',
                    '다음 보험 문서에서 핵심 키워드를 10~20개 추출하세요. '
                    || '보험 상품명, 보장 내용, 관련 시스템, 규정, 업무 프로세스 등을 포함하세요. '
                    || 'JSON 배열 형식으로만 응답하세요. 예: ["종신보험", "언더라이팅", ...]\n\n'
                    || substr(full_text, 1, {config.AI_INPUT_MAX_CHARS}),
                    modelParameters => named_struct('max_tokens', 200)
                )
            """),
        )
        df = df.withColumn(
            "keywords",
            F.from_json(_strip_code_fence(F.col("_raw_keywords")), "ARRAY<STRING>"),
        )

        # --- 메타데이터 추출 (AI 응답 코드펜스 제거 후 파싱) ---
        df = df.withColumn(
            "_raw_metadata",
            F.expr(f"""
                ai_query(
                    '{config.LLM_MODEL_NAME}',
                    '다음 보험 문서를 분석하여 메타데이터를 JSON으로 추출하세요. '
                    || '반드시 아래 필드를 포함하고, JSON만 응답하세요:\n'
                    || '- doc_category: 문서 유형 (약관/사업방법서/상품설명서/언더라이팅가이드/보험금지급기준/고객안내문 중 택1)\n'
                    || '- insurance_product_type: 보험 상품 유형 (종신/정기/CI/변액/연금/건강/실손/해당없음 중 택1)\n'
                    || '- related_systems: 관련 시스템 배열 (보험코어/언더라이팅/보상/CRM/전자청약/계리 중 해당하는 것)\n'
                    || '- sensitivity_level: 민감도 (공개/내부/민감/개인정보포함/의료정보포함 중 택1)\n'
                    || '- regulation_reference: 관련 규정 (보험업법 조항, 감독규정 등. 없으면 null)\n\n'
                    || substr(full_text, 1, {config.AI_INPUT_MAX_CHARS}),
                    modelParameters => named_struct('max_tokens', 300)
                )
            """),
        )
        df = df.withColumn("_metadata_cleaned", _strip_code_fence(F.col("_raw_metadata")))
        df = df.withColumn("metadata", F.expr("parse_json(_metadata_cleaned)"))

        return df.select(
            "document_id",
            "source_file_name",
            F.lit("LLM 추출 (청크 본문 기반)").alias("extraction_method"),
            "summary",
            "keywords",
            "metadata",
            F.current_timestamp().alias("generated_at"),
        )

    return gold_document_ai_summary


for _category in config.get_category_list():
    _generate_gold_document_ai_summary(_category)
