"""Gold Layer: 문서별 AI 요약을 생성합니다.

입력: silver_documents (Materialized View)
출력: gold_document_ai_summary (Materialized View)
  - ai_summary: LLM이 생성한 한국어 요약 (3~5문장)

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
#     # Databricks Secrets에서 API 키를 가져옵니다.
#     # config.py에 아래 항목 추가 필요:
#     #   OPENAI_API_KEY_SCOPE = "your-scope"
#     #   OPENAI_API_KEY_KEY = "openai-api-key"
#     #   OPENAI_MODEL_NAME = "gpt-4o"
#     #   OPENAI_MAX_TOKENS = 300
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
#     full_text와 figure_descriptions를 결합하여 LLM에 전달합니다.
#
#     Args:
#         df: silver_documents DataFrame (full_text, figure_descriptions 컨럼 필요)
#
#     Returns:
#         Column: ai_summary 컨텐츠가 담긴 Spark Column
#     """
#     # full_text와 figure_descriptions를 결합하여 입력 텍스트 생성
#     combined_text = F.concat(
#         F.substring("full_text", 1, config.AI_INPUT_MAX_CHARS),
#         F.when(
#             F.col("figure_descriptions").isNotNull() & (F.length("figure_descriptions") > 0),
#             F.concat(F.lit("\n\n[그림/차트 설명]\n"), F.substring("figure_descriptions", 1, 500)),
#         ).otherwise(F.lit("")),
#     )
#     return summarize_with_openai_udf(combined_text)


# =============================================================================
# 파이프라인 정의
# =============================================================================


@dp.materialized_view(comment="문서별 AI 요약 (Gold Layer)")
def gold_document_ai_summary():
    return (
        spark.read.table("silver_documents")
        .select(
            "document_id",
            "source_file_name",
            "page_count",
            F.date_format("file_modified_at", "yyyy-MM-dd").alias("file_date"),
            # --- 요약 방식 선택 ---
            # 방식 1: Databricks ai_query() (현재 활성)
            summarize_with_ai_query().alias("ai_summary"),
            # 방식 2: OpenAI API (추후 활성화 시 위 주석 해제 + 아래 주석 해제)
            # summarize_with_openai(spark.read.table("silver_documents")).alias("ai_summary"),
            F.current_timestamp().alias("gold_refreshed_at"),
            F.lit("gold").alias("gold_layer"),
        )
    )
