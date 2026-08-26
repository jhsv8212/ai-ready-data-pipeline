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
  - ai_query: Databricks 내장 ai_query() 함수 사용 [기존 방식 - 비활성화]
    → 고객사 제공 LLM API로 교체 예정이라 아래에서 주석 처리했습니다.
  - client_llm_api: 고객사에서 API 형태로 제공할 LLM 사용 [신규 방식 - 연동 준비]
    → 고객사의 개발/전달이 완료되면 아래 관련 코드의 주석을 해제하고
      config.py의 CLIENT_LLM_API_* 값을 실제 값으로 설정하면 됩니다.
      그 전까지 summary/keywords/metadata는 NULL로 채워집니다.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Column

import config


# =============================================================================
# 요약 방식 1: Databricks ai_query() [기존 방식 - 비활성화]
# =============================================================================
# --- 고객사 LLM API 연동 계획으로 대체되어 아래 함수는 주석 처리합니다.        ---
# --- 고객사 API 연동 전까지 임시로 사용하려면 아래 주석을 해제하세요.          ---
#
# def summarize_with_ai_query() -> Column:
#     """
#     Databricks 내장 ai_query() SQL 함수를 사용하여 문서를 요약합니다.
#
#     - 모델: config.LLM_MODEL_NAME
#     - 입력: full_text 앞 2500자 + figure descriptions 앞 500자
#     - 출력: 한국어 3~5문장 요약
#
#     Returns:
#         Column: ai_summary 컨텐츠가 담긴 Spark Column
#     """
#     return F.expr(f"""
#         ai_query(
#             '{config.LLM_MODEL_NAME}',
#             '다음 보험 문서를 한국어로 3~5문장으로 요약하세요. 핵심 보장 내용, 주요 조건, 대상 독자를 포함하세요. 그림/차트 설명이 있으면 해당 내용도 반영하세요.\n\n'
#                 || '[문서 텍스트]\n' || substr(full_text, 1, {config.AI_INPUT_MAX_CHARS})
#                 || CASE WHEN concat_ws(chr(10),
#                        transform(
#                            filter(
#                                try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
#                                el -> try_cast(el:type AS STRING) = 'figure'
#                                      AND coalesce(
#                                          try_cast(el:description AS STRING),
#                                          try_cast(el:content AS STRING)
#                                      ) IS NOT NULL
#                            ),
#                            el -> coalesce(
#                                try_cast(el:description AS STRING),
#                                try_cast(el:content AS STRING)
#                            )
#                        )
#                    ) != ''
#                        THEN '\n\n[그림/차트 설명]\n' || substr(
#                            concat_ws(chr(10),
#                                transform(
#                                    filter(
#                                        try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
#                                        el -> try_cast(el:type AS STRING) = 'figure'
#                                              AND coalesce(
#                                                  try_cast(el:description AS STRING),
#                                                  try_cast(el:content AS STRING)
#                                              ) IS NOT NULL
#                                    ),
#                                    el -> coalesce(
#                                        try_cast(el:description AS STRING),
#                                        try_cast(el:content AS STRING)
#                                    )
#                                )
#                            ), 1, 500)
#                        ELSE '' END,
#             modelParameters => named_struct('max_tokens', {config.AI_MAX_TOKENS})
#         )
#     """)


# =============================================================================
# 요약 방식 2: 고객사 제공 LLM API [신규 방식 - 연동 준비]
# =============================================================================
# --- 아래 코드는 고객사로부터 LLM API 스펙/키를 전달받은 후 주석을 해제하여 사용합니다. ---
# --- 사용 전 config.py의 CLIENT_LLM_API_* 값을 실제 값으로 설정하고,                  ---
# --- 요청/응답 페이로드를 실제 API 스펙(현재는 OpenAI 호환 chat completions 형식으로  ---
# --- 가정)에 맞게 수정해야 합니다.                                                    ---
#
# import pandas as pd
# import requests
# from pyspark.sql.functions import pandas_udf
# from pyspark.sql.types import StringType
#
#
# def _call_client_llm_api(system_prompt: str, user_text: str, max_tokens: int) -> str:
#     """고객사 LLM API를 호출하고 응답 텍스트를 반환합니다."""
#     api_key = dbutils.secrets.get(
#         scope=config.CLIENT_LLM_API_KEY_SCOPE, key=config.CLIENT_LLM_API_KEY_KEY
#     )
#     response = requests.post(
#         config.CLIENT_LLM_API_URL,
#         headers={"Authorization": f"Bearer {api_key}"},
#         json={
#             "model": config.CLIENT_LLM_MODEL_NAME,
#             "messages": [
#                 {"role": "system", "content": system_prompt},
#                 {"role": "user", "content": user_text},
#             ],
#             "max_tokens": max_tokens,
#         },
#         timeout=config.CLIENT_LLM_API_TIMEOUT,
#     )
#     response.raise_for_status()
#     # TODO: 실제 응답 스키마에 맞게 파싱 로직 수정 (아래는 OpenAI 호환 스펙 가정)
#     return response.json()["choices"][0]["message"]["content"]
#
#
# def _make_client_llm_udf(system_prompt: str, max_tokens: int):
#     """system_prompt/max_tokens를 고정한 고객사 LLM 호출용 pandas_udf를 생성합니다."""
#
#     @pandas_udf(StringType())
#     def _udf(texts: pd.Series) -> pd.Series:
#         results = []
#         for text in texts:
#             if text is None or len(text.strip()) == 0:
#                 results.append(None)
#                 continue
#             try:
#                 results.append(_call_client_llm_api(system_prompt, text, max_tokens))
#             except Exception as e:
#                 results.append(f"[고객사 LLM 호출 실패: {str(e)}]")
#         return pd.Series(results)
#
#     return _udf
#
#
# summarize_with_client_llm_udf = _make_client_llm_udf(
#     system_prompt=(
#         "당신은 보험 문서 요약 전문가입니다. 한국어로 3~5문장으로 요약하세요. "
#         "핵심 보장 내용, 주요 조건, 대상 독자를 포함하세요."
#     ),
#     max_tokens=config.CLIENT_LLM_MAX_TOKENS,
# )
# extract_keywords_with_client_llm_udf = _make_client_llm_udf(
#     system_prompt=(
#         "다음 보험 문서에서 핵심 키워드를 10~20개 추출하세요. "
#         "보험 상품명, 보장 내용, 관련 시스템, 규정, 업무 프로세스 등을 포함하세요. "
#         'JSON 배열 형식으로만 응답하세요. 예: ["종신보험", "언더라이팅", ...]'
#     ),
#     max_tokens=200,
# )
# extract_metadata_with_client_llm_udf = _make_client_llm_udf(
#     # 필드 목록은 config.METADATA_SCHEMA에서 관리합니다. 필드를 추가/변경하려면
#     # 이 프롬프트가 아니라 config.py의 METADATA_SCHEMA를 수정하세요.
#     system_prompt=(
#         "다음 보험 문서를 분석하여 메타데이터를 JSON으로 추출하세요. "
#         "반드시 아래 필드를 포함하고, JSON만 응답하세요:\n"
#         f"{config.build_metadata_prompt_fields()}"
#     ),
#     max_tokens=300,
# )


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

        # --- 요약: 기존 ai_query() 방식 (비활성화) ---
        # df = df.withColumn("summary", summarize_with_ai_query())
        # --- 여기까지 ---

        # --- 요약: 고객사 LLM API 연동 준비 (전달 완료 후 주석 해제) ---
        # df = df.withColumn(
        #     "summary",
        #     summarize_with_client_llm_udf(F.substring("full_text", 1, config.AI_INPUT_MAX_CHARS)),
        # )
        # --- 여기까지 ---

        # TODO: 고객사 LLM API 연동 전까지 summary는 NULL로 채워집니다 (아래 select()에서 직접 처리).
        # df = df.withColumn("summary", F.lit(None).cast("string"))

        # --- 키워드 추출: 기존 ai_query() 방식 (비활성화) ---
        # df = df.withColumn(
        #     "_raw_keywords",
        #     F.expr(f"""
        #         ai_query(
        #             '{config.LLM_MODEL_NAME}',
        #             '다음 보험 문서에서 핵심 키워드를 10~20개 추출하세요. '
        #             || '보험 상품명, 보장 내용, 관련 시스템, 규정, 업무 프로세스 등을 포함하세요. '
        #             || 'JSON 배열 형식으로만 응답하세요. 예: ["종신보험", "언더라이팅", ...]\n\n'
        #             || substr(full_text, 1, {config.AI_INPUT_MAX_CHARS}),
        #             modelParameters => named_struct('max_tokens', 200)
        #         )
        #     """),
        # )
        # df = df.withColumn(
        #     "keywords",
        #     F.from_json(_strip_code_fence(F.col("_raw_keywords")), "ARRAY<STRING>"),
        # )
        # --- 여기까지 ---

        # --- 키워드 추출: 고객사 LLM API 연동 준비 (전달 완료 후 주석 해제) ---
        # df = df.withColumn(
        #     "_raw_keywords",
        #     extract_keywords_with_client_llm_udf(F.substring("full_text", 1, config.AI_INPUT_MAX_CHARS)),
        # )
        # df = df.withColumn(
        #     "keywords",
        #     F.from_json(_strip_code_fence(F.col("_raw_keywords")), "ARRAY<STRING>"),
        # )
        # --- 여기까지 ---

        # TODO: 고객사 LLM API 연동 전까지 keywords는 NULL로 채워집니다 (아래 select()에서 직접 처리).
        # df = df.withColumn("keywords", F.lit(None).cast("array<string>"))

        # --- 메타데이터 추출: 기존 ai_query() 방식 (비활성화) ---
        # df = df.withColumn(
        #     "_raw_metadata",
        #     F.expr(f"""
        #         ai_query(
        #             '{config.LLM_MODEL_NAME}',
        #             '다음 보험 문서를 분석하여 메타데이터를 JSON으로 추출하세요. '
        #             || '반드시 아래 필드를 포함하고, JSON만 응답하세요:\n'
        #             || '{config.build_metadata_prompt_fields()}'
        #             || '\n\n'
        #             || substr(full_text, 1, {config.AI_INPUT_MAX_CHARS}),
        #             modelParameters => named_struct('max_tokens', 300)
        #         )
        #     """),
        # )
        # df = df.withColumn("_metadata_cleaned", _strip_code_fence(F.col("_raw_metadata")))
        # df = df.withColumn("metadata", F.expr("parse_json(_metadata_cleaned)"))
        # --- 여기까지 ---

        # --- 메타데이터 추출: 고객사 LLM API 연동 준비 (전달 완료 후 주석 해제) ---
        # df = df.withColumn(
        #     "_raw_metadata",
        #     extract_metadata_with_client_llm_udf(F.substring("full_text", 1, config.AI_INPUT_MAX_CHARS)),
        # )
        # df = df.withColumn("_metadata_cleaned", _strip_code_fence(F.col("_raw_metadata")))
        # df = df.withColumn("metadata", F.expr("parse_json(_metadata_cleaned)"))
        # --- 여기까지 ---

        # TODO: 고객사 LLM API 연동 전까지 metadata는 NULL로 채워집니다 (아래 select()에서 직접 처리).
        # df = df.withColumn("metadata", F.expr("CAST(NULL AS VARIANT)"))

        return df.select(
            "document_id",
            "source_file_name",
            F.lit("고객사 LLM API 연동 대기 (준비 코드 작성 완료)").alias("extraction_method"),
            F.lit(None).cast("string").alias("summary"),
            F.lit(None).cast("array<string>").alias("keywords"),
            F.expr("CAST(NULL AS VARIANT)").alias("metadata"),
            F.current_timestamp().alias("generated_at"),
        )

    return gold_document_ai_summary


for _category in config.get_category_list():
    _generate_gold_document_ai_summary(_category)
