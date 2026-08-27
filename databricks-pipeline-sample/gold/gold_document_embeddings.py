"""Gold Layer: Vector Search 소스 테이블

입력: {카테고리명}_silver_document_chunks (Streaming Table)
출력: {카테고리명}_gold_document_embeddings (Streaming Table, 카테고리별로 동적 생성)
  - config.get_category_list()로 스캔한 카테고리마다 별도 테이블을 생성한다.
  - Vector Search 인덱스의 소스 테이블로 사용
  - [기존 방식] 임베딩은 Vector Search가 chunk_content 컬럼에서 직접 계산 (embedding_source_columns 방식)
  - [확정 방식 - 임베딩 서버 개발 진행 중] 다른 팀이 개발 중인 bge-m3 임베딩 FastAPI 서비스가
    완료되는 대로, 이 파이프라인에서 chunk_content를 직접 API로 호출해 임베딩 벡터를 계산/저장하고
    (embedding_vector_column 방식) Databricks Vector Search로 인덱스를 생성해 그 엔드포인트를
    에이전트가 직접 호출(similarity_search)하는 구조로 전환하기로 확정되었다. 관련 준비 코드는 아래
    `embed_with_bge_m3_api` 부분에 주석 처리되어 있으며, 서비스 개발 완료 후 주석을 해제하고
    config.py의 EMBEDDING_API_* 값을 실제 값으로 설정하면 된다.

[Vector Search 인덱스 동기화 안내]
  이 테이블들을 생성한 뒤, 카테고리별로 아래 단계를 반복해 Vector Search 인덱스를 설정하세요
  (인덱스 생성은 이 파이프라인 밖에서 별도 노트북/스크립트로 수행합니다):

  1. Vector Search 엔드포인트 생성 (UI 또는 SDK, 카테고리 간 공용):
     from databricks.vector_search.client import VectorSearchClient
     vsc = VectorSearchClient()
     vsc.create_endpoint(name="document-search-endpoint")

  2-a. [기존 방식] 카테고리별 Delta Sync 인덱스 생성
       (Vector Search가 chunk_content에서 임베딩 자동 계산):
     for category in config.get_category_list():
         vsc.create_delta_sync_index(
             endpoint_name="document-search-endpoint",
             index_name=f"dev_haesung.gold.{category}_gold_document_embeddings_index",
             source_table_name=f"dev_haesung.gold.{category}_gold_document_embeddings",
             pipeline_type="TRIGGERED",
             primary_key="chunk_id",
             embedding_source_columns=[{
                 "name": "chunk_content",
                 "model_endpoint_name": "databricks-qwen3-embedding-0-6b"
             }],
         )

  2-b. [신규 방식 - bge-m3 FastAPI 연동 완료 후 사용] 이 파이프라인이 미리 계산해 저장한
       embedding 컬럼을 그대로 사용하는 Delta Sync 인덱스 생성:
     for category in config.get_category_list():
         vsc.create_delta_sync_index(
             endpoint_name="document-search-endpoint",
             index_name=f"dev_haesung.gold.{category}_gold_document_embeddings_index",
             source_table_name=f"dev_haesung.gold.{category}_gold_document_embeddings",
             pipeline_type="TRIGGERED",
             primary_key="chunk_id",
             embedding_vector_column="embedding",
             embedding_dimension=config.EMBEDDING_API_DIMENSION,
         )

  3. 인덱스 동기화는 소스 테이블 업데이트 시 자동 또는 수동(triggered)으로 실행됩니다.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# import config  # 파이프라인 비활성화 - 골드 레이어 임시 중단


# =============================================================================
# bge-m3 FastAPI 임베딩 서비스 연동 (다른 팀 개발 완료 후 사용)
# =============================================================================
# --- 아래 코드는 다른 팀의 bge-m3 임베딩 FastAPI 서비스 개발이 완료된 후 주석을 해제하여 ---
# --- 사용합니다. 사용 전 config.py의 EMBEDDING_API_* 값을 실제 값으로 설정하고,        ---
# --- 요청/응답 페이로드를 실제 API 스펙에 맞게 수정해야 합니다(아래는 가정된 스펙).    ---
# --- 사용 시 아래 @dp.table(...) 의 schema에 `embedding ARRAY<FLOAT>,` 컬럼을 추가하고 ---
# --- select()에도 "embedding"을 포함해야 합니다.                                     ---
#
# import pandas as pd
# import requests
# from pyspark.sql.functions import pandas_udf
# from pyspark.sql.types import ArrayType, FloatType
#
#
# def _call_embedding_api(texts: list) -> list:
#     """bge-m3 FastAPI 서비스를 호출하여 텍스트 목록의 임베딩 벡터를 반환합니다."""
#     response = requests.post(
#         config.EMBEDDING_API_URL,
#         json={"model": config.EMBEDDING_API_MODEL_NAME, "texts": texts},
#         timeout=config.EMBEDDING_API_TIMEOUT,
#     )
#     response.raise_for_status()
#     # TODO: 실제 응답 스키마에 맞게 파싱 로직 수정 (아래는 {"embeddings": [[...], ...]} 형태 가정)
#     return response.json()["embeddings"]
#
#
# @pandas_udf(ArrayType(FloatType()))
# def embed_with_bge_m3_api(texts: pd.Series) -> pd.Series:
#     """chunk_content 컬럼을 bge-m3 FastAPI 서비스에 배치 전송하여 임베딩 벡터로 변환합니다."""
#     batch_size = config.EMBEDDING_API_BATCH_SIZE
#     text_list = texts.fillna("").tolist()
#     results = []
#     for start in range(0, len(text_list), batch_size):
#         batch = text_list[start:start + batch_size]
#         try:
#             results.extend(_call_embedding_api(batch))
#         except Exception:
#             results.extend([None] * len(batch))  # 실패한 배치는 NULL로 유지
#     return pd.Series(results, index=texts.index)


def _generate_gold_document_embeddings(category: str):
    """카테고리 하나에 대한 {category}_gold_document_embeddings 테이블을 정의한다."""

    @dp.table(
        name=f"gold.`{category}_gold_document_embeddings`",
        comment=f"'{category}' 카테고리 Vector Search 소스 테이블 (Gold Layer) - 현재는 Vector Search가 chunk_content에서 자동 계산, bge-m3 API 전환 확정(서비스 개발 완료 대기)",
        table_properties={"delta.enableChangeDataFeed": "true"},
        schema=f"""
            chunk_id STRING NOT NULL,
            document_id STRING NOT NULL,
            source_file_name STRING,
            element_type STRING,
            element_page INT,
            element_idx INT,
            chunk_idx INT,
            chunk_content STRING,
            chunk_type STRING,
            chunked_at TIMESTAMP,
            CONSTRAINT `pk_{category}_gold_embeddings` PRIMARY KEY (chunk_id),
            CONSTRAINT `fk_{category}_embeddings_document` FOREIGN KEY (document_id) REFERENCES silver.`{category}_silver_documents`(document_id)
        """,
        # bge-m3 FastAPI 연동 완료 후 위 schema에 `embedding ARRAY<FLOAT>,` 컬럼을 추가하세요.
    )
    def gold_document_embeddings():
        df = (
            spark.readStream.table(f"silver.`{category}_silver_document_chunks`")
            .withColumn(
                "chunk_id",
                F.md5(F.concat(
                    "document_id", F.lit("_"),
                    F.col("element_idx").cast("string"), F.lit("_"),
                    F.col("chunk_idx").cast("string")
                ))
            )
        )

        # --- bge-m3 FastAPI 임베딩 서비스 연동 준비 (다른 팀 개발 완료 후 주석 해제) ---
        # df = df.withColumn("embedding", embed_with_bge_m3_api(F.col("chunk_content")))
        # --- 여기까지 ---

        return df.select(
            "chunk_id",
            "document_id",
            "source_file_name",
            "element_type",
            "element_page",
            "element_idx",
            "chunk_idx",
            "chunk_content",
            "chunk_type",
            "chunked_at",
            # "embedding",  # bge-m3 FastAPI 연동 완료 후 주석 해제
        )

    return gold_document_embeddings


# for _category in config.get_category_list():
#     _generate_gold_document_embeddings(_category)
