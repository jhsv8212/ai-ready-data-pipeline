"""Gold Layer: Vector Search 소스 테이블

입력: {카테고리명}_silver_document_chunks (Streaming Table)
출력: {카테고리명}_gold_document_embeddings (Streaming Table, 카테고리별로 동적 생성)
  - config.get_category_list()로 스캔한 카테고리마다 별도 테이블을 생성한다.
  - Vector Search 인덱스의 소스 테이블로 사용
  - [신규 방식] bge-m3 임베딩 FastAPI 서비스가 연동 완료되어, 이 파이프라인에서
    chunk_content를 직접 API로 호출해 임베딩 벡터를 계산/저장한다 (embedding_vector_column
    방식). Databricks Vector Search로 인덱스를 생성해 그 엔드포인트를 에이전트가 직접
    호출(similarity_search)하는 구조로 전환한다.

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

  2-b. [신규 방식] 이 파이프라인이 미리 계산해 저장한
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
import sys
sys.path.insert(0, "/Workspace/Shared/rag_document_processing_pipeline_f237c77c/transformations")

import pandas as pd
import requests
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType

import config

# Worker 노드에서 config 모듈을 import할 수 없으므로,
# UDF가 캡처할 수 있도록 plain 값으로 추출
_EMBEDDING_API_URL = config.EMBEDDING_API_URL
_EMBEDDING_API_KEY = config.EMBEDDING_API_KEY
_EMBEDDING_API_TIMEOUT = config.EMBEDDING_API_TIMEOUT
_EMBEDDING_API_BATCH_SIZE = config.EMBEDDING_API_BATCH_SIZE


# =============================================================================
# bge-m3 FastAPI 임베딩 서비스 연동
# =============================================================================
def _call_embedding_api(texts: list) -> list:
    """bge-m3 FastAPI 서비스를 호출하여 텍스트 목록의 임베딩 벡터를 반환합니다."""
    response = requests.post(
        _EMBEDDING_API_URL,
        headers={
            "x-api-key": _EMBEDDING_API_KEY,
            "Content-Type": "application/json",
        },
        json={"texts": texts},
        timeout=_EMBEDDING_API_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()["embeddings"]


@pandas_udf(ArrayType(FloatType()))
def embed_with_bge_m3_api(texts: pd.Series) -> pd.Series:
    """chunk_content 컬럼을 bge-m3 FastAPI 서비스에 배치 전송하여 임베딩 벡터로 변환합니다."""
    batch_size = _EMBEDDING_API_BATCH_SIZE
    text_list = texts.fillna("").tolist()
    results = []
    for start in range(0, len(text_list), batch_size):
        batch = text_list[start:start + batch_size]
        try:
            results.extend(_call_embedding_api(batch))
        except Exception:
            results.extend([None] * len(batch))  # 실패한 배치는 NULL로 유지
    return pd.Series(results, index=texts.index)


def _generate_gold_document_embeddings(category: str):
    """카테고리 하나에 대한 {category}_gold_document_embeddings 테이블을 정의한다."""

    @dp.table(
        name=f"gold.`{category}_gold_document_embeddings`",
        comment=f"'{category}' 카테고리 Vector Search 소스 테이블 (Gold Layer) - bge-m3 FastAPI 서비스로 임베딩 벡터를 계산/저장 (embedding_vector_column 방식)",
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
            embedding ARRAY<FLOAT>,
            CONSTRAINT `pk_{category}_gold_embeddings` PRIMARY KEY (chunk_id),
            CONSTRAINT `fk_{category}_embeddings_document` FOREIGN KEY (document_id) REFERENCES silver.`{category}_silver_documents`(document_id)
        """,
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
            .withColumn("embedding", embed_with_bge_m3_api(F.col("chunk_content")))
        )

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
            "embedding",
        )

    return gold_document_embeddings


for _category in config.get_category_list():
    _generate_gold_document_embeddings(_category)
