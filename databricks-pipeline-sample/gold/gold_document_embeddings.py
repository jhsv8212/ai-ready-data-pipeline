"""Gold Layer: Vector Search 소스 테이블

입력: {상품명}_silver_document_chunks (Streaming Table)
출력: {상품명}_gold_document_embeddings (Streaming Table, 상품별로 동적 생성)
  - config.get_product_list()로 스캔한 상품마다 별도 테이블을 생성한다.
  - Vector Search 인덱스의 소스 테이블로 사용
  - 임베딩은 Vector Search가 chunk_content 컬럼에서 직접 계산 (embedding_source_columns 방식)

[Vector Search 인덱스 동기화 안내]
  이 테이블들을 생성한 뒤, 상품별로 아래 단계를 반복해 Vector Search 인덱스를 설정하세요
  (인덱스 생성은 이 파이프라인 밖에서 별도 노트북/스크립트로 수행합니다):

  1. Vector Search 엔드포인트 생성 (UI 또는 SDK, 상품 간 공용):
     from databricks.vector_search.client import VectorSearchClient
     vsc = VectorSearchClient()
     vsc.create_endpoint(name="document-search-endpoint")

  2. 상품별 Delta Sync 인덱스 생성 (Vector Search가 chunk_content에서 임베딩 자동 계산):
     for product in config.get_product_list():
         vsc.create_delta_sync_index(
             endpoint_name="document-search-endpoint",
             index_name=f"dev_haesung.gold.{product}_gold_document_embeddings_index",
             source_table_name=f"dev_haesung.gold.{product}_gold_document_embeddings",
             pipeline_type="TRIGGERED",
             primary_key="chunk_id",
             embedding_source_columns=[{
                 "name": "chunk_content",
                 "model_endpoint_name": "databricks-qwen3-embedding-0-6b"
             }],
         )

  3. 인덱스 동기화는 소스 테이블 업데이트 시 자동 또는 수동(triggered)으로 실행됩니다.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

import config


def _generate_gold_document_embeddings(product: str):
    """상품 하나에 대한 {product}_gold_document_embeddings 테이블을 정의한다."""

    @dp.table(
        name=f"dev_haesung.gold.{product}_gold_document_embeddings",
        comment=f"'{product}' 상품 Vector Search 소스 테이블 (Gold Layer) - 임베딩은 Vector Search가 chunk_content에서 자동 계산",
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
            CONSTRAINT `pk_{product}_gold_embeddings` PRIMARY KEY (chunk_id),
            CONSTRAINT `fk_{product}_embeddings_document` FOREIGN KEY (document_id) REFERENCES dev_haesung.silver.{product}_silver_documents(document_id)
        """,
    )
    def gold_document_embeddings():
        return (
            spark.readStream.table(f"dev_haesung.silver.{product}_silver_document_chunks")
            .withColumn(
                "chunk_id",
                F.md5(F.concat(
                    "document_id", F.lit("_"),
                    F.col("element_idx").cast("string"), F.lit("_"),
                    F.col("chunk_idx").cast("string")
                ))
            )
            .select(
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
            )
        )

    return gold_document_embeddings


for _product in config.get_product_list():
    _generate_gold_document_embeddings(_product)
