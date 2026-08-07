"""Gold Layer: Vector Search용 임베딩 벡터 생성 테이블

입력: gold_document_chunks (Materialized View)
출력: gold_document_embeddings (Materialized View)
  - gold_document_chunks의 chunk_content를 databricks-bge-large-en 모델로 임베딩
  - Vector Search 인덱스의 소스 테이블로 사용

[Vector Search 인덱스 동기화 안내]
  이 테이블을 생성한 뒤, 아래 단계로 Vector Search 인덱스를 설정하세요:

  1. Vector Search 엔드포인트 생성 (UI 또는 SDK):
     from databricks.vector_search.client import VectorSearchClient
     vsc = VectorSearchClient()
     vsc.create_endpoint(name="document-search-endpoint")

  2. Delta Sync 인덱스 생성 (임베딩이 이미 계산되어 있으므로 직접 동기화):
     vsc.create_delta_sync_index(
         endpoint_name="document-search-endpoint",
         index_name="developer팀.default.gold_document_embeddings_index",
         source_table_name="developer팀.default.gold_document_embeddings",
         pipeline_type="TRIGGERED",
         primary_key="chunk_id",
         embedding_vector_column="embedding",
         embedding_dimension=1024,  # BGE-large-en 차원
         embedding_vector_columns=None,
     )

  3. 인덱스 동기화는 소스 테이블 업데이트 시 자동 또는 수동(triggered)으로 실행됩니다.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import config

# =============================================================================
# 임베딩 설정값 (config.py에서 관리)
# =============================================================================

# 임베딩 모델 엔드포인트명
EMBEDDING_MODEL = "databricks-bge-large-en"

# 임베딩 대상 컬럼명
EMBEDDING_SOURCE_COL = "chunk_content"


@dp.materialized_view(
    comment="Vector Search용 문서 청크 임베딩 (Gold Layer) - databricks-bge-large-en"
)
def gold_document_embeddings():
    return (
        spark.read.table("gold_document_chunks")
        .withColumn(
            "chunk_id",
            F.md5(F.concat("document_id", F.lit("_"), F.col("element_idx").cast("string")))
        )
        .withColumn(
            "embedding",
            F.expr(f"""
                ai_query(
                    '{EMBEDDING_MODEL}',
                    {EMBEDDING_SOURCE_COL}
                )
            """)
        )
        .select(
            "chunk_id",
            "document_id",
            "source_file_name",
            "element_type",
            "element_page",
            "element_idx",
            F.col(EMBEDDING_SOURCE_COL).alias("chunk_content"),
            "chunk_type",
            "embedding",
            "chunked_at",
        )
    )
