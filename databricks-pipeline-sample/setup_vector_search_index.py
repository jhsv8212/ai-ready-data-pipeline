# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search Index 설정 (카테고리별 동적 생성)
# MAGIC 
# MAGIC 파이프라인의 Gold 레이어 테이블(`{category}_gold_document_embeddings`)을 소스로 하는
# MAGIC AI Search (Vector Search) Delta Sync Index를 카테고리별로 생성합니다.
# MAGIC 
# MAGIC **사전 요구사항:**
# MAGIC 1. 파이프라인이 최소 1회 실행되어 Gold 테이블이 존재해야 합니다.
# MAGIC 2. Vector Search 엔드포인트가 생성되어 있어야 합니다 (없으면 아래 코드에서 생성).
# MAGIC 3. S3 Landing Zone에 카테고리 폴더가 존재해야 합니다.

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Shared/rag_document_processing_pipeline_f237c77c/transformations")

from databricks.sdk import WorkspaceClient

import config

w = WorkspaceClient()

# =============================================================================
# 설정값 - 파이프라인 config.py와 databricks.yml 기반으로 동적 생성
# =============================================================================

# 파이프라인 카탈로그 (databricks.yml의 var.catalog과 동일)
CATALOG = spark.conf.get("catalog", "a_ws_ard_dev_ane2_01")

# Gold 스키마 (파이프라인에서 gold.{table} 형태로 생성)
GOLD_SCHEMA = "gold"

# Vector Search 설정
VS_ENDPOINT_NAME = "<YOUR_VECTOR_SEARCH_ENDPOINT>"  # TODO: 실제 엔드포인트명으로 변경

# 임베딩 설정 (현재: embedding_source_columns 방식 — VS가 chunk_content에서 임베딩 자동 계산)
EMBEDDING_MODEL_ENDPOINT = "databricks-qwen3-embedding-0-6b"  # TODO: 실제 임베딩 모델 엔드포인트
EMBEDDING_SOURCE_COLUMN = "chunk_content"
PRIMARY_KEY = "chunk_id"

# 동기화 모드: "TRIGGERED" (수동 동기화) 또는 "CONTINUOUS" (자동 동기화)
PIPELINE_TYPE = "TRIGGERED"

# 카테고리 목록 (S3 Landing Zone 1뎁스 폴더 자동 스캔)
categories = config.get_category_list()
print(f"감지된 카테고리: {categories}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Vector Search 엔드포인트 생성 (이미 있으면 스킵)

# COMMAND ----------

from databricks.sdk.service.vectorsearch import EndpointType

# 기존 엔드포인트 확인
try:
    endpoint = w.vector_search_endpoints.get_endpoint(VS_ENDPOINT_NAME)
    print(f"✅ 엔드포인트 '{VS_ENDPOINT_NAME}'가 이미 존재합니다. (상태: {endpoint.endpoint_status.state})")
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
        print(f"🔧 엔드포인트 '{VS_ENDPOINT_NAME}' 생성 중...")
        w.vector_search_endpoints.create_endpoint(
            name=VS_ENDPOINT_NAME,
            endpoint_type=EndpointType.STANDARD,
        )
        print(f"✅ 엔드포인트 생성 완료. 프로비저닝까지 수 분 소요될 수 있습니다.")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: 카테고리별 Delta Sync Index 생성 (Compute-managed Embeddings)
# MAGIC 
# MAGIC Vector Search가 `chunk_content` 컬럼에서 임베딩을 자동 계산합니다.
# MAGIC 
# MAGIC > **향후 전환 예정:** bge-m3 FastAPI 서비스 개발 완료 후,
# MAGIC > 파이프라인에서 미리 계산한 `embedding` 컬럼을 사용하는
# MAGIC > `embedding_vector_columns` 방식으로 전환합니다.

# COMMAND ----------

from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    PipelineType,
)

created_indexes = []

for category in categories:
    source_table = f"{CATALOG}.{GOLD_SCHEMA}.{category}_gold_document_embeddings"
    index_name = f"{CATALOG}.{GOLD_SCHEMA}.{category}_gold_document_embeddings_index"

    try:
        existing_index = w.vector_search_indexes.get_index(index_name)
        print(f"✅ [{category}] 인덱스 '{index_name}' 이미 존재. 상태: {existing_index.status.message}")
        created_indexes.append(index_name)
    except Exception as e:
        if "RESOURCE_DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
            print(f"🔧 [{category}] 인덱스 '{index_name}' 생성 중...")

            w.vector_search_indexes.create_index(
                name=index_name,
                endpoint_name=VS_ENDPOINT_NAME,
                primary_key=PRIMARY_KEY,
                index_type="DELTA_SYNC",
                delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                    source_table=source_table,
                    pipeline_type=PipelineType(PIPELINE_TYPE),
                    embedding_source_columns=[
                        EmbeddingSourceColumn(
                            name=EMBEDDING_SOURCE_COLUMN,
                            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
                        )
                    ],
                    # RAG 검색 시 반환할 컬럼 (Gold 테이블 스키마에 맞춤)
                    columns_to_sync=[
                        PRIMARY_KEY,
                        "document_id",
                        "chunk_content",
                        "source_file_name",
                        "chunk_idx",
                        "chunk_type",
                    ],
                ),
            )
            print(f"✅ [{category}] 인덱스 생성 완료.")
            created_indexes.append(index_name)
        else:
            raise e

print(f"\n총 {len(created_indexes)}개 인덱스 처리 완료.")

# =============================================================================
# [향후 전환] bge-m3 FastAPI 연동 완료 후 아래로 교체:
# =============================================================================
# from databricks.sdk.service.vectorsearch import EmbeddingVectorColumn
#
# w.vector_search_indexes.create_index(
#     ...
#     delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
#         ...
#         embedding_vector_columns=[
#             EmbeddingVectorColumn(
#                 name="embedding",
#                 embedding_dimension=config.EMBEDDING_API_DIMENSION,
#             )
#         ],
#     ),
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: 인덱스 상태 확인

# COMMAND ----------

import time


def wait_for_index_ready(index_name, timeout_minutes=30):
    """인덱스가 준비될 때까지 대기합니다."""
    start = time.time()
    while True:
        index = w.vector_search_indexes.get_index(index_name)
        status = index.status

        if status.ready:
            print(f"✅ 인덱스 '{index_name}' 준비 완료!")
            return index

        elapsed = (time.time() - start) / 60
        if elapsed > timeout_minutes:
            print(f"⚠️ 타임아웃 ({timeout_minutes}분). 현재 상태: {status.message}")
            return index

        print(f"⏳ 대기 중... ({elapsed:.1f}분 경과) - {status.message}")
        time.sleep(30)


# 카테고리별 인덱스 상태 확인
for index_name in created_indexes:
    try:
        index_info = w.vector_search_indexes.get_index(index_name)
        print(f"[{index_name}] 상태: {index_info.status.message} | 준비: {index_info.status.ready}")
    except Exception as e:
        print(f"[{index_name}] 조회 실패: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: 인덱스 검색 테스트

# COMMAND ----------

# TRIGGERED 모드에서 수동 동기화 실행 (필요시)
# for index_name in created_indexes:
#     w.vector_search_indexes.sync_index(index_name)

# 검색 테스트 예시 (카테고리 선택 후 실행)
# test_category = categories[0]  # 첫 번째 카테고리로 테스트
# test_index = f"{CATALOG}.{GOLD_SCHEMA}.{test_category}_gold_document_embeddings_index"
#
# results = w.vector_search_indexes.query_index(
#     index_name=test_index,
#     columns=["chunk_id", "chunk_content", "source_file_name", "document_id"],
#     query_text="검색하고 싶은 내용을 입력하세요",
#     num_results=5,
# )
# print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 파이프라인 + Vector Search 연동 아키텍처
# MAGIC 
# MAGIC ```
# MAGIC [S3 Landing Zone]                    [AI Search Index (카테고리별)]
# MAGIC      │                                        ▲
# MAGIC      ▼                                        │ (Delta Sync)
# MAGIC ┌──────────────┐  ┌───────────────┐  ┌────────────────────────────────┐
# MAGIC │   Bronze     │→│    Silver     │→│           Gold                 │
# MAGIC │{cat}_bronze_ │  │{cat}_silver_  │  │ {cat}_gold_document_embeddings │
# MAGIC │  documents   │  │doc_chunks     │  │  (CDF enabled)                 │
# MAGIC └──────────────┘  └───────────────┘  └────────────────────────────────┘
# MAGIC  binaryFile        path-based         chunk_content → VS 임베딩 자동 계산
# MAGIC  S3 수집            chunking           (향후: bge-m3 API → embedding 컬럼)
# MAGIC ```
# MAGIC 
# MAGIC **동기화 방식:**
# MAGIC - `TRIGGERED`: 파이프라인 실행 후 수동으로 `sync_index()` 호출
# MAGIC - `CONTINUOUS`: Gold 테이블 업데이트 시 자동으로 인덱스 동기화 (추가 비용 발생)
# MAGIC 
# MAGIC **임베딩 방식:**
# MAGIC - [현재] `embedding_source_columns`: Vector Search가 chunk_content에서 임베딩 자동 계산
# MAGIC - [향후] `embedding_vector_columns`: bge-m3 FastAPI 서비스에서 미리 계산한 embedding 사용
