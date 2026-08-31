# Databricks notebook source
# DBTITLE 1,Vector Search Index 동기화
# MAGIC %md
# MAGIC # Vector Search Index 동기화
# MAGIC
# MAGIC RAG Document Processing Pipeline 완료 후 카테고리별 Vector Search Delta Sync Index를 동기화하는 노트북입니다.
# MAGIC
# MAGIC **실행 시점:** 파이프라인(Bronze → Silver → Gold) 완료 후, Job 태스크로 실행
# MAGIC
# MAGIC **동작:**
# MAGIC 1. S3 Landing Zone의 카테고리 목록을 스캔
# MAGIC 2. 각 카테고리별 `{category}_gold_document_embeddings_index` 인덱스를 동기화
# MAGIC 3. 동기화 상태 확인

# COMMAND ----------

# DBTITLE 1,설정값 로드
import sys
sys.path.insert(0, "/Workspace/Shared/rag_document_processing_pipeline_f237c77c/transformations")

import config

# S3 Landing Zone 1덱스 폴더명을 카테고리 목록으로 스캔
categories = config.get_category_list()

# Unity Catalog 경로
CATALOG = "a_ws_ard_dev_ane2_01"
GOLD_SCHEMA = "gold"

# Vector Search 엔드포인트
VS_ENDPOINT_NAME = config.VS_ENDPOINT_NAME  # "document-search-endpoint"

print(f"감지된 카테고리 ({len(categories)}개): {categories}")
print(f"Vector Search 엔드포인트: {VS_ENDPOINT_NAME}")

# COMMAND ----------

# DBTITLE 1,카테고리별 인덱스 동기화
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

sync_results = []

for category in categories:
    index_name = f"{CATALOG}.{GOLD_SCHEMA}.{category}_gold_document_embeddings_index"
    try:
        w.vector_search_indexes.sync_index(index_name=index_name)
        print(f"✅ [{category}] 동기화 요청 성공: {index_name}")
        sync_results.append({"category": category, "index_name": index_name, "status": "SYNC_REQUESTED"})
    except Exception as e:
        error_msg = str(e)[:200]
        print(f"❌ [{category}] 동기화 실패: {index_name}")
        print(f"   오류: {error_msg}")
        sync_results.append({"category": category, "index_name": index_name, "status": "FAILED", "error": error_msg})

print(f"\n--- 동기화 요약 ---")
success_count = sum(1 for r in sync_results if r["status"] == "SYNC_REQUESTED")
fail_count = len(sync_results) - success_count
print(f"성공: {success_count} / 실패: {fail_count} / 전체: {len(sync_results)}")

# COMMAND ----------

# DBTITLE 1,동기화 상태 확인
import time

print("인덱스 동기화 상태 확인 중...\n")

for category in categories:
    index_name = f"{CATALOG}.{GOLD_SCHEMA}.{category}_gold_document_embeddings_index"
    try:
        index_info = w.vector_search_indexes.get_index(index_name=index_name)
        status = index_info.status
        print(
            f"[{category}] "
            f"준비: {status.ready} | "
            f"상태: {status.message}"
        )
    except Exception as e:
        print(f"[{category}] 조회 실패: {str(e)[:120]}")