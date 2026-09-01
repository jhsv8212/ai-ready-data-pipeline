# Databricks notebook source
# DBTITLE 1,Gold Delta 복사 & Vector Search 동기화
# MAGIC %md
# MAGIC # Gold Delta 복사 & Vector Search 동기화
# MAGIC
# MAGIC RAG Document Processing Pipeline 완료 후 실행하는 노트북입니다.
# MAGIC
# MAGIC **실행 시점:** 잡 태스크로, 파이프라인(Bronze → Silver → Gold) 완료 후 자동 실행
# MAGIC
# MAGIC **동작:**
# MAGIC 1. 카테고리별 Gold Streaming Table을 UNION ALL로 합쳐 **단일 Delta Table** (`gold_embeddings`)로 복사
# MAGIC 2. 통합 Delta Sync Index 1개 생성 (최초 1회, 이후 스킵)
# MAGIC 3. Vector Search 인덱스 동기화
# MAGIC
# MAGIC **카테고리 구분:** `metadata` 컬럼의 `agent_id`, `doc_path` 필터로 검색 시 구분 가능

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
VS_ENDPOINT_NAME = "a-aisearch-ard-dev-ane2-01"
EMBEDDING_DIMENSION = config.EMBEDDING_API_DIMENSION  # 1024

# 통합 테이블/인덱스명
UNIFIED_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.gold_embeddings"
UNIFIED_INDEX = f"{CATALOG}.{GOLD_SCHEMA}.gold_embeddings_index"

print(f"감지된 카테고리 ({len(categories)}개): {categories}")
print(f"영문명: {[config.get_table_name(c) for c in categories]}")
print(f"통합 테이블: {UNIFIED_TABLE}")
print(f"통합 인덱스: {UNIFIED_INDEX}")
print(f"Vector Search 엔드포인트: {VS_ENDPOINT_NAME}")

# COMMAND ----------

# DBTITLE 1,Gold Streaming Table → 통합 Delta Table 복사 (UNION ALL)
# --- [이전 방식: 카테고리별 개별 Delta Table 복사] ---
# copy_results = []
# for category in categories:
#     table_name = config.get_table_name(category)
#     source = f"`{CATALOG}`.`{GOLD_SCHEMA}`.`{table_name}_gold_document_embeddings`"
#     target = f"`{CATALOG}`.`{GOLD_SCHEMA}`.`{table_name}_gold_embeddings`"
#     try:
#         spark.sql(f"""
#             CREATE OR REPLACE TABLE {target}
#             TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
#             AS SELECT * FROM {source}
#         """)
#         count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table_name}_gold_embeddings").count()
#         print(f"✅ [{table_name}] {count}개 행 복사 완료: {target}")
#         copy_results.append({"table_name": table_name, "status": "OK", "rows": count})
#     except Exception as e:
#         error_msg = str(e)[:200]
#         print(f"❌ [{table_name}] 복사 실패: {error_msg}")
#         copy_results.append({"table_name": table_name, "status": "FAILED", "error": error_msg})
# print(f"\n--- 복사 요약 ---")
# success_count = sum(1 for r in copy_results if r["status"] == "OK")
# print(f"성공: {success_count} / 전체: {len(copy_results)}")
# --- [이전 방식 끝] ---

# 카테고리별 소스 테이블을 UNION ALL로 통합
union_queries = []
skipped = []

for category in categories:
    table_name = config.get_table_name(category)
    source = f"`{CATALOG}`.`{GOLD_SCHEMA}`.`{table_name}_gold_document_embeddings`"
    try:
        cnt = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table_name}_gold_document_embeddings").count()
        if cnt > 0:
            union_queries.append(f"SELECT * FROM {source}")
            print(f"✅ [{table_name}] {cnt}개 행 감지")
        else:
            print(f"⚠️ [{table_name}] 빈 테이블 — 스킵")
            skipped.append(table_name)
    except Exception as e:
        print(f"❌ [{table_name}] 소스 테이블 없음 — 스킵: {str(e)[:120]}")
        skipped.append(table_name)

if not union_queries:
    raise RuntimeError("통합할 소스 테이블이 없습니다. 파이프라인 실행 결과를 확인하세요.")

union_sql = " UNION ALL ".join(union_queries)

spark.sql(f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.`{GOLD_SCHEMA}`.`gold_embeddings`
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS {union_sql}
""")

total_rows = spark.table(UNIFIED_TABLE).count()
print(f"\n--- 통합 복사 완료 ---")
print(f"통합 테이블: {UNIFIED_TABLE}")
print(f"총 행 수: {total_rows}")
print(f"스킵된 카테고리: {skipped if skipped else '없음'}")

# COMMAND ----------

# DBTITLE 1,통합 인덱스 생성 (최초 1회) & 동기화
# --- [이전 방식: 카테고리별 인덱스 5개 생성 & 동기화] ---
# sync_results = []
# for category in categories:
#     table_name = config.get_table_name(category)
#     source_table = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}_gold_embeddings"
#     index_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}_gold_embeddings_index"
#     index_exists = False
#     try:
#         w.vector_search_indexes.get_index(index_name=index_name)
#         index_exists = True
#     except Exception:
#         pass
#     if not index_exists:
#         w.vector_search_indexes.create_index(
#             name=index_name, endpoint_name=VS_ENDPOINT_NAME,
#             primary_key="chunk_id", index_type="DELTA_SYNC",
#             delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
#                 source_table=source_table, pipeline_type="TRIGGERED",
#                 embedding_vector_columns=[EmbeddingVectorColumn(name="embedding", embedding_dimension=EMBEDDING_DIMENSION)]))
#     w.vector_search_indexes.sync_index(index_name=index_name)
# --- [이전 방식 끝] ---

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingVectorColumn,
    PipelineType,
    VectorIndexType,
)

w = WorkspaceClient()

# 1. 인덱스 존재 여부 확인
index_exists = False
try:
    w.vector_search_indexes.get_index(index_name=UNIFIED_INDEX)
    index_exists = True
    print(f"ℹ️ 인덱스 이미 존재: {UNIFIED_INDEX}")
except Exception:
    pass

# 2. 인덱스 생성 (최초 1회)
if not index_exists:
    try:
        w.vector_search_indexes.create_index(
            name=UNIFIED_INDEX,
            endpoint_name=VS_ENDPOINT_NAME,
            primary_key="chunk_id",
            index_type=VectorIndexType.DELTA_SYNC,
            delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
                source_table=UNIFIED_TABLE,
                pipeline_type=PipelineType.TRIGGERED,
                embedding_vector_columns=[
                    EmbeddingVectorColumn(name="embedding", embedding_dimension=EMBEDDING_DIMENSION)
                ]
            )
        )
        print(f"✅ 인덱스 생성 완료: {UNIFIED_INDEX}")
    except Exception as e:
        raise RuntimeError(f"인덱스 생성 실패: {str(e)[:300]}")

# 3. 인덱스 준비 대기 후 동기화 요청
import time

max_wait = 300  # 최대 5분 대기
wait_interval = 15
elapsed = 0
while elapsed < max_wait:
    idx = w.vector_search_indexes.get_index(index_name=UNIFIED_INDEX)
    if idx.status and idx.status.ready:
        print(f"✅ 인덱스 준비 완료")
        break
    print(f"⏳ 인덱스 초기화 중... ({elapsed}s / {max_wait}s)")
    time.sleep(wait_interval)
    elapsed += wait_interval
else:
    print(f"⚠️ {max_wait}초 경과 — 인덱스가 아직 준비되지 않았습니다. 나중에 수동 동기화하세요.")

if idx.status and idx.status.ready:
    try:
        w.vector_search_indexes.sync_index(index_name=UNIFIED_INDEX)
        print(f"✅ 동기화 요청 성공: {UNIFIED_INDEX}")
    except Exception as e:
        raise RuntimeError(f"동기화 실패: {str(e)[:300]}")

# COMMAND ----------

# DBTITLE 1,동기화 상태 확인
# --- [이전 방식: 카테고리별 인덱스 상태 확인] ---
# for category in categories:
#     table_name = config.get_table_name(category)
#     index_name = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}_gold_embeddings_index"
#     try:
#         index_info = w.vector_search_indexes.get_index(index_name=index_name)
#         status = index_info.status
#         print(f"[{table_name}] 준비: {status.ready} | 상태: {status.message}")
#     except Exception as e:
#         print(f"[{table_name}] 조회 실패: {str(e)[:120]}")
# --- [이전 방식 끝] ---

print("통합 인덱스 동기화 상태 확인 중...\n")

try:
    index_info = w.vector_search_indexes.get_index(index_name=UNIFIED_INDEX)
    status = index_info.status
    print(f"인덱스: {UNIFIED_INDEX}")
    print(f"준비: {status.ready}")
    print(f"상태: {status.message}")
except Exception as e:
    print(f"조회 실패: {str(e)[:200]}")